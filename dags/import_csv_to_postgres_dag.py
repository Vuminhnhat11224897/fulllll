from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago

import os
import pandas as pd
import logging
from sqlalchemy import create_engine
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Thiết lập đường dẫn đến thư mục chứa dữ liệu
DATA_FOLDER = '/opt/airflow/data'  # Đường dẫn trong container

def get_csv_files():
    """Lấy danh sách các file CSV trong thư mục data"""
    csv_files = []
    
    # Kiểm tra thư mục data và các thư mục con
    for root, dirs, files in os.walk(DATA_FOLDER):
        for file in files:
            if file.endswith('.csv'):
                csv_files.append(os.path.join(root, file))
    
    logging.info(f"Tìm thấy {len(csv_files)} file CSV: {csv_files}")
    return csv_files

def create_article_table(hook, table_name):
    """Tạo bảng articles với cấu trúc cụ thể cho dữ liệu tin tức"""
    
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        title TEXT,
        link TEXT,
        content TEXT,
        date TIMESTAMP,
        author TEXT,
        category TEXT,
        source TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Thực thi câu lệnh tạo bảng
    hook.run(create_stmt)
    logging.info(f"Đã tạo bảng {table_name} với cấu trúc bài báo")

def create_table_from_df(hook, df, table_name):
    """Tạo bảng PostgreSQL dựa trên DataFrame, 
    hoặc sử dụng cấu trúc bài báo nếu có các cột phù hợp"""
    
    # Kiểm tra xem DataFrame có phải là dữ liệu bài báo không
    expected_columns = ['title', 'link', 'content', 'date', 'author', 'category', 'source']
    df_columns = [col.lower().strip() for col in df.columns]
    
    # Nếu có ít nhất 5 trong số các cột mong đợi, đây có thể là dữ liệu bài báo
    matching_columns = [col for col in expected_columns if col in df_columns]
    if len(matching_columns) >= 5:
        create_article_table(hook, table_name)
        return
    
    # Nếu không, tạo bảng thông thường dựa trên cấu trúc DataFrame
    columns = []
    
    for column, dtype in df.dtypes.items():
        col_name = column.strip()  # Loại bỏ khoảng trắng thừa
        if 'int' in str(dtype):
            columns.append(f'"{col_name}" INTEGER')
        elif 'float' in str(dtype):
            columns.append(f'"{col_name}" FLOAT')
        elif 'datetime' in str(dtype) or col_name.lower() == 'date':
            columns.append(f'"{col_name}" TIMESTAMP')
        else:
            columns.append(f'"{col_name}" TEXT')
    
    create_stmt = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        id SERIAL PRIMARY KEY,
        {', '.join(columns)},
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    
    # Thực thi câu lệnh tạo bảng
    hook.run(create_stmt)
    logging.info(f"Đã tạo bảng {table_name}")

def import_csv_to_postgres(csv_file, **kwargs):
    """Import dữ liệu từ CSV vào PostgreSQL"""
    try:
        # Lấy tên file để tạo tên bảng (loại bỏ đường dẫn và phần mở rộng)
        file_name = os.path.basename(csv_file)
        table_name = os.path.splitext(file_name)[0].lower()
        
        logging.info(f"Bắt đầu import file {csv_file} vào bảng {table_name}")
        
        # Đọc file CSV với xử lý lỗi encoding
        encodings = ['utf-8', 'latin1', 'cp1252']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(csv_file, encoding=encoding)
                logging.info(f"Đã đọc file CSV với encoding {encoding}")
                break
            except UnicodeDecodeError:
                continue
        
        if df is None:
            raise ValueError(f"Không thể đọc file CSV {csv_file} với các encoding đã thử")
        
        logging.info(f"Đã đọc file CSV với {len(df)} dòng và {len(df.columns)} cột")
        
        # Làm sạch tên cột (loại bỏ khoảng trắng thừa, ký tự đặc biệt)
        df.columns = [col.strip() for col in df.columns]
        
        # Chuẩn hóa tên cột (chuyển về lowercase)
        df.columns = [col.lower() for col in df.columns]
        
        # Xử lý trường date nếu tồn tại
        if 'date' in df.columns:
            try:
                # Cố gắng chuyển đổi thành định dạng datetime
                df['date'] = pd.to_datetime(df['date'], errors='coerce')
            except Exception as e:
                logging.warning(f"Không thể chuyển đổi cột date thành datetime: {e}")
        
        # Kết nối đến PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Tạo bảng nếu chưa tồn tại
        create_table_from_df(pg_hook, df, table_name)
        
        # Lấy kết nối từ hook
        engine = pg_hook.get_sqlalchemy_engine()
        
        # Xử lý các cột bị thiếu trong dữ liệu báo (nếu là bảng báo)
        expected_columns = ['title', 'link', 'content', 'date', 'author', 'category', 'source']
        df_columns = list(df.columns)
        
        for col in expected_columns:
            if col not in df_columns:
                df[col] = None
                logging.info(f"Đã thêm cột {col} với giá trị NULL")
        
        # Import dữ liệu vào PostgreSQL
        df.to_sql(
            table_name,
            engine,
            if_exists='replace',  # Thay thế nếu bảng đã tồn tại
            index=False,
            chunksize=1000,  # Xử lý theo lô để tránh quá tải bộ nhớ
            method='multi'  # Phương thức multi để tăng tốc độ import
        )
        
        # Đếm số dòng sau khi insert
        row_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {table_name}")[0]
        
        logging.info(f"Đã import thành công {len(df)} dòng vào bảng {table_name}. Tổng số dòng: {row_count}")
        
        return f"Imported {len(df)} rows to {table_name}"
    except Exception as e:
        logging.error(f"Lỗi khi import dữ liệu: {str(e)}")
        raise

with DAG(
    'import_csv_to_postgres',
    default_args=default_args,
    description='Import CSV files into PostgreSQL',
    schedule_interval=None,  # Chạy thủ công
    start_date=days_ago(1),
    catchup=False,
    tags=['import', 'csv', 'postgres'],
) as dag:
    
    # Task lấy danh sách file CSV
    get_files_task = PythonOperator(
        task_id='get_csv_files',
        python_callable=get_csv_files,
    )
    
    # Tạo dynamically các task import theo danh sách file CSV
    def create_import_task(csv_file):
        # Tạo task_id an toàn từ tên file
        safe_file_name = os.path.basename(csv_file).replace('.', '_').replace('-', '_').replace(' ', '_')
        task_id = f'import_{safe_file_name}'
        
        return PythonOperator(
            task_id=task_id,
            python_callable=import_csv_to_postgres,
            op_kwargs={'csv_file': csv_file},
            dag=dag,
        )
    
    def process_csv_files(**context):
        csv_files = context['ti'].xcom_pull(task_ids='get_csv_files')
        if not csv_files:
            logging.warning("Không tìm thấy file CSV nào!")
            return
            
        for csv_file in csv_files:
            import_task = create_import_task(csv_file)
            get_files_task >> import_task
    
    # Task tạo các nhiệm vụ import động
    process_task = PythonOperator(
        task_id='process_csv_files',
        python_callable=process_csv_files,
        provide_context=True,
    )
    
    # Thiết lập phụ thuộc
    get_files_task >> process_task