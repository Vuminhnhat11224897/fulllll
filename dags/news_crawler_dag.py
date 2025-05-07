from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Import crawler classes
from crawlers.vnexpress import VNExpressCrawler
from crawlers.tienphong import TienPhongCrawler

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def crawl_vnexpress():
    crawler = VNExpressCrawler()
    crawler.crawl(pages=2, max_articles=10)

def crawl_tienphong():
    crawler = TienPhongCrawler()
    crawler.crawl(pages=2, max_articles=10)

with DAG(
    dag_id='daily_news_crawler',
    default_args=default_args,
    description='DAG to crawl news from VNExpress and TienPhong',
    schedule_interval='0 7 * * *',  # 7:00 AM every day
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['news', 'crawler'],
) as dag:

    vnexpress_task = PythonOperator(
        task_id='crawl_vnexpress',
        python_callable=crawl_vnexpress,
    )

    tienphong_task = PythonOperator(
        task_id='crawl_tienphong',
        python_callable=crawl_tienphong,
    )

    vnexpress_task >> tienphong_task  # hoặc chạy song song: [vnexpress_task, tienphong_task]
