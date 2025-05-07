# News Crawler Airflow System

Hệ thống thu thập tin tức tự động sử dụng Apache Airflow và Docker.

## Tính năng

- Thu thập tin tức tự động từ VNExpress (có thể mở rộng thêm nguồn)
- Lập lịch thu thập theo giờ/ngày
- Lưu trữ dữ liệu vào PostgreSQL
- Xử lý dữ liệu thô và trích xuất nội dung
- Containerized với Docker và Docker Compose

## Yêu cầu

- Docker và Docker Compose
- Python 3.9+

## Cài đặt

1. Clone repository:
   ```bash
   git clone https://github.com/yourusername/news-crawler-airflow.git
   cd news-crawler-airflow
   ```

2. Tạo file môi trường:
   ```bash
   cp .env.example .env
   ```

3. Chỉnh sửa các biến trong file `.env` theo cấu hình mong muốn.

4. Khởi động hệ thống:
   ```bash
   docker-compose up -d
   ```

5. Airflow UI sẽ khả dụng tại http://localhost:8080
   - Username: airflow
   - Password: airflow

## Cấu trúc dự án

```
news-crawler-airflow/
│
├── README.md                        # Hướng dẫn cài đặt và sử dụng
├── requirements.txt                 # Các thư viện cần thiết
├── docker-compose.yml              # Docker Compose cho Airflow, PostgreSQL
├── .env                            # Biến môi trường
│
├── dags/                           # DAGs cho Airflow
│   └── news_crawler_dag.py        # DAG chính chạy các crawler
│
├── crawlers/                       # Crawler từng nguồn tin
│   ├── base.py                     # BaseCrawler: xử lý fetch, parse, save
│   ├── vnexpress.py                # Crawler cho VNExpress
│   └── utils.py                    # Hàm tiện ích như parse HTML, chuẩn hóa
│
├── data/                           # Dữ liệu lưu tạm
│   ├── raw/                        # HTML thô từ crawler
│   └── processed/                  # Dữ liệu đã xử lý
│
├── airflow/                        
│   └── airflow.cfg                 # Cấu hình Airflow (nếu tùy chỉnh)
│
├── scripts/
│   ├── setup.sh                    # Cài đặt hệ thống
│   └── init_postgres.sql          # Script tạo bảng PostgreSQL
│
└── models/                         # Mô hình dữ liệu
    └── article.py                 # Model bài báo (SQLAlchemy hoặc dataclass)
```

## Sử dụng

### Thêm nguồn tin mới

1. Tạo một lớp crawler mới trong thư mục `crawlers/`, kế thừa từ `BaseCrawler`
2. Thêm crawler mới vào `news_crawler_dag.py`

### Xem dữ liệu đã thu thập

Kết nối đến PostgreSQL:
```bash
docker-compose exec postgres psql -U airflow -d airflow
```

Truy vấn dữ liệu:
```sql
SELECT * FROM articles ORDER BY published_date DESC LIMIT 10;
```

## Giải quyết sự cố

Xem log của Airflow:
```bash
docker-compose logs -f webserver
docker-compose logs -f scheduler
```

Kiểm tra trạng thái các container:
```bash
docker-compose ps
```

## Giấy phép

MIT