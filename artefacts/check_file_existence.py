from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import DAG, Variable

# Параметры вашего S3 хранилища
FILE_KEY = 'words_subset.txt'
def check_file_exists():
    hook = S3Hook(aws_conn_id='yc-s3')  # Замените на ваш идентификатор подключения
    exists = hook.check_for_key(key=FILE_KEY, bucket_name=Variable.get('S3_bucket'))
    if exists:
        print(f"Файл {FILE_KEY} существует в бакете {Variable.get('S3_bucket')}.")
    else:
        print(f"Файл {FILE_KEY} не найден в бакете {Variable.get('S3_bucket')}.")

with DAG(
    dag_id='check_s3_file_exists',
    schedule_interval='@once',
    start_date=datetime(2024, 11, 19),
    catchup=False,
) as dag:
    
    check_file_task = PythonOperator(
        task_id='check_file_exists_task',
        python_callable=check_file_exists,
    )

check_file_task