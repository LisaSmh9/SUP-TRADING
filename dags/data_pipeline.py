from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from main import fetch_intraday_data, save_to_database

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 6, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'intraday_data_pipeline',
    default_args=default_args,
    description='A simple data pipeline for intraday trading data',
    schedule_interval=timedelta(days=1),
)

def fetch_and_save_data():
    excel_path = fetch_intraday_data()
    save_to_database(excel_path)

fetch_save_data_task = PythonOperator(
    task_id='fetch_save_data',
    python_callable=fetch_and_save_data,
    dag=dag,
)

fetch_save_data_task
