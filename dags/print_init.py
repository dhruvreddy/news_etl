import datetime
import logging
import requests
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),    
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    # 'retries': 1,
    # 'retry_delay': datetime.timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

URL="https://saurav.tech/NewsAPI/top-headlines/category/health/in.json"

def extract_news_data():
    url = URL
    response = requests.get(url)
    data = response.json()
    print(data)


with DAG(
    dag_id="print_init",
    description="Upload news DF to MySQL",
    default_args=default_args,
    schedule_interval="@Daily",
    catchup=False
) as dag:
    
    print_init = PythonOperator(
        task_id="init",
        python_callable=extract_news_data
    )
    
    print_init