import datetime

import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.mysql.operators.mysql import MySqlOperator

from news.news_json_to_df import news_json_to_df

my_sql_conn = "my_sql_conn"

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

def print_init():
    print("Init")

def store_df_in_mysql(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='get_news')
    
    mysql_hook = MySqlHook(mysql_conn_id=my_sql_conn)
    conn = mysql_hook.get_conn()
    cursor = conn.cursor()
    
    sql = """
        INSERT INTO news_articles (source_name, author, title, description, url, url_to_image, published_at, content)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    for index, row in df.iterrows():
        cursor.execute(sql, (
            row['source']['name'],
            row['author'],
            row['title'],
            row['description'],
            row['url'],
            row['urlToImage'],
            row['publishedAt'],
            row['content']
        ))
    
    conn.commit()
    cursor.close()
    conn.close()
    
    
with DAG(
    dag_id="upload_new_to_mysql_v1",
    description="Upload news DF to MySQL",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    print_init = PythonOperator(
        task_id="init",
        python_callable=print_init
    )

    create_table_task = MySqlOperator(
        task_id='mysql_create_table',
        mysql_conn_id=my_sql_conn,
        sql="""
            CREATE TABLE IF NOT EXISTS news_articles (
                id INT AUTO_INCREMENT,
                source_name VARCHAR(255),
                author VARCHAR(255),
                title VARCHAR(255),
                description TEXT,
                url VARCHAR(255),
                url_to_image VARCHAR(255),
                published_at TIMESTAMP,
                content TEXT,
                PRIMARY KEY (id)
            );
        """,
    )

    get_news = PythonOperator(
        task_id="get_news",
        python_callable=news_json_to_df,
    )

    store_df = PythonOperator(
        task_id="store_df",
        python_callable=store_df_in_mysql,
    )

    print_init
    create_table_task >> get_news >> store_df