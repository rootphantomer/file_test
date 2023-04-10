from airflow import DAG
from airflow.providers.apache.hive.operators.hive import HiveOperator
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'start_date': days_ago(1),

    'retries': 10,
    'retry_delay': timedelta(seconds=5),

}

dag = DAG('test_hive2', default_args=default_args, schedule_interval='*/1 * * * *',  catchup=False)

t1 = HiveOperator(
    task_id='hive_task',
    hql='select * from test.data_demo',
    dag=dag)
t1