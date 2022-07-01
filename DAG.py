from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from datetime import datetime
from main import Run


default_args = {
    "owner": "airflow",
    "start_date": datetime(2022,7,1),
    "depends_on_past": False,
    "email": "admin@admin.com",
    "email_on_retry": False,
    "retries": 0, 
}

with DAG ( dag_id="weather_data",default_args=default_args,schedule_interval='0 * * * *') as dag:

    start = DummyOperator(task_id="start")

    weather_download = PythonOperator(
        task_id="download_weather",
        python_callable=Run,
        dag=dag
    )

    complete = DummyOperator(task_id="complete")

start >> weather_download >> complete






