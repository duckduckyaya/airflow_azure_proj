from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
import os

default_args = {
    'owner'                 : 'airflow',
    'description'           : 'Data pipeline for immo eliza',
    'depend_on_past'        : False,
    'start_date'            : datetime(2022, 11, 21),
    'email_on_failure'      : False,
    'email_on_retry'        : False,
    'retries'               : 1,
    'retry_delay'           : timedelta(minutes=1)
}

with DAG(
    'scraping_dag',
    default_args=default_args,
    description='Scrapes data using Docker container',
    schedule_interval=timedelta(hours=1),
) as dag:
    
    start_dag = DummyOperator(
        task_id='start_dag'
        )

    end_dag = DummyOperator(
        task_id='end_dag'
        )

task_1 = DockerOperator(
    task_id='scraping',
    image='airflow_scraper:latest',
    container_name='task___scraper',
    api_version='auto',
    auto_remove=True,
    # command="/bin/sleep 30",
    docker_url="unix://var/run/docker.sock",
    network_mode="bridge",
    environment={
        "AZURE_CONNECTION_STRING": os.getenv("DefaultEndpointsProtocol=https;AccountName=yayaz;AccountKey=YglLHtJxmf3/CTPj49pwpo0IjEjZNT9CcUfRuKq/jleb7hFn8Xsrgtw+RElDOfWC54HaqPD4dvcx+AStLQS58A==;EndpointSuffix=core.windows.net"),
        "AZURE_CONTAINER_NAME": os.getenv("yayaz"),
    },
    dag=dag
)

    
start_dag >> task_1 >> end_dag
