from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

current_date = datetime.now().strftime('%Y-%m-%d')

dag = DAG(
    'embulk_pipeline_csv',
    description='Pipeline para armazenar dados em formato CSV',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 29),
    catchup=False,
)

create_csv_dir = BashOperator(
    task_id='create_csv_dir',
    bash_command=f"mkdir -p /opt/airflow/data/csv/{current_date}",
    dag=dag,
)

create_postgres_dir = BashOperator(
    task_id='create_postgres_dir',
    bash_command=f"mkdir -p /opt/airflow/data/postgres/{current_date}",
    dag=dag,
)

load_csv_to_csv = BashOperator(
    task_id='load_csv_to_csv',
    bash_command=f'java -cp /opt/embulk/jaxb-api-2.3.1.jar:/opt/embulk/jaxb-impl-2.3.1.jar -jar /opt/embulk/embulk-0.9.25.jar run /opt/airflow/dags/config_load_csv.yml',
    dag=dag,
)

load_postgres_to_csv = BashOperator(
    task_id='load_postgres_to_csv',
    bash_command=f'java -cp /opt/embulk/jaxb-api-2.3.1.jar:/opt/embulk/jaxb-impl-2.3.1.jar -jar /opt/embulk/embulk-0.9.25.jar run /opt/airflow/dags/config_load_postgres.yml',
    dag=dag,
)

create_csv_dir >> create_postgres_dir >> load_csv_to_csv >> load_postgres_to_csv
