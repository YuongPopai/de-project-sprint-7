from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess


def create_user_data():
    subprocess.run(['python', 'C:\Users\User\Desktop\de-project-sprint-7\src\scripts\create_user_data.py'], check=True)


def create_user_recomendations():
    subprocess.run(['python', 'C:\Users\User\Desktop\de-project-sprint-7\src\scripts\create_user_recomendations.py'], check=True)


def create_zones_data():
    subprocess.run(['python', 'C:\Users\User\Desktop\de-project-sprint-7\src\scripts\create_zones_data.py'], check=True)


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 25),
    'retries': 1,
}

dag = DAG(
    'data_processing_dag',
    default_args=default_args,
    schedule_interval='@daily',
)

task1 = PythonOperator(
    task_id='run_user_data_processor',
    python_callable=create_user_data,
    dag=dag,
)

task2 = PythonOperator(
    task_id='run_recommendation_data_processor',
    python_callable=create_user_recomendations,
    dag=dag,
)

task3 = PythonOperator(
    task_id='run_zone_data_processor',
    python_callable=create_zones_data,
    dag=dag,
)

task1 >> task2 >> task3
