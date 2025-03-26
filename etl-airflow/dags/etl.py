from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd

def extract():
    data = {"name": ["Alice", "Bob"], "age": [25, 30]}
    df = pd.DataFrame(data)
    df.to_csv("/opt/airflow/dags/extracted_data.csv", index=False)

def transform():
    df = pd.read_csv("/opt/airflow/dags/extracted_data.csv")
    df["age"] = df["age"] + 5
    df.to_csv("/opt/airflow/dags/transformed_data.csv", index=False)

def load():
    df = pd.read_csv("/opt/airflow/dags/transformed_data.csv")
    print("Loading data:")
    print(df)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 3, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG("simple_etl", default_args=default_args, schedule_interval="@daily")

extract_task = PythonOperator(task_id="extract", python_callable=extract, dag=dag)
transform_task = PythonOperator(task_id="transform", python_callable=transform, dag=dag)
load_task = PythonOperator(task_id="load", python_callable=load, dag=dag)

extract_task >> transform_task >> load_task
