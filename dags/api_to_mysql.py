import logging
from datetime import datetime, timedelta
from airflow.decorators import task, dag
from airflow.operators.bash import BashOperator

from src.utils.db_connection import connect_to_weather_data_database, create_daily_weather_table
from src.extract.fetch_api import fetch_weather_data_from_api
from src.transform.clean_data import clean_weather_data
from src.load.load_to_database import load_weather_data_to_db

default_args = {
    'owner': 'Matasit',
    'depends_on_past': False,
    'email': ['gotji.matasit@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

# task1 - Connect to database
@task
def connect_to_mysql_database():
    connect_to_weather_data_database()


# task2 - Create table in database
@task
def create_mysql_table():
    create_daily_weather_table()


# task3 - Extracting data
@task
def extract_weather_data():
    fetch_weather_data_from_api()


# task4 - Transforming and load data to database
@task
def transform_and_load_weather():
    data = fetch_weather_data_from_api()
    df_cleaned = clean_weather_data(data)
    load_weather_data_to_db(df_cleaned)
    

# DAG
@dag(
    dag_id = "my_testing_dag",
    default_args=default_args, 
    schedule='@daily', 
    start_date=datetime(2026,2,1), 
    tags=["workshop"]
)


def project_pipeline():

    connect = connect_to_mysql_database()
    create_table = create_mysql_table()

    extract = extract_weather_data()
    transform_load = transform_and_load_weather()

    connect >> create_table >> extract >> transform_load

dag = project_pipeline()