from airflow.models import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from datetime import timedelta

import sys 
import os 
import yaml
os.environ['SPARK_HOME']
sys.path.append("/home/wr95/Pinterest_App/Pinterest-Data-Pipeline") # Replace this pathway with the pathway to where the SparkCleaning Module is kept 
from s3_to_spark_cleaning import SparkCleaning

#TODO: Implement yaml file config in default_args 

# with open('default_args_config.yaml') as file:
#     creds = yaml.safe_load(file)

default_args = {
    'owner': 'Wayne',
    'depends_on_past': False,
    'email': [], # Place your email address inside the square brackets 
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'start_date': datetime(2022, 7, 21),
    'retry_delay': timedelta(minutes=5),
    'end_date': datetime(2022, 7, 23),
}

spark_session = SparkCleaning()


with DAG(dag_id='dag_spark',
         default_args=default_args,
         schedule_interval='5 * * * *',
         catchup=False,
         tags=['spark']
         ) as dag:
    # define task
    Spark_task = PythonOperator(
        task_id = 'run_spark_job',
        python_callable = spark_session.run_spark_etl,
        )