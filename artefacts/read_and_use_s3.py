import logging
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG, Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook 
import pandas as pd
import numpy as np
import pickle
import io


LOGGER = logging.getLogger()
LOGGER.addHandler(logging.StreamHandler())

DEFAULT_ARGS = {
    'owner': 'Kirill Simonov',
    'retry': 3,
    'retry_delay': timedelta(minutes=1),    
}

dag = DAG(
    dag_id = 'variable_example',
    schedule_interval = '0 1 * * *',
    start_date = days_ago(2),
    catchup = False,
    tags = ['mlops'],
    default_args = DEFAULT_ARGS
)

def read_variables():
    bucket = Variable.get('S3_bucket')
    LOGGER.info(bucket)

def use_s3():
    s3_hook = S3Hook('yc-s3')
    dataset = s3_hook.download_file(key='datasets/test_df.pkl',
                          bucket_name=Variable.get('S3_bucket'))
    #pd.read_pickle() does not work due to error withn numpy
    with open(dataset, 'rb') as file:
        df = pickle.load(file)
    LOGGER.info(f'dataframe columns: {df.columns}')
    #Now load file back to s3
    buffer = io.BytesIO()
    df.to_pickle(buffer)
    buffer.seek(0)
    s3_hook.load_file_obj(file_obj=buffer,
                          key='datasets/df_from_dag.pkl',
                          bucket_name=Variable.get('S3_bucket'))
    LOGGER.info(f'loaded df to s3')

                    


task_read_variables = PythonOperator(
    task_id='read_variables',
    python_callable=read_variables,
    dag=dag)

task_use_s3 = PythonOperator(
    task_id='use_s3',
    python_callable=use_s3,
    dag=dag)

task_read_variables >> task_use_s3