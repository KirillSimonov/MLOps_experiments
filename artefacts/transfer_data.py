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
from typing import Dict, Any
import json


LOGGER = logging.getLogger()
LOGGER.addHandler(logging.StreamHandler())

DEFAULT_ARGS = {
    'owner': 'Kirill Simonov',
    'retry': 3,
    'retry_delay': timedelta(minutes=1),    
}

dag = DAG(
    dag_id = 'read_args_get_output',
    schedule_interval = '0 1 * * *',
    start_date = days_ago(2),
    catchup = False,
    tags = ['mlops'],
    default_args = DEFAULT_ARGS
)

def read_variables(test:str)->Dict[str,Any]:
    bucket = Variable.get('S3_bucket')
    LOGGER.info(bucket)
    LOGGER.info(f'read input argument: {test}')
    #now create output
    metrics = {}
    metrics['RMSE'] = 1
    return metrics

def use_s3(**kwargs)-> None:
    s3_hook = S3Hook('yc-s3')
    ti = kwargs['ti']
    metrics = ti.xcom_pull(task_ids='read_variables')
    metrics['MSE'] = 2
    buffer = io.BytesIO()
    buffer.write(json.dumps(metrics).encode())
    buffer.seek(0)
    s3_hook.load_file_obj(file_obj=buffer,
                          key='datasets/metrics.json.pkl',
                          bucket_name=Variable.get('S3_bucket'))
    LOGGER.info(f'loaded df to s3')

                    


task_read_variables = PythonOperator(
    task_id='read_variables',
    python_callable=read_variables,
    dag=dag,
    op_kwargs={"test":"privet!"})

task_use_s3 = PythonOperator(
    task_id='use_s3',
    python_callable=use_s3,
    dag=dag,
    provide_context=True
)

task_read_variables >> task_use_s3