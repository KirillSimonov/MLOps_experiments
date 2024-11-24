from airflow.models import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator

DEFAULT_ARGS = {
    'owner': 'Kirill Simonov',
    'email': 'kirillsimonov.v@gmail.com',
    'email_on_failure': True,
    'email_on_retry': False,
    'retry': 3,
    'retry_delay': timedelta(minutes=1),    
}

dag = DAG(
    dag_id = 'mlops_dag_1',
    schedule_interval = '0 1 * * *',
    start_date = days_ago(2),
    catchup = False,
    tags = ['mlops'],
    default_args = DEFAULT_ARGS
)

def init():
    print('Hello World')

task_init = PythonOperator(task_id='init',
                           python_callable=init,
                           dag=dag)
task_init