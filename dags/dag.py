from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'depends_on_past': False,
    'start_date': datetime(2021, 11, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('demo-dag', default_args=default_args, schedule_interval=timedelta(days=1))

def print_context(ds, **kwargs):
    """Print the Airflow context and ds variable from the context."""
    print(ds)
    print(str(kwargs))
    print('test yo!')
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='task1',
    provide_context=True,
    python_callable=print_context,
    dag=dag,
)