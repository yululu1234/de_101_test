from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

from daily import return_daily_drives_and_failed_drives
from yearly import return_yearly_failed_drives

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 14),
    'retries': 1,
}

daily_dag = DAG(
    'process_daily__files_dag',
    default_args=default_args,
    schedule='@daily',
)

yearly_dag = DAG(
    'process_yearly_files_dag',
    default_args=default_args,
    schedule='@yearly',
)

daily_process_files_task = PythonOperator(
    task_id='daily_process_files',
    python_callable=return_daily_drives_and_failed_drives,
    op_kwargs={
        'input_folder': '/Users/lulu.yu/Downloads/DATA_ENGINEERING_101',
        'column_name': 'failure',
        'filter_value': 1
    },
    dag=daily_dag
)

yearly_process_files_task = PythonOperator(
    task_id='yearly_process_files',
    python_callable=return_yearly_failed_drives,
    op_kwargs={
        'input_folder': '/Users/lulu.yu/Downloads/DATA_ENGINEERING_101',
        'column_name': 'failure',
        'filter_value': 1
    },
    dag=yearly_dag
)


daily_process_files_task
yearly_process_files_task
