import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

path = '/opt/airflow/dags'
default_args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2025, 1, 12), 
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=600),
}

with DAG('stude_perfo_ETL',
         description='Proses ETL pipeline untuk data influencer merchandise',
         schedule_interval='10-30/10 9 * * 6', #  Diatur tiap hari sabtu dengan interval 10 menit
         default_args=default_args,
         ) as dag:

    # Atur penjadwalan urutan kode yang dijalankan
    extract_transform = BashOperator(task_id='extract_and_transform',
                               bash_command=f'sudo -u airflow python /opt/airflow/dags/extract.py')
    load = BashOperator(task_id='load',
                               bash_command=f'sudo -u airflow python /opt/airflow/dags/load.py')
    
# Urutan eksekusi program
extract_transform >> load
