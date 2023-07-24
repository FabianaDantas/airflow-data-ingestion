import os

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from dags_ingestion_dag.utils import get_utils_tuxedo_lawsuits_prod_gcs_path


default_args = {
    'owner': 'airflow',
    'start_date': days_ago(5)
}

def show_latest_date_2(b,**kwargs):
    print(f"Latest date path: {b}")

with DAG(
    'get_latest_date_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:
    utils = get_utils_tuxedo_lawsuits_prod_gcs_path()

    some_task = PythonOperator(
        task_id="some_task_that_uses_lawsuit_prod_gcs_path",
        provide_context=True,
        do_xcom_push=True,
        python_callable=lambda x: print(f"Latest date path: {x}"),
        op_args=[utils.output]
    )

    utils >> some_task


