import os

from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator



def get_utils_tuxedo_lawsuits_prod_gcs_path():
      return get_data_prod_gcs_path("gs://tuxedo_lawsuits_prod", "tuxedo")


def get_data_prod_gcs_path(prefix, task_name):
    def _get_year_from_date(date: str):
            return date[:4]

    def _get_month_from_date(date: str):
            return date[5:7]

    def _format_tuxedo_lawsuits_prod_gcs_path(date, y,m):
        storage_execution_date_path = f"storage_execution_date={date}"
        created_year = f"_created_at.year={y}"
        created_month = f"_created_at.month={m}"

        return os.path.join(
            prefix,
            storage_execution_date_path,
            created_year,
            created_month,
        )
    
    run_get_tuxedo_lawsuits_objects_task = ["storage=2023-01-06","storage=2023-02-06"]
    latest_date_patterns = "|".join(["\\d{8}", "\\d{4}-\\d{2}-\\d{2}"])
    run_get_latest_date_task = BashOperator(
        task_id=f"run_get_latest_date_{task_name}",
        bash_command=(
                f"printf '%s\\n' '{run_get_tuxedo_lawsuits_objects_task}'"
                f" | grep -P '{latest_date_patterns}' -o | sort | tail -n 1"
        ),
        do_xcom_push=True
    )

    get_year_from_date_task = PythonOperator(
        task_id=f"get_year_from_date_{task_name}",
        provide_context=True,
        do_xcom_push=True,
        python_callable=_get_year_from_date,
        op_args=[run_get_latest_date_task.output]
    )

    get_month_from_date_task = PythonOperator(
        task_id=f"get_month_from_date_{task_name}",
        provide_context=True,
        do_xcom_push=True,
        python_callable=_get_month_from_date,
        op_args=[run_get_latest_date_task.output]
    )

    format_output_task = PythonOperator(
        task_id=f"get_format_output_{task_name}",
        provide_context=True,
        do_xcom_push=True,
        python_callable=_format_tuxedo_lawsuits_prod_gcs_path,
        op_args=[run_get_latest_date_task.output, get_year_from_date_task.output, get_month_from_date_task.output]
    )

    return run_get_latest_date_task >> [get_year_from_date_task, get_month_from_date_task] >> format_output_task
