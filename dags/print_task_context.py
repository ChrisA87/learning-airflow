import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id='print_task_context',
    start_date=pendulum.today('UTC').add(days=-3),
    schedule_interval='@daily'
)


def _print_context(**context):
    print(context)


print_context = PythonOperator(
    task_id='print_context',
    python_callable=_print_context,
    dag=dag
)


print_context
