import pendulum
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor


dag1 = DAG(
    dag_id='ingest_client_data_v2',
    start_date=pendulum.now(),
    schedule=None
)
    
dag2 = DAG(
    dag_id='create_report',
    start_date=pendulum.now(),
    schedule=None
)


EmptyOperator(task_id='copy_to_raw', dag=dag1) >> EmptyOperator(task_id='process_data', dag=dag1)


wait = ExternalTaskSensor(
    task_id='wait_for_data_processing',
    external_dag_id='ingest_client_data_v2',
    external_task_id='process_data',
    dag=dag2
)

report = EmptyOperator(task_id='create_report', dag=dag2)
wait >> report
