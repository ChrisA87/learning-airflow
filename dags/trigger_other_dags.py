import airflow
import pendulum
from pathlib import Path
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


dag1 = DAG(
    dag_id='ingest_client_data',
    start_date=pendulum.now(),
    schedule=None
)

for client_id in ['client1', 'client2', 'client3']:
    trigger_create_metrics_dag = TriggerDagRunOperator(
        task_id=f'trigger_create_metrics_for_{client_id}',
        trigger_dag_id='create_metrics',
        dag=dag1
    )
    
dag2 = DAG(
    dag_id='create_metrics',
    start_date=pendulum.now(),
    schedule=None
)
