"""
Suppose there is a system which pulls together multiple datasets
which are delivered by third parties, all at different, non-standard times.
We can use polling to check for the arrival of files and trigger other tasks
accordingly.
"""

import airflow
import pendulum
from pathlib import Path
from airflow import DAG
from airflow.sensors.python import PythonSensor
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id='polling_sensor_example',
    start_date=pendulum.now(),
    schedule=None
)


def _wait_for_dataset(client_id):
    data_path = Path(f"/data/{client_id}")
    data_files = data_path.glob("data-*.csv")
    success_file = data_path / "_SUCCESS"
    return data_files and success_file.exists()


def _copy_client_data(client_id):
    print(f"copying {client_id} data")


def _process_client_data(client_id):
    print(f'processing {client_id} data')


def _calculate_metrics():
    print(f'calculating metrics')


wait_for_client_1 = PythonSensor(
    task_id=f'wait_for_client_1',
    python_callable=_wait_for_dataset,
    op_kwargs={'client_id': 'client1'},
    dag=dag
)


wait_for_client_2 = PythonSensor(
    task_id=f'wait_for_client_2',
    python_callable=_wait_for_dataset,
    op_kwargs={'client_id': 'client2'},
    dag=dag
)


wait_for_client_3 = PythonSensor(
    task_id=f'wait_for_client_3',
    python_callable=_wait_for_dataset,
    op_kwargs={'client_id': 'client3'},
    dag=dag
)


copy_client_1 = PythonOperator(
    task_id='copy_client_1_data',
    python_callable=_copy_client_data,
    op_kwargs={'client_id': 'client1'},
    dag=dag
)


copy_client_2 = PythonOperator(
    task_id='copy_client_2_data',
    python_callable=_copy_client_data,
    op_kwargs={'client_id': 'client2'},
    dag=dag
)


copy_client_3 = PythonOperator(
    task_id='copy_client_3_data',
    python_callable=_copy_client_data,
    op_kwargs={'client_id': 'client3'},
    dag=dag
)


process_client_1_data = PythonOperator(
    task_id='process_client_1_data',
    python_callable=_process_client_data,
    op_kwargs={'client_id': 'client1'},
    dag=dag
)


process_client_2_data = PythonOperator(
    task_id='process_client_2_data',
    python_callable=_process_client_data,
    op_kwargs={'client_id': 'client2'},
    dag=dag
)


process_client_3_data = PythonOperator(
    task_id='process_client_3_data',
    python_callable=_process_client_data,
    op_kwargs={'client_id': 'client3'},
    dag=dag
)


metrics = PythonOperator(
    task_id='calculate_metrics',
    python_callable=_calculate_metrics,
    dag=dag
)


wait_for_client_1 >> copy_client_1 >> process_client_1_data
wait_for_client_2 >> copy_client_2 >> process_client_2_data
wait_for_client_3 >> copy_client_3 >> process_client_3_data
[process_client_1_data, process_client_2_data, process_client_3_data] >> metrics
