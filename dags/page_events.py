from pathlib import Path
import pandas as pd

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id='page_events',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval='@daily'
)


fetch_events = BashOperator(
    task_id='fetch_events',
    bash_command=(
        'mkdir -p /data && '
        'curl -o /data/events/{{ds}}.json '
        'https://localhost:5000/events?'
        'start_date={{ds}}&'
        'end_date={{next_ds}}'
    ),
    dag=dag
)


def _calculate_stats(**context):
    input_path = context['templates_dict']['input_path']
    output_path = context['templates_dict']['output_path']

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    events = pd.read_json(input_path)
    stats = events.groupby(['date', 'user'], as_index=False).size()
    stats.to_csv(output_path, index=False)


calculate_stats = PythonOperator(
    task_id='calculate_stats',
    python_callable=_calculate_stats,
    templates_dict={
        'input_path': '/data/events/{{ds}}.json',
        'output_path': 'data/stats/{{ds}}.csv'},
    dag=dag
)


fetch_events >> calculate_stats
