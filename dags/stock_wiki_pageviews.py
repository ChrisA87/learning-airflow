import gzip
import json
import pendulum
from datetime import timedelta
from urllib import request
from urllib.error import HTTPError
from airflow import DAG
from airflow.operators.python import PythonOperator


dag = DAG(
    dag_id='stock_wiki_pageviews',
    start_date=pendulum.today('UTC').add(days=-1),
    schedule_interval='@hourly'
)


def _get_data(execution_date):
    data_date = execution_date - timedelta(hours=3)
    url = (
        'https://dumps.wikimedia.org/other/pageviews/'
        f'{data_date.year}/'
        f'{data_date.strftime("%Y-%m")}/'
        f'pageviews-{data_date.strftime("%Y%m%d-%H0000")}.gz'
    )
    file_name = url.split('/')[-1]
    output_path = f'/tmp/{file_name}'
    print(f'attempting to read data from {url}')
    try:
        request.urlretrieve(url, output_path)
        print(f'Downloaded {url} to {output_path}')
    except HTTPError as exc:
        print(f'data is not available - {exc}')


def _process_data(pagenames, execution_date):
    data_date = execution_date - timedelta(hours=3)
    result = dict.fromkeys(pagenames, 0)
    with gzip.open(f'/tmp/pageviews-{data_date.strftime("%Y%m%d-%H0000")}.gz', 'r') as f:
        for line in f:
            _, page, count, _ = line.decode().split(' ')
            if page in pagenames:
                result[page] += int(count)

    print(result)

    with open(f'/tmp/stock_pageviews_{data_date.strftime("%Y%m%d%H")}.json', 'w+') as f:
        json.dump(result, f)


get_data = PythonOperator(
    task_id='get_data',
    python_callable=_get_data,
    dag=dag
)


process_data = PythonOperator(
    task_id='process_data',
    python_callable=_process_data,
    op_kwargs={
        'pagenames': {
            'Google',
            'Amazon',
            'Facebook',
            'Apple',
            'Microsoft'
        }
    }
)


get_data >> process_data
