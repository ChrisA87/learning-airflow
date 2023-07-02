import gzip
import json
import pendulum
from datetime import timedelta
from urllib import request
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


dag = DAG(
    dag_id='stock_wiki_pageviews',
    start_date=pendulum.today('UTC'),
    schedule_interval='@hourly',
    template_searchpath='/tmp'
)


def _get_data(execution_date, output_path):
    data_date = execution_date - timedelta(hours=3)
    url = (
        'https://dumps.wikimedia.org/other/pageviews/'
        f'{data_date.year}/'
        f'{data_date.strftime("%Y-%m")}/'
        f'pageviews-{data_date.strftime("%Y%m%d-%H")}0000.gz'
    )
    print(f'attempting to read data from {url}')
    request.urlretrieve(url, output_path)
    print(f'Downloaded {url} to {output_path}')


def _process_data(pagenames, input_path, output_path):
    result = dict.fromkeys(pagenames, 0)
    with gzip.open(input_path, 'r') as f:
        for line in f:
            _, page, count, _ = line.decode().split(' ')
            if page in pagenames:
                result[page] += int(count)

    print(result)

    with open(output_path, 'w+') as f:
        json.dump(result, f)


def _generate_sql_query(input_path, output_path, execution_date):
    with open(input_path, 'r') as f:
        data = json.load(f)

    query = 'INSERT INTO pageview_counts\nVALUES\n'
    for stock, count in data.items():
        query += f"\t('{stock}', {count}, '{execution_date}'),\n"
    query = query.strip(',\n')
    query += '\n;'

    print(query)

    with open(output_path, 'w+') as f:
        f.write(query)


get_data = PythonOperator(
    task_id='get_data',
    python_callable=_get_data,
    op_kwargs={
        'output_path': '/tmp/pageviews.gz'
    },
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
        },
        'input_path': '/tmp/pageviews.gz',
        'output_path': '/tmp/pageview_counts_{{ ts_nodash }}.json'
    }
)


generate_sql_query = PythonOperator(
    task_id='generate_sql_query',
    python_callable=_generate_sql_query,
    op_kwargs={
        'input_path': '/tmp/pageview_counts_{{ ts_nodash }}.json',
        'output_path': '/tmp/insert_query.sql'
    }
)


write_to_postgres = PostgresOperator(
    task_id='write_to_postgres',
    postgres_conn_id='postgres_connection',
    sql='insert_query.sql',
    dag=dag
)


get_data >> process_data >> generate_sql_query >> write_to_postgres
