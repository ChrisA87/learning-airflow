import pendulum
from time import sleep
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.latest_only import LatestOnlyOperator


SALES_SYSTEM_SWITCH_DATE = pendulum.datetime(2023, 7, 3, 15)


dag = DAG(
    dag_id='branching_example',
    start_date=pendulum.today('UTC').add(days=-2),
    schedule='@hourly'
)


def _fetch_weather():
    print('fetching weather data')
    sleep(1)


def _clean_weather():
    print('cleaning weather data')
    sleep(1)


def _fetch_sales_old():
    print('fetching sales data')
    sleep(2)


def _clean_sales_old():
    print('cleaning sales data')
    sleep(2)


def _fetch_sales_new():
    print('fetching sales data - the new system')
    sleep(1)


def _clean_sales_new():
    print('cleaning sales data - the new system')
    sleep(1)


def _choose_sales_system(execution_date):
    if execution_date < SALES_SYSTEM_SWITCH_DATE:
        return 'fetch_sales_data_old'
    else:
        return 'fetch_sales_data_new'


def _join_datasets():
    print('joining weather sales data')
    sleep(1)


def _train_model():
    print('training model')
    sleep(3)


def _deploy_model():
    print('deploying model')
    sleep(1)


def _upload_traing_data():
    print('uploading training data')


start = EmptyOperator(
    task_id='start',
    dag=dag
)


fetch_weather_data = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=_fetch_weather,
    dag=dag
)


fetch_sales_data_old = PythonOperator(
    task_id='fetch_sales_data_old',
    python_callable=_fetch_sales_old,
    dag=dag
)

clean_sales_old = PythonOperator(
    task_id='clean_sales_data_old',
    python_callable=_clean_sales_old,
    dag=dag
)


fetch_sales_data_new = PythonOperator(
    task_id='fetch_sales_data_new',
    python_callable=_fetch_sales_new,
    dag=dag
)

clean_sales_new = PythonOperator(
    task_id='clean_sales_data_new',
    python_callable=_clean_sales_new,
    dag=dag
)


clean_weather = PythonOperator(
    task_id='clean_weather_data',
    python_callable=_clean_weather,
    dag=dag
)


join_data = PythonOperator(
    task_id='join_datsets',
    python_callable=_join_datasets,
    dag=dag
)


upload_training_data = PythonOperator(
    task_id='upload_training_data',
    python_callable=_upload_traing_data,
    dag=dag
)


train_model = PythonOperator(
    task_id='train_model',
    python_callable=_train_model,
    dag=dag
)


deploy_model = PythonOperator(
    task_id='deploy_model',
    python_callable=_deploy_model,
    dag=dag
)


latest_only = LatestOnlyOperator(
    task_id='latest_only',
    dag=dag
)

choose_sales_system = BranchPythonOperator(
    task_id='choose_sales_system',
    python_callable=_choose_sales_system,
    dag=dag
)


join_sales_branch = EmptyOperator(
    task_id='rejoin_sales_branch',
    dag=dag,
    trigger_rule='none_failed'
)


start >> [fetch_weather_data, choose_sales_system]
choose_sales_system >> [fetch_sales_data_new, fetch_sales_data_old]
fetch_weather_data >> clean_weather
fetch_sales_data_old >> clean_sales_old
fetch_sales_data_new >> clean_sales_new
[clean_sales_new, clean_sales_old] >> join_sales_branch
[join_sales_branch, clean_weather] >> join_data >> [upload_training_data, train_model]
[latest_only, train_model] >> deploy_model
