from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import json
from pandas import json_normalize
from datetime import datetime

def _process_joke(ti):
    joke = ti.xcom_pull(task_ids="extract_joke")
    print('=========================================')
    print('user type - ', type(joke))
    print(joke)
    print('=========================================')
    processed_joke = json_normalize({
        'id': joke['id'],
        'type': joke['type'],
        'setup': joke['setup'],
        'punchline': joke['punchline']})
    processed_joke.to_csv('/tmp/processed_jokes.csv', index=None, header=False)

def _store_joke():
    hook = PostgresHook(postgres_conn_id='jokes_api')
    hook.copy_expert(sql="COPY jokes FROM stdin WITH DELIMITER ',' ",
    filename='/tmp/processed_jokes.csv')

with DAG('jokes_accumulator',
         start_date=datetime(2022, 1, 1),
         schedule_interval='*/15 13-15 * * 2,4', catchup=False) as dag:
    create_table = PostgresOperator(
        postgres_conn_id='postgres',
        task_id='create_table',
        sql='''
            CREATE TABLE IF NOT EXISTS jokes(
            id INTEGER UNIQUE, 
            type VARCHAR(200), 
            setup TEXT NOT NULL, 
            punchline TEXT NOT NULL);''')

    is_api_available = HttpSensor(
        task_id="is_api_available",
        http_conn_id='jokes_api',
        endpoint='/random_joke'
    )

    extract_joke = SimpleHttpOperator(
        task_id='extract_joke',
        http_conn_id='jokes_api',
        endpoint='/random_joke',
        method='GET',
        response_filter= lambda response: json.loads(response.text),
        log_response = True
    )

    process_joke = PythonOperator(
        task_id='process_joke',
        python_callable=_process_joke
    )

    store_joke = PythonOperator(
        task_id='store_joke',
        python_callable=_store_joke
    )

    create_table >> is_api_available >> extract_joke >> process_joke >> store_joke