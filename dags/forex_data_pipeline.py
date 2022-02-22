from asyncio import tasks
from airflow import DAG
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.python import PythonOperator
#from airflow.operators.email import EmailOperator
from functions.aws_s3 import create_bucket, store_rates_file
from datetime import datetime, timedelta
from botocore.exceptions import ClientError
import csv
import requests
import json
import boto3
import logging
import os

default_args = {
    "owner": "airflow",
    "email_on_failure": True,
    "email_on_retry": True,
    "email": "hailson.fsj@gmail.com",
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

def download_rates():
    BASE_URL = "https://gist.githubusercontent.com/marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b/raw/"
    ENDPOINTS = {
        'USD': 'api_forex_exchange_usd.json',
        'EUR': 'api_forex_exchange_eur.json'
    }
    with open('/opt/airflow/dags/files/forex_currencies.csv') as forex_currencies:
        reader = csv.DictReader(forex_currencies, delimiter=';')
        for idx, row in enumerate(reader):
            base = row['base']
            with_pairs = row['with_pairs'].split(' ')
            indata = requests.get(f"{BASE_URL}{ENDPOINTS[base]}").json()
            outdata = {'base': base, 'rates': {}, 'last_update': indata['date']}
            for pair in with_pairs:
                outdata['rates'][pair] = indata['rates'][pair]
            with open('/opt/airflow/dags/files/forex_rates.json', 'a') as outfile:
                json.dump(outdata, outfile)
                outfile.write('\n')

def _get_message() -> str:
    return "Hi from forex data pipeline"

with DAG("forex_data_pipeline", start_date=datetime(2022, 2, 8), schedule_interval="@daily", default_args=default_args, catchup=False) as dag:

    is_forex_rates_available = HttpSensor(
        task_id="is_forex_rates_available",
        http_conn_id="forex_api",
        endpoint="marclamberti/f45f872dea4dfd3eaa015a4a1af4b39b",
        response_check=lambda response: "rates" in response.text,
        poke_interval=5,
        timeout=20
    )

    is_forex_rates_file_available = FileSensor(
        task_id="is_forex_rates_file_available",
        fs_conn_id="forex_path",
        filepath="forex_currencies.csv",
        poke_interval=5,
        recursive=True,
        timeout=20
    )

    downloading_rates_file = PythonOperator(
        task_id="downloading_rates_file",
        python_callable=download_rates
    )

    creating_s3_bucket = PythonOperator(
        task_id="creating_s3_bucket",
        python_callable=create_bucket,
        op_args={'airflow-hailson-bucket'}
    )

    saving_rates = PythonOperator(
        task_id="saving_rates",
        python_callable=store_rates_file,
        op_args={'airflow-hailson-bucket', '/opt/airflow/dags/files/forex_rates.json', 'forex_rates.json'},
        retries=10
    )

    mysql_db = MySqlOperator(
        task_id="mysql_db",
        sql="mysql/db.sql",
        mysql_conn_id="mysql_conn"
    )

    #send_email_notification = EmailOperator(
    #    task_id="send_email_notification",
    #    to="hailson.fsj@gmail.com",
    #    subject="Airflow Forex Data Pipeline",
    #    html_content="<h3>Forex Data Pipeline</h3>"
    #)

    send_slack_notification = SlackWebhookOperator(
        task_id="send_slack_notification",
        http_conn_id="slack_conn",
        message=_get_message(),
        channel="#monitoring"
    )

    is_forex_rates_available >> is_forex_rates_file_available >> downloading_rates_file >> creating_s3_bucket 
    creating_s3_bucket >> saving_rates >> mysql_db >> send_slack_notification