from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
import requests, base64, json


@dag(
    dag_id="get_1c_warehouses",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["1c", "api", "dwh"]
)
def get_1c_warehouses():

    @task
    def fetch_key():
        BASE_URL = Variable.get('1C_API_URL')
        USERNAME = Variable.get('1C_API_USERNAME')
        PASSWORD = Variable.get('1C_API_PASSWORD')

        auth = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/json",
            "Source": "DWH"
        }

        payload = {"Имя": "Склады"}
        r = requests.post(f"{BASE_URL}/query/key", headers=headers, json=payload)
        r.raise_for_status()
        return r.json()["Key"]

    @task
    def fetch_data(key: str):
        BASE_URL = Variable.get('1C_API_URL')
        r = requests.get(f"{BASE_URL}/query/data", params={"key": key})
        r.raise_for_status()
        print(json.dumps(r.json(), indent=2, ensure_ascii=False))

    fetch_data(fetch_key())


get_1c_warehouses()
