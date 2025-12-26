import requests
import base64
import json
from airflow.models import Variable

BASE_URL = Variable.get('1C_API_URL')

USERNAME = Variable.get('1C_API_USERNAME')
PASSWORD = Variable.get('1C_API_PASSWORD')

auth_str = f"{USERNAME}:{PASSWORD}".encode("utf-8")
b64_auth = base64.b64encode(auth_str).decode("utf-8")

HEADERS = {
    "Authorization": f"Basic {b64_auth}",
    "Content-Type": "application/json",
    "Source": "DWH"
}

# 1️⃣ Получаем ключ
payload = {"Имя": "Склады"}
response = requests.post(f"{BASE_URL}/query/key", headers=HEADERS, data=json.dumps(payload))

if response.status_code != 200:
    print(f"Ошибка получения ключа: {response.status_code}")
    print(response.text)
    exit()

key = response.json().get("Key")  # ключ запроса
print(f"Ключ: {key}")

# 2️⃣ Получаем данные по ключу
response = requests.get(f"{BASE_URL}/query/data", headers=HEADERS, params={"key": key})

if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=2, ensure_ascii=False))
else:
    print(f"Ошибка получения данных: {response.status_code}")
    print(response.text)