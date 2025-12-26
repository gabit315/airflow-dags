import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from psycopg2.extras import execute_batch
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dateutil.parser import parse
from airflow.models import Variable

PG_SCHEMA = "1c"
PG_TABLE = "Онлайн.Магазин"

BATCH_SIZE = 1000  # размер чанка для вставки

def parse_date(date_str, target_type="date"):
    if not date_str:
        return None
    try:
        parsed = parse(date_str, dayfirst=True)
        if target_type == "date":
            return parsed.date()
        elif target_type == "time":
            return parsed.time()
        elif target_type == "timestamp":
            return parsed
    except (ValueError, TypeError):
        return None

def fetch_data(start_date, end_date):
    url = "http://192.168.7.46:24025/AstInetShop/hs/GetOrder/getordersinfo"
    payload = {
        "date1": start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "date2": end_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    }
    username = "ws"
    password = Variable.get("1C_API_PASSWORD")

    response = requests.post(url, json=payload, auth=HTTPBasicAuth(username, password))
    response.raise_for_status()
    data = response.json()

    # API возвращает {"success": True, "data": [...]}
    if isinstance(data, dict) and "data" in data:
        return data["data"]
    if isinstance(data, list):
        return data
    return []

def load_to_postgres(ti):
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DBNAME,
        user=PG_USER, password=PG_PASSWORD
    )
    cursor = conn.cursor()

    insert_query = f"""
    INSERT INTO "{PG_SCHEMA}"."{PG_TABLE}" (
        "ТипКонтрагент", "ТелефонПокупателя", "ИсточникЗаказа", "НомерЗаказаИсточник",
        "НомерЗаказаПокупателя", "ДатаЗаказаПокупателя", "ВремяЗаказаПокупателя",
        "ДатаОтгрузкиЗаказаПокупателя", "НоменклатураКод", "РазмещениеСклада",
        "ПодразделениеКод", "ВидПлатежа", "БанкКредитор", "КредитнаяПрограмма",
        "Количество", "Сумма", "СпособОтгрузки", "СлужбаДоставки", "КаспиДоставка",
        "ЭкспрессДоставка", "СтатусЗаказа", "ПричинаОтмены", "ИсполнительОператора",
        "ИсполнительПродавецКонсультант", "ДатаРеализации", "НомерРеализации",
        "РеализацияПроведена", "РеализацияУдалена", "ДатаФискальногоЧекаОплатаККМ",
        "ВремяФискальногоЧекаОплатаККМ", "ДокументПродажи"
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    start_date = datetime(2025, 12, 15)
    end_date = datetime(2025, 12, 16, 23, 59, 59)

    current_date = start_date
    while current_date < end_date:
        interval_end = min(current_date + timedelta(days=1) - timedelta(seconds=1), end_date)
        print(f"▶ Загружаем данные за период {current_date} - {interval_end}")

        try:
            data = fetch_data(current_date, interval_end)
        except Exception as e:
            print(f"⚠ Ошибка при запросе {current_date} - {interval_end}: {e}")
            current_date = interval_end + timedelta(seconds=1)
            continue

        if not isinstance(data, list):
            print(f"⚠ API вернул неожиданный тип: {type(data)} → {data}")
            current_date = interval_end + timedelta(seconds=1)
            continue

        total_for_day = 0
        print(f"Получено {len(data)} записей")
        if len(data) > 0:
            print("Пример первой записи:", data[0])

        batch_data = []
        for order in data:
            if not isinstance(order, dict):
                print(f"⚠ Пропущена некорректная запись: {order}")
                continue

            values = (
                order.get("ТипКонтрагент"),
                order.get("ТелефонПокупателя"),
                order.get("ИсточникЗаказа"),
                order.get("НомерЗаказаИсточник"),
                order.get("НомерЗаказаПокупателя"),
                parse_date(order.get("ДатаЗаказаПокупателя"), "date"),
                parse_date(order.get("ВремяЗаказаПокупателя"), "time"),
                parse_date(order.get("ДатаОтгрузкиЗаказаПокупателя"), "timestamp"),
                order.get("НоменклатураКод"),
                order.get("РазмещениеСклада"),
                order.get("ПодразделениеКод"),
                order.get("ВидПлатежа"),
                order.get("БанкКредитор"),
                order.get("КредитнаяПрограмма"),
                order.get("Количество", 0),
                order.get("Сумма", 0.0),
                order.get("СпособОтгрузки"),
                order.get("СлужбаДоставки"),
                order.get("КаспиДоставка", False),
                order.get("ЭкспрессДоставка", False),
                order.get("СтатусЗаказа"),
                order.get("ПричинаОтмены"),
                order.get("ИсполнительОператора"),
                order.get("ИсполнительПродавецКонсультант"),
                parse_date(order.get("ДатаРеализации"), "timestamp"),
                order.get("НомерРеализации"),
                order.get("РеализацияПроведена", False),
                order.get("РеализацияУдалена", False),
                parse_date(order.get("ДатаФискальногоЧекаОплатаККМ"), "date"),
                parse_date(order.get("ВремяФискальногоЧекаОплатаККМ"), "time"),
                order.get("ДокументПродажи")
            )
            batch_data.append(values)

            if len(batch_data) >= BATCH_SIZE:
                try:
                    execute_batch(cursor, insert_query, batch_data, page_size=100)
                    conn.commit()
                    total_for_day += len(batch_data)
                    print(f"✔ Загружено {len(batch_data)} записей в БД (батч)")
                except Exception as e:
                    print(f"❌ Ошибка при вставке в БД: {e}")
                    conn.rollback()
                batch_data = []

        # остаток
        if batch_data:
            try:
                execute_batch(cursor, insert_query, batch_data, page_size=100)
                conn.commit()
                total_for_day += len(batch_data)
                print(f"✔ Загружено {len(batch_data)} записей в БД (остаток)")
            except Exception as e:
                print(f"❌ Ошибка при вставке остатка: {e}")
                conn.rollback()

        print(f"▶ Итог за {current_date.date()}: обработано {total_for_day} записей.")
        current_date = interval_end + timedelta(seconds=1)

    cursor.close()
    conn.close()


with DAG(
    'load_orders_to_postgres',
    default_args={'owner': 'airflow', 'start_date': datetime(2022, 1, 1)},
    schedule_interval=None,
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres
    )