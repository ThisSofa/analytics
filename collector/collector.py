import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import schedule
import json

load_dotenv()

OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'weather')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'weatheruser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'weatherpass')

CITIES = {
    "Kyiv": (50.4501, 30.5234),
    "Stuttgart": (48.7758, 9.1829),
    "Paris": (48.8566, 2.3522),
    "Amsterdam": (52.3676, 4.9041),
    "Vienna": (48.2082, 16.3738)
}

def ensure_table():
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS weather_history (
            id SERIAL PRIMARY KEY,
            city VARCHAR(64),
            dt TIMESTAMP,
            temp REAL,
            humidity INTEGER,
            pressure INTEGER,
            wind_speed REAL,
            weather_desc TEXT,
            raw_json JSONB
        );
    """)
    conn.commit()
    cur.close()
    conn.close()

def get_historical_weather(lat, lon, dt):
    url = (
        f"https://api.openweathermap.org/data/3.0/onecall/timemachine"
        f"?lat={lat}&lon={lon}&dt={dt}&appid={OPENWEATHER_API_KEY}&units=metric"
    )
    response = requests.get(url)
    response.raise_for_status()
    return response.json()

def save_weather_to_db(city, weather_data):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )
    cur = conn.cursor()
    # В One Call 3.0 данные по часам лежат в "data"
    records = weather_data.get("data") or weather_data.get("hourly") or []
    # Если ответ - одиночный (часто так бывает), обернем в список
    if isinstance(records, dict):
        records = [records]
    elif not records and "current" in weather_data:
        # Иногда только current
        records = [weather_data["current"]]

    for hour_data in records:
        dt_ts = hour_data["dt"]
        dt_obj = datetime.utcfromtimestamp(dt_ts)
        temp = hour_data.get("temp")
        humidity = hour_data.get("humidity")
        pressure = hour_data.get("pressure")
        wind_speed = hour_data.get("wind_speed")
        weather_desc = None
        if "weather" in hour_data and hour_data["weather"]:
            weather_desc = hour_data["weather"][0].get("description")
        # raw_json для дебага и расширения
        cur.execute("""
            INSERT INTO weather_history (city, dt, temp, humidity, pressure, wind_speed, weather_desc, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
        """, (
            city,
            dt_obj,
            temp,
            humidity,
            pressure,
            wind_speed,
            weather_desc,
            json.dumps(hour_data)
        ))
    conn.commit()
    cur.close()
    conn.close()

def collect_and_store():
    ensure_table()
    # OpenWeatherMap отдаёт только 5 дней назад максимум (и только почасово)
    # Берём вчерашний день (12:00 UTC), чтобы API точно успело обновить данные
    target_ts = int((datetime.utcnow() - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0).timestamp())
    for city, (lat, lon) in CITIES.items():
        try:
            print(f"Fetching for {city}...")
            weather_data = get_historical_weather(lat, lon, target_ts)
            save_weather_to_db(city, weather_data)
            print(f"Saved weather for {city}")
        except Exception as e:
            print(f"Error for {city}: {e}")

schedule.every().day.at("00:05").do(collect_and_store)

if __name__ == "__main__":
    print("Starting weather collector. Waiting for schedule...")
    collect_and_store()  # Можно убрать, если не нужно запускать сразу
    while True:
        schedule.run_pending()
        time.sleep(60)