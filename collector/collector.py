import os
import time
import requests
import psycopg2
from datetime import date, timedelta, datetime
from dotenv import load_dotenv
import schedule
import json

print("Starting Meteostat weather collector script...")
load_dotenv()
print("Environment variables loaded.")

METEOSTAT_RAPIDAPI_KEY = os.getenv('METEOSTAT_RAPIDAPI_KEY')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'weather')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'weatheruser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'weatherpass')

print(f"METEOSTAT_RAPIDAPI_KEY: {METEOSTAT_RAPIDAPI_KEY}")
print(f"POSTGRES_HOST: {POSTGRES_HOST}")
print(f"POSTGRES_DB: {POSTGRES_DB}")
print(f"POSTGRES_USER: {POSTGRES_USER}")

STATIONS = {
    "Vienna": "11035",   # Wien/Hohe Warte
    "Berlin": "10382",   # Berlin-Tegel
    "Paris": "07156"     # Paris-Montsouris
}

def ensure_table():
    print(f"Current Date and Time (UTC): {date.today()} - Ensuring table exists...")
    try:
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
                dt DATE,
                tavg REAL,
                tmin REAL,
                tmax REAL,
                prcp REAL,
                snow INTEGER,
                wdir INTEGER,
                wspd REAL,
                wpgt REAL,
                pres REAL,
                tsun INTEGER,
                raw_json JSONB
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Current Date and Time (UTC): {date.today()} - Table ensured successfully.")
    except Exception as e:
        print(f"Current Date and Time (UTC): {date.today()} - Error ensuring table: {e}")

def get_historical_weather(city, station, days=30):
    print(f"Fetching historical weather for {city}...")
    end_date = date.today()
    start_date = end_date - timedelta(days=days-1)
    url = f"https://meteostat.p.rapidapi.com/stations/daily?station={station}&start={start_date}&end={end_date}&units=metric"
    headers = {
        "x-rapidapi-host": "meteostat.p.rapidapi.com",
        "x-rapidapi-key": METEOSTAT_RAPIDAPI_KEY
    }
    print(f"API URL: {url}")
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print(f"API request successful for {city}.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed for {city}: {e}")
        return None

def save_weather_to_db(city, weather_data):
    print(f"Saving weather data for city: {city}")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        for day_data in weather_data.get("data", []):
            try:
                dt_str = day_data["date"]  # формат YYYY-MM-DD
                dt_obj = datetime.strptime(dt_str, "%Y-%m-%d").date()
                tavg = day_data.get("tavg")
                tmin = day_data.get("tmin")
                tmax = day_data.get("tmax")
                prcp = day_data.get("prcp")
                snow = day_data.get("snow")
                wdir = day_data.get("wdir")
                wspd = day_data.get("wspd")
                wpgt = day_data.get("wpgt")
                pres = day_data.get("pres")
                tsun = day_data.get("tsun")
                cur.execute("""
                    INSERT INTO weather_history (
                        city, dt, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    city,
                    dt_obj,
                    tavg,
                    tmin,
                    tmax,
                    prcp,
                    snow,
                    wdir,
                    wspd,
                    wpgt,
                    pres,
                    tsun,
                    json.dumps(day_data)
                ))
            except Exception as e:
                print(f"Error inserting data for {city}, date {day_data.get('date')}: {e}")
                continue
        conn.commit()
        cur.close()
        conn.close()
        print(f"Weather data saved for {city}.")
    except Exception as e:
        print(f"Error saving weather data for {city}: {e}")

def collect_and_store():
    print(f"Starting data collection and storage...")
    ensure_table()
    for city, station in STATIONS.items():
        weather_data = get_historical_weather(city, station, days=30)
        if weather_data and weather_data.get("data"):
            save_weather_to_db(city, weather_data)
        else:
            print(f"Failed to retrieve weather data for {city}.")

# Планируем запуск 1 раз в день
schedule.every().day.at("00:05").do(collect_and_store)

if __name__ == "__main__":
    print(f"Starting Meteostat weather collector application...")
    collect_and_store()
    while True:
        schedule.run_pending()
        time.sleep(60)