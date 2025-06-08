import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import schedule
import json

print("Starting weather collector script...")
load_dotenv()
print("Environment variables loaded.")

VISUALCROSSING_API_KEY = os.getenv('VISUALCROSSING_API_KEY')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'weather')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'weatheruser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'weatherpass')

print(f"VISUALCROSSING_API_KEY: {VISUALCROSSING_API_KEY}")
print(f"POSTGRES_HOST: {POSTGRES_HOST}")
print(f"POSTGRES_DB: {POSTGRES_DB}")
print(f"POSTGRES_USER: {POSTGRES_USER}")

CITIES = {
    "Kyiv": (50.4501, 30.5234),
    "Dnipro": (48.4647, 35.0462)
}

def ensure_table():
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - Ensuring table exists...")
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
                dt TIMESTAMP,
                temp REAL,
                humidity INTEGER,
                pressure REAL,
                windspeed REAL,
                winddir REAL,
                cloudcover INTEGER,
                solarradiation REAL,
                precip REAL,
                description TEXT,
                raw_json JSONB
            );
        """)
        conn.commit()
        cur.close()
        conn.close()
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - Table ensured successfully.")
    except Exception as e:
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - Error ensuring table: {e}")

def get_historical_weather(city_name, lat, lon, days=14, max_retries=3):
    print(f"Fetching historical weather for {city_name}...")
    end_date = datetime.utcnow().date()
    start_date = end_date - timedelta(days=days-1)
    url = (
        f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        f"{lat},{lon}/{start_date}/{end_date}?unitGroup=metric&key={VISUALCROSSING_API_KEY}&contentType=json"
    )
    print(f"API URL: {url}")
    for attempt in range(max_retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            print(f"API request successful for {city_name}.")
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"API request failed for {city_name}, attempt {attempt+1}: {e}")
            if attempt == max_retries - 1:
                print(f"Max retries reached for {city_name}.")
                return None
            delay = 2 ** attempt
            print(f"Waiting {delay} seconds before retrying...")
            time.sleep(delay)
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
        for day_data in weather_data["days"]:
            try:
                dt_str = day_data["datetime"]
                dt_obj = datetime.strptime(dt_str, "%Y-%m-%d")
                temp = day_data.get("temp")
                humidity = day_data.get("humidity")
                pressure = day_data.get("pressure")
                windspeed = day_data.get("windspeed")
                winddir = day_data.get("winddir")
                cloudcover = day_data.get("cloudcover")
                solarradiation = day_data.get("solarradiation")
                precip = day_data.get("precip")
                description = day_data.get("description")
                cur.execute("""
                    INSERT INTO weather_history (
                        city, dt, temp, humidity, pressure, windspeed, winddir, cloudcover, solarradiation, precip, description, raw_json
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    city,
                    dt_obj,
                    temp,
                    humidity,
                    pressure,
                    windspeed,
                    winddir,
                    cloudcover,
                    solarradiation,
                    precip,
                    description,
                    json.dumps(day_data)
                ))
            except Exception as e:
                print(f"Error inserting data for {city}, date {dt_str}: {e}")
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
    for city, (lat, lon) in CITIES.items():
        weather_data = get_historical_weather(city, lat, lon, days=14)
        if weather_data:
            save_weather_to_db(city, weather_data)
        else:
            print(f"Failed to retrieve weather data for {city}.")

# Планируем запуск 1 раз в день
schedule.every().day.at("00:05").do(collect_and_store)

if __name__ == "__main__":
    print(f"Starting weather collector application...")
    collect_and_store()
    while True:
        schedule.run_pending()
        time.sleep(60)