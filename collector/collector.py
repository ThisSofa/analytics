import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import schedule
import json

load_dotenv()

VISUALCROSSING_API_KEY = os.getenv('VISUALCROSSING_API_KEY')
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
    """Ensures the weather_history table exists in the database."""
    print("Ensuring table exists...")
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
        print("Table ensured successfully.")
    except Exception as e:
        print(f"Error ensuring table: {e}")

def get_historical_weather(lat, lon):
    """Retrieves historical weather data from the Visual Crossing Weather API."""
    print(f"Fetching historical weather data for lat={lat}, lon={lon}")
    try:
        url = (
            f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
            f"{lat},{lon}/last30days?unitGroup=metric&key={VISUALCROSSING_API_KEY}&contentType=json"
        )
        print(f"API URL: {url}")
        response = requests.get(url)
        response.raise_for_status()
        print("API request successful.")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"API request failed: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred during API request: {e}")
        return None

def save_weather_to_db(city, weather_data):
    """Saves weather data to the database."""
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
                temp = day_data["temp"]
                humidity = day_data["humidity"]
                pressure = day_data["pressure"]
                windspeed = day_data["windspeed"]
                winddir = day_data["winddir"]
                cloudcover = day_data["cloudcover"]
                solarradiation = day_data["solarradiation"]
                precip = day_data["precip"]
                description = day_data["description"]

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
                print(f"Error inserting data for date {dt_str}: {e}")
                continue # Continue to the next day even if one fails

        conn.commit()
        cur.close()
        conn.close()
        print("Weather data saved successfully.")

    except Exception as e:
        print(f"Error saving weather data: {e}")

def collect_and_store():
    """Collects and stores weather data for all cities."""
    print("Starting data collection and storage...")
    ensure_table()
    for city, (lat, lon) in CITIES.items():
        try:
            print(f"Fetching historical weather for {city}...")
            weather_data = get_historical_weather(lat, lon)
            if weather_data:
                save_weather_to_db(city, weather_data)
                print(f"Saved historical weather for {city}")
            else:
                print(f"Failed to retrieve weather data for {city}.")
        except Exception as e:
            print(f"Error processing {city}: {e}")
    print("Data collection and storage complete.")

schedule.every().day.at("00:05").do(collect_and_store)

if __name__ == "__main__":
    print("Starting weather collector application...")
    collect_and_store()
    while True:
        schedule.run_pending()
        time.sleep(60)