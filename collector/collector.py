import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import schedule
import json

# Load environment variables from .env file
load_dotenv()

# Retrieve environment variables, with default values if not found
OPENWEATHER_API_KEY = os.getenv('OPENWEATHER_API_KEY')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'weather')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'weatheruser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'weatherpass')

# Define cities and their coordinates
CITIES = {
    "Kyiv": (50.4501, 30.5234),
    "Stuttgart": (48.7758, 9.1829),
    "Paris": (48.8566, 2.3522),
    "Amsterdam": (52.3676, 4.9041),
    "Vienna": (48.2082, 16.3738)
}

def ensure_table():
    """Ensures that the weather_history table exists in the PostgreSQL database."""
    print("Ensuring table exists...")
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print("Successfully connected to the database.")
        # Create a cursor object to execute SQL queries
        cur = conn.cursor()
        # Execute a SQL query to create the weather_history table if it doesn't exist
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
        print("Table ensured.")
        # Commit the changes to the database
        conn.commit()
        # Close the cursor and connection
        cur.close()
        conn.close()
        print("Connection closed.")
    except Exception as e:
        # Print any exceptions that occur during the process
        print(f"Error ensuring table: {e}")

def get_historical_weather(lat, lon, dt):
    """Retrieves historical weather data from the OpenWeatherMap API."""
    print(f"Fetching historical weather data for lat={lat}, lon={lon}, dt={dt}")
    try:
        # Construct the API URL
        url = (
            f"https://api.openweathermap.org/data/3.0/onecall/timemachine"
            f"?lat={lat}&lon={lon}&dt={dt}&appid={OPENWEATHER_API_KEY}&units=metric"
        )
        print(f"API URL: {url}")
        # Send a GET request to the API
        response = requests.get(url)
        # Raise an exception for non-200 status codes
        response.raise_for_status()
        print("API request successful.")
        # Return the JSON response
        return response.json()
    except requests.exceptions.RequestException as e:
        # Print any exceptions that occur during the API request
        print(f"API request failed: {e}")
        return None

def save_weather_to_db(city, weather_data):
    """Saves weather data to the weather_history table in the PostgreSQL database."""
    print(f"Saving weather data for city: {city}")
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print("Successfully connected to the database.")
        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Extract weather data records from the API response
        records = weather_data.get("data") or weather_data.get("hourly") or []
        # If the response contains a single record, wrap it in a list
        if isinstance(records, dict):
            records = [records]
        elif not records and "current" in weather_data:
            # If no hourly data is available, use current data if present
            records = [weather_data["current"]]

        # Iterate over the weather data records
        for hour_data in records:
            try:
                # Extract data points from the record
                dt_ts = hour_data["dt"]
                dt_obj = datetime.utcfromtimestamp(dt_ts)
                temp = hour_data.get("temp")
                humidity = hour_data.get("humidity")
                pressure = hour_data.get("pressure")
                wind_speed = hour_data.get("wind_speed")
                weather_desc = None
                if "weather" in hour_data and hour_data["weather"]:
                    weather_desc = hour_data["weather"][0].get("description")

                # Execute a SQL query to insert the weather data into the weather_history table
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
                print(f"Data inserted for datetime: {dt_obj}")
            except Exception as e:
                # Print any exceptions that occur during the data insertion process
                print(f"Error inserting data: {e}")

        # Commit the changes to the database
        conn.commit()
        print("Changes committed.")
        # Close the cursor and connection
        cur.close()
        conn.close()
        print("Connection closed.")
    except Exception as e:
        # Print any exceptions that occur during the process
        print(f"Error saving weather data: {e}")

def collect_and_store():
    """Collects historical weather data for each city and saves it to the database."""
    print("Starting collect_and_store...")
    # Ensure that the weather_history table exists
    ensure_table()
    # Calculate the timestamp for yesterday at 12:00 UTC
    target_ts = int((datetime.utcnow() - timedelta(days=1)).replace(hour=12, minute=0, second=0, microsecond=0).timestamp())
    # Iterate over the cities
    for city, (lat, lon) in CITIES.items():
        try:
            print(f"Fetching weather data for {city}...")
            # Retrieve historical weather data from the OpenWeatherMap API
            weather_data = get_historical_weather(lat, lon, target_ts)
            if weather_data:
                # Save the weather data to the database
                save_weather_to_db(city, weather_data)
                print(f"Saved weather data for {city}")
            else:
                print(f"No weather data received for {city}")
        except Exception as e:
            # Print any exceptions that occur during the process
            print(f"Error collecting and storing data for {city}: {e}")

# Schedule the collect_and_store function to run every day at 00:05
schedule.every().day.at("00:05").do(collect_and_store)

if __name__ == "__main__":
    """Main entry point of the application."""
    print("Starting weather collector...")
    # Collect and store data immediately upon startup (optional)
    collect_and_store()
    # Start the scheduler to run the collect_and_store function at the specified time
    # while True:
    #     schedule.run_pending()
    #     time.sleep(60)