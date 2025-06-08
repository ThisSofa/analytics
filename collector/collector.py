import os
import time
import requests
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import schedule
import json
import random

# Load environment variables
print("Starting weather collector script...")
load_dotenv()
print("Environment variables loaded.")

# API key and database connection details from environment variables
VISUALCROSSING_API_KEY = os.getenv('VISUALCROSSING_API_KEY')
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'db')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'weather')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'weatheruser')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'weatherpass')

print(f"VISUALCROSSING_API_KEY: {VISUALCROSSING_API_KEY}")
print(f"POSTGRES_HOST: {POSTGRES_HOST}")
print(f"POSTGRES_DB: {POSTGRES_DB}")
print(f"POSTGRES_USER: {POSTGRES_USER}")

# Define the cities and their coordinates
CITIES = {
    "Kyiv": (50.4501, 30.5234),
    "Lviv": (49.8397, 24.0297),
    "Odesa": (46.4825, 30.7233),
    "Kharkiv": (49.9935, 36.2304),
    "Dnipro": (48.4647, 35.0462)
}


def ensure_table():
    """Ensures the weather_history table exists in the database."""
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Ensuring table exists...")
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Successfully connected to the database.")
        # Create a cursor object to execute SQL queries
        cur = conn.cursor()
        # Execute the CREATE TABLE statement to create the weather_history table if it doesn't exist
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
        # Commit the changes to the database
        conn.commit()
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Table ensured.")
        # Close the cursor and connection
        cur.close()
        conn.close()
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Connection closed.")
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Table ensured successfully.")
    except Exception as e:
        # Handle any exceptions that occur during the process
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Error ensuring table: {e}")


def get_historical_weather(lat, lon, max_retries=3):
    """Retrieves historical weather data from the Visual Crossing Weather API with retries."""
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Fetching historical weather data for lat={lat}, lon={lon}")
    for attempt in range(max_retries):
        try:
            # Construct the API URL with latitude, longitude, and API key
            url = (
                f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
                f"{lat},{lon}/last30days?unitGroup=metric&key={VISUALCROSSING_API_KEY}&contentType=json"
            )
            print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Attempt {attempt + 1}: API URL: {url}")
            # Send a GET request to the API and use 'with' statement to ensure connection is closed
            with requests.get(url) as response:
                # Raise an exception for non-200 status codes
                response.raise_for_status()
                print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - API request successful.")
                # Return the JSON response
                return response.json()
        except requests.exceptions.RequestException as e:
            # Handle API request errors
            print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Attempt {attempt + 1}: API request failed: {e}")
            if attempt == max_retries - 1:
                print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Max retries reached.")
                return None
            # Implement exponential backoff
            delay = 2 ** attempt
            print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Waiting {delay} seconds before retrying...")
            time.sleep(delay)
        except Exception as e:
            # Handle any unexpected errors during the API request
            print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - An unexpected error occurred during API request: {e}")
            return None
    return None


def save_weather_to_db(city, weather_data):
    """Saves weather data to the database."""
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Saving weather data for city: {city}")
    try:
        # Establish a connection to the PostgreSQL database
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Successfully connected to the database.")
        # Create a cursor object to execute SQL queries
        cur = conn.cursor()

        # Iterate over each day's data in the weather data
        for day_data in weather_data["days"]:
            try:
                # Extract relevant weather information from the day's data
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

                # Execute an INSERT statement to save the weather data into the weather_history table
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
                # Handle any exceptions that occur during data insertion
                print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Error inserting data for date {dt_str}: {e}")
                continue  # Continue to the next day even if one fails

        # Commit the changes to the database
        conn.commit()
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Data committed.")
        # Close the cursor and connection
        cur.close()
        conn.close()
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Connection closed.")
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Weather data saved successfully.")

    except Exception as e:
        # Handle any exceptions that occur during the database connection or data saving process
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Error saving weather data: {e}")


def collect_and_store():
    """Collects and stores weather data for one random city."""
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Starting data collection and storage...")
    # Ensure that the weather_history table exists in the database
    ensure_table()

    # Choose a random city from the list of cities
    city = random.choice(list(CITIES.keys()))
    lat, lon = CITIES[city]

    try:
        # Fetch historical weather data for the chosen city
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Fetching historical weather for {city}...")
        weather_data = get_historical_weather(lat, lon)
        if weather_data:
            # Save the weather data to the database
            save_weather_to_db(city, weather_data)
            print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Saved historical weather for {city}")
        else:
            # Log a message if weather data retrieval fails
            print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Failed to retrieve weather data for {city}.")
    except Exception as e:
        # Handle any exceptions that occur during the data processing
        print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Error processing {city}: {e}")
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Data collection and storage complete.")


# Schedule the data collection and storage to run every day at 00:05
schedule.every().day.at("00:05").do(collect_and_store)

if __name__ == "__main__":
    print(f"Current Date and Time (UTC): {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} - User: CyberSofa - Starting weather collector application...")
    # Run the data collection and storage function once initially
    collect_and_store()
    # Start the scheduler to run the job every day at 00:05
    while True:
        schedule.run_pending()
        time.sleep(60)