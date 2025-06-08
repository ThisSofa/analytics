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
    print(f"Current Date and Time (UTC): {datetime.now()} - Ensuring table exists...")
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        
        # Проверяем существование таблицы
        cur.execute("SELECT to_regclass('public.weather_history')")
        table_exists = cur.fetchone()[0] is not None
        
        if table_exists:
            print("Table weather_history exists, checking structure...")
            # Проверяем структуру таблицы
            cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'weather_history'")
            columns = [row[0] for row in cur.fetchall()]
            print(f"Existing columns: {columns}")
            
            # Проверяем, есть ли столбец raw_json
            if 'raw_json' in columns:
                print("Column raw_json exists but is no longer needed. Dropping and recreating table.")
                cur.execute("DROP TABLE weather_history")
                table_exists = False
            else:
                # Проверяем наличие необходимых столбцов
                required_columns = ['tavg', 'tmin', 'tmax', 'prcp', 'snow', 'wdir', 'wspd', 'wpgt', 'pres', 'tsun']
                missing = [col for col in required_columns if col not in columns]
                
                if missing:
                    print(f"Missing columns: {missing}. Dropping and recreating the table.")
                    cur.execute("DROP TABLE weather_history")
                    table_exists = False
        
        if not table_exists:
            print("Creating weather_history table without raw_json column...")
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
                    tsun INTEGER
                );
            """)
            print("Table created successfully.")
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"Current Date and Time (UTC): {datetime.now()} - Table structure ensured successfully.")
    except Exception as e:
        print(f"Current Date and Time (UTC): {datetime.now()} - Error ensuring table: {e}")
        import traceback
        traceback.print_exc()

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
        
        # Устанавливаем autocommit, чтобы избежать блокировки транзакции при ошибках
        conn.autocommit = True
        cur = conn.cursor()
        
        for day_data in weather_data.get("data", []):
            try:
                dt_str = day_data["date"]  # Может быть в формате YYYY-MM-DD или YYYY-MM-DD HH:MM:SS
                
                # Логируем сырые данные даты
                print(f"Raw date from API: {dt_str}")
                
                # Определяем формат даты и преобразуем
                if " " in dt_str:
                    dt_obj = datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S").date()
                else:
                    dt_obj = datetime.strptime(dt_str, "%Y-%m-%d").date()
                
                # Выполняем вставку данных без raw_json
                cur.execute("""
                    INSERT INTO weather_history (
                        city, dt, tavg, tmin, tmax, prcp, snow, wdir, wspd, wpgt, pres, tsun
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    city,
                    dt_obj,
                    day_data.get("tavg"),
                    day_data.get("tmin"),
                    day_data.get("tmax"),
                    day_data.get("prcp"),
                    day_data.get("snow"),
                    day_data.get("wdir"),
                    day_data.get("wspd"),
                    day_data.get("wpgt"),
                    day_data.get("pres"),
                    day_data.get("tsun")
                ))
                
                print(f"Inserted data for {city}, date {dt_obj}")
                
            except Exception as e:
                print(f"Error inserting data for {city}, date {day_data.get('date')}: {e}")
                import traceback
                traceback.print_exc()
                # Продолжаем с следующей записью, без прерывания транзакции
        
        cur.close()
        conn.close()
        print(f"Weather data processing completed for {city}.")
    except Exception as e:
        print(f"Error in database connection for {city}: {e}")
        import traceback
        traceback.print_exc()

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