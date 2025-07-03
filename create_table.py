import os
import sys
import pandas as pd
from dotenv import load_dotenv
import psycopg2
from psycopg2 import connect

# Load environment variables
load_dotenv()

# Fetch variables
USER = os.getenv("user")
PASSWORD = os.getenv("password")
HOST = os.getenv("host")
PORT = os.getenv("port")
DBNAME = os.getenv("dbname")

# Connect to the database
try:
    connection = psycopg2.connect(
        user=USER,
        password=PASSWORD,
        host=HOST,
        port=PORT,
        dbname=DBNAME
    )
    print("Connection successful!")
    
    # Create a cursor to execute SQL queries
    cursor = connection.cursor()
    
    # Example query
    cursor.execute("SELECT NOW();")
    result = cursor.fetchone()
    print("Current Time:", result)

    # SQL-запросы на создание таблиц
    city_table_sql = """
    CREATE TABLE IF NOT EXISTS city (
        id INTEGER PRIMARY KEY,
        name TEXT,
        region TEXT,
        federal_district TEXT,
        population BIGINT,
        foundation_year INTEGER,
        status TEXT,
        old_name TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
    """

    city_buffer_sql = """
    CREATE TABLE IF NOT EXISTS city_buffer (
        id INTEGER PRIMARY KEY,
        name TEXT,
        region TEXT,
        federal_district TEXT,
        population BIGINT,
        foundation_year INTEGER,
        status TEXT,
        old_name TEXT,
        latitude DOUBLE PRECISION,
        longitude DOUBLE PRECISION
    );
    """

    weather_table_sql = """
    CREATE TABLE IF NOT EXISTS weather (
        city_id INTEGER REFERENCES city(id),
        date TIMESTAMP,
        temperature_2m DOUBLE PRECISION,
        relative_humidity_2m DOUBLE PRECISION,
        rain DOUBLE PRECISION,
        snowfall DOUBLE PRECISION,
        snow_depth DOUBLE PRECISION,
        is_day BOOLEAN,
        precipitation DOUBLE PRECISION,
        wind_direction_100m DOUBLE PRECISION,
        wind_speed_100m DOUBLE PRECISION,
        PRIMARY KEY (city_id, date)
    );
    """

    weather_buffer_sql = """
    CREATE TABLE IF NOT EXISTS weather_buffer (
        city_id INTEGER,
        date TIMESTAMP,
        temperature_2m DOUBLE PRECISION,
        relative_humidity_2m DOUBLE PRECISION,
        rain DOUBLE PRECISION,
        snowfall DOUBLE PRECISION,
        snow_depth DOUBLE PRECISION,
        is_day BOOLEAN,
        precipitation DOUBLE PRECISION,
        wind_direction_100m DOUBLE PRECISION,
        wind_speed_100m DOUBLE PRECISION,
        PRIMARY KEY (city_id, date)
    );
    """

    # Список всех таблиц
    tables = {
        "city": city_table_sql,
        "city_buffer": city_buffer_sql,
        "weather": weather_table_sql,
        "weather_buffer": weather_buffer_sql
    }

    # Создание таблиц
    for name, query in tables.items():
        try:
            cursor.execute(query)
            connection.commit()
            print(f"✅ Таблица {name} создана (или уже существует).")
        except Exception as e:
            print(f"❌ Ошибка при создании таблицы {name}: {e}")

    # Пример загрузки данных из weather_slice.feather (не вставляет в БД, только читает)
    try:
        df = pd.read_feather("weather_slice.feather")
        print(f"📦 Файл weather_slice.feather загружен: {df.shape[0]} строк, {df.shape[1]} колонок.")
        print(f"Типы данных:\n{df.dtypes}")
    except Exception as e:
        print(f"⚠️ Ошибка при чтении weather_slice.feather: {e}")

    # Close the cursor and connection
    cursor.close()
    connection.close()
    print("Connection closed.")

except Exception as e:
    print(f"Failed to connect: {e}")