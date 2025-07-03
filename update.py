import os
import time
import pandas as pd
import openmeteo_requests
from tqdm import tqdm
import requests_cache
from datetime import datetime
import logging

# --- Настройка логгера ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# --- Константы ---
START_DATE = "1985-05-30"
END_DATE = datetime.now().strftime("%Y-%m-%d")
CITY_FILE = "city_data_good.csv"
WEATHER_FILE = "weather_slice.csv"
OUTPUT_FILE = "irkutsk_weather_updated.csv"

# --- Кэширование запросов ---
session = requests_cache.CachedSession('weather_cache', expire_after=3600)
openmeteo = openmeteo_requests.Client(session=session)

# --- Загрузка городов Иркутской области ---
cities_df = pd.read_csv(CITY_FILE)
cities_df = cities_df.dropna(subset=["latitude", "longitude"])
irkutsk_cities = cities_df[cities_df["region"] == "Иркутская область"]

# --- Загрузка уже имеющихся погодных данных ---
if os.path.exists(WEATHER_FILE):
    weather_df = pd.read_csv(WEATHER_FILE, parse_dates=["date"])
else:
    weather_df = pd.DataFrame(columns=["city_id", "date"])

# --- Определим недостающие города по city_id ---
existing_city_ids = set(weather_df["city_id"].unique())
irkutsk_missing = irkutsk_cities[~irkutsk_cities["id"].isin(existing_city_ids)]

logger.info(f"Городов Иркутской области с недостающими погодными данными: {len(irkutsk_missing)}")

# --- Сбор недостающих погодных данных ---
new_weather_data = []

for _, row in tqdm(irkutsk_missing.iterrows(), total=len(irkutsk_missing), desc="Скачиваем города"):
    city_id, lat, lon = row["id"], row["latitude"], row["longitude"]

    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": START_DATE,
            "end_date": END_DATE,
            "hourly": [
                "temperature_2m", "relative_humidity_2m", "rain",
                "snowfall", "snow_depth", "is_day", "precipitation",
                "wind_direction_100m", "wind_speed_100m"
            ],
            "timezone": "Asia/Irkutsk",
            "temporal_resolution": "hourly_6"
        }

        responses = openmeteo.weather_api("https://archive-api.open-meteo.com/v1/archive", params=params)
        response = responses[0]

        hourly = response.Hourly()
        dates = pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left"
        ).tz_localize(None)

        df = pd.DataFrame({
            "date": dates,
            "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
            "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
            "rain": hourly.Variables(2).ValuesAsNumpy(),
            "snowfall": hourly.Variables(3).ValuesAsNumpy(),
            "snow_depth": hourly.Variables(4).ValuesAsNumpy(),
            "is_day": hourly.Variables(5).ValuesAsNumpy(),
            "precipitation": hourly.Variables(6).ValuesAsNumpy(),
            "wind_direction_100m": hourly.Variables(7).ValuesAsNumpy(),
            "wind_speed_100m": hourly.Variables(8).ValuesAsNumpy(),
        })

        df.insert(0, "city_id", city_id)  # вставляем city_id в начало
        new_weather_data.append(df)
        logger.info(f"Добавлено строк: {len(df)} для города с id={city_id}")
        time.sleep(5)

    except Exception as e:
        logger.error(f"Ошибка при загрузке данных для города id={city_id}: {e}")
        time.sleep(10)

# --- Объединение и приведение типов ---
if new_weather_data:
    new_df = pd.concat(new_weather_data, ignore_index=True)

    # Объединяем с уже существующими
    full_df = pd.concat([weather_df, new_df], ignore_index=True) if not weather_df.empty else new_df

    # Удаляем дубликаты по city_id + date
    full_df = full_df.drop_duplicates(subset=["city_id", "date"]).sort_values("date")

    # Приведение типов
    if full_df["date"].dtype != "datetime64[ns]":
        full_df["date"] = pd.to_datetime(full_df["date"], errors="coerce", utc=False)
    full_df["is_day"] = full_df["is_day"].astype(bool)

    # Приведение всех float-колонок к float64 (на случай object-типов)
    for col in full_df.columns:
        if col not in ["city_id", "date", "is_day"]:
            full_df[col] = pd.to_numeric(full_df[col], errors="coerce")

    # Убедимся, что city_id в начале
    cols = ["city_id", "date", "temperature_2m", "relative_humidity_2m", "rain",
            "snowfall", "snow_depth", "is_day", "precipitation", "wind_direction_100m", "wind_speed_100m"]
    full_df = full_df[cols]

    # Сохраняем
    full_df.to_csv(OUTPUT_FILE, index=False)
    logger.info(f"✅ Сохранено в файл {OUTPUT_FILE}, всего строк: {len(full_df)}")
    # Контроль типов
    logger.info(f"Тип колонки 'date' после сохранения: {full_df['date'].dtype}")

else:
    logger.warning("⚠️ Новых данных не получено")
