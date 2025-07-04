import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import openmeteo_requests
from supabase import create_client
from dotenv import load_dotenv

# ───── Настройки ─────
USE_CACHE = True  # Включить кэш при отладке локально

# ───── Загрузка переменных окружения ─────
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("❌ SUPABASE_URL и/или SUPABASE_KEY не заданы в .env")

# ───── Supabase клиент ─────
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ───── Кэш запросов (опционально) ─────
if USE_CACHE:
    import requests_cache
    requests_cache.install_cache("weather_cache", expire_after=3600)

# ───── Open-Meteo клиент ─────
openmeteo = openmeteo_requests.Client()

# ───── Диапазон дат: 7 дней назад и 7 вперёд ─────
now = datetime.now(timezone.utc)
start_date = (now - timedelta(days=7)).strftime("%Y-%m-%d")
end_date = (now + timedelta(days=7)).strftime("%Y-%m-%d")

# ───── Загрузка городов из Supabase ─────
response = supabase.table("city").select("*").execute()
cities = response.data

if not cities:
    raise RuntimeError("❌ В таблице city нет городов.")

print(f"🔍 Найдено городов: {len(cities)}")

all_data = []

for city in cities:
    city_id = city["id"]
    lat = city["latitude"]
    lon = city["longitude"]
    name = city["name"]

    print(f"🌍 Обработка: {name} (ID {city_id})")

    try:
        params = {
            "latitude": lat,
            "longitude": lon,
            "start_date": start_date,
            "end_date": end_date,
            "hourly": [
                "temperature_2m", "relative_humidity_2m", "rain",
                "snowfall", "snow_depth", "is_day", "precipitation",
                "wind_direction_100m", "wind_speed_100m"
            ],
            "timezone": "Asia/Irkutsk",
            "temporal_resolution": "hourly_6"
        }

        responses = openmeteo.weather_api("https://archive-api.open-meteo.com/v1/archive", params=params)

        if not responses or responses[0] is None:
            print(f"⚠️ Нет ответа от Open-Meteo для {name}")
            continue

        response = responses[0]
        hourly = response.Hourly()
        if hourly is None or hourly.Time() is None or hourly.TimeEnd() is None:
            print(f"⚠️ Пустой блок hourly для {name}")
            continue

        # Временные метки
        start_time = pd.to_datetime(hourly.Time(), unit="s", utc=True)
        end_time = pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True)
        interval = pd.Timedelta(seconds=hourly.Interval())
        dates = pd.date_range(start=start_time, end=end_time, freq=interval, inclusive="left")

        # Безопасное извлечение переменной
        def safe_values(hourly_obj, index):
            try:
                variable = hourly_obj.Variables(index)
                if variable is None:
                    return [None] * len(dates)
                return variable.ValuesAsNumpy()
            except Exception:
                return [None] * len(dates)

        df = pd.DataFrame({
            "city_id": city_id,
            "date": dates,
            "temperature_2m": safe_values(hourly, 0),
            "relative_humidity_2m": safe_values(hourly, 1),
            "rain": safe_values(hourly, 2),
            "snowfall": safe_values(hourly, 3),
            "snow_depth": safe_values(hourly, 4),
            "is_day": [bool(v) if v is not None else None for v in safe_values(hourly, 5)],
            "precipitation": safe_values(hourly, 6),
            "wind_direction_100m": safe_values(hourly, 7),
            "wind_speed_100m": safe_values(hourly, 8),
        })

        all_data.append(df)
        time.sleep(1)

    except Exception as e:
        print(f"❌ Ошибка при обработке {name}: {e}")
        time.sleep(5)
        continue

# ───── Загрузка данных в Supabase ─────
if all_data:
    result_df = pd.concat(all_data, ignore_index=True)
    print(f"📦 Готово к загрузке: {len(result_df)} строк")

    batch_size = 500
    for i in range(0, len(result_df), batch_size):
        batch = result_df.iloc[i:i + batch_size].to_dict(orient="records")
        supabase.table("weather_buffer").upsert(batch).execute()
        print(f"✅ Загружено: {i + len(batch)} / {len(result_df)}")

else:
    print("⚠️ Нет данных для загрузки.")
