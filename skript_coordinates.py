import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

df = pd.read_csv('filtered_table.csv')  # замените на ваш файл

geolocator = Nominatim(user_agent="city_coordinates_app")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)  # ограничение запросов

# Функция для получения координат
def get_coordinates(city):
    try:
        location = geocode(city)
        if location:
            return location.latitude, location.longitude
        else:
            return None, None
    except:
        return None, None

# Добавляем колонки с координатами
df['latitude'], df['longitude'] = zip(*df['Город'].apply(get_coordinates))
#Переименовываем остальные колонки
df = df.rename(columns= {"Город":"city", "Регион":"region", "Федеральный округ":"federal_district", "Население": "population"})
df = df.query("city != 'Краснослободск' & city != 'Кировск'")
# Сохраняем результат
df.to_csv('cities_with_coordinates.csv', index=False)
print("Координаты успешно добавлены в таблицу!")