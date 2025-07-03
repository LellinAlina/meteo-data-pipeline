import pandas as pd
import requests
from bs4 import BeautifulSoup

def parse_table_from_url(url, table_selector=None):
    """
    Парсит таблицу с веб-страницы и возвращает DataFrame
    :param url: URL страницы с таблицей
    :param table_selector: Селектор для нахождения таблицы (если None - берется первая таблица на странице)
    :return: pandas DataFrame с данными таблицы
    """
    # Получаем HTML-страницу
    response = requests.get(url)
    response.raise_for_status()  # Проверяем, что запрос успешен
    
    # Парсим HTML
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Находим таблицу
    if table_selector:
        table = soup.select_one(table_selector)
    else:
        table = soup.find('table')
    
    if not table:
        raise ValueError("Таблица не найдена на странице")
    
    # Преобразуем HTML-таблицу в DataFrame
    df = pd.read_html(str(table))[0]
    
    # Очищаем заголовки (могут быть многоуровневыми)
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [' '.join(col).strip() for col in df.columns.values]
    else:
        df.columns = df.columns.str.strip()
    
    return df

def filter_table_by_keys(input_df, keys_file, columns_to_keep=None):
    """
    Фильтрует DataFrame по ключам из файла и оставляет только нужные колонки
    input_df: Исходный DataFrame
    keys_file: Путь к CSV-файлу с ключами
    key_column_name: Название колонки для сопоставления с ключами
    columns_to_keep: Список колонок для сохранения (если None - сохраняются все)
    :return: Отфильтрованный DataFrame
    """
    # Загружаем ключи из CSV
    keys_df = pd.read_csv(keys_file)
    keys = keys_df.iloc[75:112]['city'].tolist() 
    
    # Фильтруем строки
    filtered_df = input_df[input_df["Город"].isin(keys)]
    
    # Выбираем только нужные колонки
    if columns_to_keep:
        filtered_df = filtered_df[columns_to_keep]
    
    return filtered_df

def main():
    # Конфигурация
    url = "https://ru.wikipedia.org/wiki/Список_городов_России"  # Замените на реальный URL
    keys_csv = "cities.csv"  # Файл с ключами для фильтрации
    output_csv = "filtered_table.csv"       # Выходной файл
    
    # Настройки для конкретной таблицы (замените на свои)
    table_selector = "#mw-content-text > div.mw-content-ltr.mw-parser-output > table:nth-child(6)"     # CSS-селектор таблицы (может быть None)
    columns_to_keep = ["Город", "Регион", "Федеральный округ", "Население"]     
    try:
        # Парсим таблицу
        print(f"Парсинг таблицы с {url}...")
        table_df = parse_table_from_url(url, table_selector)
        
        # Фильтруем данные
        print(f"Фильтрация данных по ключам из {keys_csv}...")
        filtered_df = filter_table_by_keys(
            table_df, 
            keys_csv,
            columns_to_keep
        )
        
        # Сохраняем результат
        filtered_df.to_csv(output_csv, index=False)
        print(f"Результат сохранен в {output_csv}")
        print(f"Найдено {len(filtered_df)} подходящих строк")
        
    except Exception as e:
        print(f"Произошла ошибка: {e}")

if __name__ == "__main__":
    main()