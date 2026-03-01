"""
Обогащение данных о погоде - ENRICHED Layer
Добавление географической и туристической информации
"""

import pandas as pd
import os
from datetime import datetime
import numpy as np

# 1. Константы для расчета comfort_index
TEMP_WEIGHT = 0.4
HUMIDITY_WEIGHT = 0.3
WIND_WEIGHT = -0.3

# 2. Правила для recommended_activity
def get_recommended_activity(weather_main, season, temperature):
    """
    Определение рекомендуемой активности на основе погоды и сезона
    """
    # Зимние активности
    if season == "winter":
        if weather_main in ["Snow", "Clouds"] and temperature > -15:
            return "зимние прогулки"
        elif weather_main == "Clear" and temperature < -20:
            return "катание на коньках"
        elif weather_main == "Snow" and temperature < -25:
            return "домашний отдых"
        else:
            return "посещение музеев"
    
    # Летние активности
    elif season == "summer":
        if weather_main == "Clear" and temperature > 25:
            return "прогулки у воды"
        elif weather_main == "Clear" and 15 <= temperature <= 25:
            return "экскурсии на природе"
        elif weather_main == "Rain":
            return "посещение музеев"
        elif weather_main == "Clouds":
            return "городские прогулки"
        else:
            return "отдых в парке"
    
    # Весна и осень
    else:  # spring or fall
        if weather_main == "Clear" and temperature > 10:
            return "пешие прогулки"
        elif weather_main == "Rain":
            return "посещение театров"
        elif weather_main == "Clouds":
            return "фото-прогулки"
        else:
            return "кафе и рестораны"

def get_season_from_date(date_str):
    """
    Определение сезона по дате
    """
    date = pd.to_datetime(date_str)
    month = date.month
    
    if month in [12, 1, 2]:
        return "winter"
    elif month in [3, 4, 5]:
        return "spring"
    elif month in [6, 7, 8]:
        return "summer"
    else:
        return "fall"

def calculate_comfort_index(row):
    """
    Расчет индекса комфортности
    Формула: (temp * 0.4) + (100 - humidity) * 0.3 + (wind_speed * -0.3)
    """
    temp = row['temperature']
    humidity = row['humidity']
    wind_speed = row['wind_speed']
    
    # Расчет сырого индекса
    raw_index = (temp * TEMP_WEIGHT) + ((100 - humidity) * HUMIDITY_WEIGHT) + (wind_speed * WIND_WEIGHT)
    
    # Нормализация к шкале 0-10
    # Предполагаем что raw_index обычно между -20 и 40
    normalized = ((raw_index + 20) / 60) * 10
    normalized = max(0, min(10, normalized))  # Ограничение 0-10
    
    return round(normalized, 1)

def get_comfort_category(comfort_index):
    """
    Категоризация индекса комфортности
    """
    if comfort_index >= 7:
        return "комфортно"
    elif comfort_index >= 4:
        return "приемлемо"
    else:
        return "дискомфортно"

def check_tourist_season_match(date_str, tourism_season):
    """
    Проверка соответствия даты туристическому сезону
    """
    if tourism_season == "круглогодично":
        return "Да"
    
    date = pd.to_datetime(date_str)
    month = date.month
    
    # Парсинг сезона (например "май-сентябрь")
    try:
        if "-" in tourism_season:
            months_map = {
                'январь': 1, 'февраль': 2, 'март': 3, 'апрель': 4,
                'май': 5, 'июнь': 6, 'июль': 7, 'август': 8,
                'сентябрь': 9, 'октябрь': 10, 'ноябрь': 11, 'декабрь': 12
            }
            
            start_month_str, end_month_str = tourism_season.split('-')
            start_month = months_map.get(start_month_str.lower(), 1)
            end_month = months_map.get(end_month_str.lower(), 12)
            
            if start_month <= end_month:
                # Обычный случай: май-сентябрь
                if start_month <= month <= end_month:
                    return "Да"
            else:
                # Переход через год: ноябрь-март
                if month >= start_month or month <= end_month:
                    return "Да"
    except:
        pass
    
    return "Нет"

def load_city_reference(reference_path):
    """
    Загрузка справочника городов
    """
    try:
        df = pd.read_csv(reference_path, encoding='utf-8-sig')
        print(f"Загружен справочник городов: {len(df)} записей")
        return df
    except Exception as e:
        print(f"Ошибка загрузки справочника: {str(e)}")
        return None

def enrich_data(cleaned_path, reference_df, enriched_path):
    """
    Основная функция обогащения данных
    """
    print("Начало обогащения данных...")
    
    # Поиск самого свежего cleaned файла
    cleaned_files = [f for f in os.listdir(cleaned_path) if f.startswith("weather_cleaned_") and f.endswith(".csv")]
    if not cleaned_files:
        print("Не найдены cleaned файлы")
        return None
    
    latest_file = sorted(cleaned_files)[-1]
    cleaned_file_path = os.path.join(cleaned_path, latest_file)
    
    print(f"Чтение cleaned данных: {latest_file}")
    
    # Загрузка cleaned данных
    df_cleaned = pd.read_csv(cleaned_file_path, encoding='utf-8-sig')
    print(f"Загружено записей: {len(df_cleaned)}")
    
    # Обогащение данных
    print("Добавление новых полей...")
    
    # Словарь для быстрого поиска по справочнику
    city_ref_dict = reference_df.set_index('city_name').to_dict('index')
    
    # Создание новых колонок
    df_cleaned['federal_district'] = None
    df_cleaned['timezone'] = None
    df_cleaned['population'] = None
    df_cleaned['tourism_season'] = None
    df_cleaned['season'] = None
    df_cleaned['comfort_index'] = None
    df_cleaned['comfort_category'] = None
    df_cleaned['recommended_activity'] = None
    df_cleaned['tourist_season_match'] = None
    
    # Статистика для лога
    stats = {
        'total_records': len(df_cleaned),
        'cities_found': set(),
        'seasons': {'winter': 0, 'spring': 0, 'summer': 0, 'fall': 0}
    }
    
    # Обработка каждой записи
    for idx, row in df_cleaned.iterrows():
        city_name = row['city_name']
        stats['cities_found'].add(city_name)
        
        # Получение информации из справочника
        if city_name in city_ref_dict:
            city_info = city_ref_dict[city_name]
            df_cleaned.at[idx, 'federal_district'] = city_info['federal_district']
            df_cleaned.at[idx, 'timezone'] = city_info['timezone']
            df_cleaned.at[idx, 'population'] = city_info['population']
            df_cleaned.at[idx, 'tourism_season'] = city_info['tourism_season']
        
        # Определение сезона
        season = get_season_from_date(row['collection_time'])
        df_cleaned.at[idx, 'season'] = season
        stats['seasons'][season] += 1
        
        # Расчет индекса комфортности
        comfort_index = calculate_comfort_index(row)
        df_cleaned.at[idx, 'comfort_index'] = comfort_index
        df_cleaned.at[idx, 'comfort_category'] = get_comfort_category(comfort_index)
        
        # Рекомендуемая активность
        df_cleaned.at[idx, 'recommended_activity'] = get_recommended_activity(
            row['weather_description'],
            season,
            row['temperature']
        )
        
        # Проверка туристического сезона
        if city_name in city_ref_dict:
            df_cleaned.at[idx, 'tourist_season_match'] = check_tourist_season_match(
                row['collection_time'],
                city_info['tourism_season']
            )
    
    #  Сохранение обогащенных данных
    today = datetime.now().strftime("%Y%m%d")
    enriched_filename = f"weather_enriched_{today}.csv"
    enriched_filepath = os.path.join(enriched_path, enriched_filename)
    
    df_cleaned.to_csv(enriched_filepath, index=False, encoding='utf-8-sig')
    
    #  Вывод статистики
    print("СТАТИСТИКА ОБОГАЩЕНИЯ")
    print(f"Всего записей обработано: {stats['total_records']}")
    print(f"Города: {', '.join(stats['cities_found'])}")
    print(f"Зима: {stats['seasons']['winter']} записей")
    print(f"Весна: {stats['seasons']['spring']} записей")
    print(f"Лето: {stats['seasons']['summer']} записей")
    print(f"Осень: {stats['seasons']['fall']} записей")
    print(f"Сохранено: {enriched_filename}")
    
    return enriched_filepath

def main():
    """Основная функция"""
    
    # 1. Определение путей
    project_root = "E:/Semester 2/New Techonlogy/lecture 1/weather_tourism_pipeline"
    enriched_path = os.path.join(project_root, "data", "enriched")
    cleaned_path = os.path.join(project_root, "data", "cleaned")
    
    # Создание папок
    os.makedirs(enriched_path, exist_ok=True)
    reference_path = os.path.join(enriched_path , 'cities_reference.csv')
    
    # 2. Проверка наличия справочника
    if not os.path.exists(reference_path):
        print("Файл справочника не найден!")
        print(f"Ожидается: {reference_path}")
        print("\nСоздайте файл cities_reference.csv со структурой:")
      
        return
    
    # 3. Загрузка справочника
    reference_df = load_city_reference(reference_path)
    if reference_df is None:
        return
    
    # 4. Обогащение данных
    enriched_file = enrich_data(cleaned_path, reference_df, enriched_path)
    
    if enriched_file:
        print(f"\nОбогащение данных успешно завершено!")
        print(f"Файл сохранен: {enriched_file}")
    else:
        print("\nОбогащение данных не выполнено")

if __name__ == "__main__":
    main()

