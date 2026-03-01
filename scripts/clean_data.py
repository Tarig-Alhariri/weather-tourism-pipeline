"""
Очистка данных о погоде - CLEANED Layer
Очистка данных из RAW слоя и создание стандартизированного CSV файла
"""

import json
import pandas as pd
import os
from datetime import datetime
import glob

current_date = datetime.now()
year = current_date.year
month = current_date.month
day = current_date.day

# Словар перевод названий городов на русслктй

CITY_TRANSLATION = {
    "Moscow": "Москва",
    "Samara": "Самара",
    "Penza": "Пенза",
    "Sochi": "Сочи",
    "Novosibirsk": "Новосибирск",
}

# Параметры валидации
MIN_TEMP = -50
MAX_TEMP = 60
MIN_NUMIDITY = 0
MAX_NUIFITY = 100
MIN_PRESSURE = 870 # минимальное атмосферное давление на Земле
MAX_PRESSURE = 1085 # максмальное атмосферное давление
MIN_WIND_SPEED = 0
MAX_WIND_SPEED = 150 # максмальная скорость ветра в урагане


def read_raw_json_files(raw_path):
    """
    Чтение всех JSON файлов из RAW слоя
    Возвращает список словарей с данными
    """

    print("Чтение RAW  Дданных...")

    # Посик всех JSON  файлов (кроме лог-файла)
    json_files = glob.glob(os.path.join(raw_path , "weather_*.json"))

    if not json_files:
        print(f"не найдены JSON файлы в :{raw_path}")
        return []
    print(f"найдено файлов : {len(json_files)}")

    all_records = []
    file_processed = 0

    for file_path in json_files:
        try:
            with open(file_path , 'r' , encoding='utf-8') as f:
                data = json.load(f)

            # Извлечение информации о городе 
            city_name = data.get("_metadata", {}).get("city", "Unknown")

            # Обработка ежедневных записей
            daily_records = data.get("daily_records", [])

            for record in daily_records:
                # Добавляем название города к каждой записи
                record["_city"] = city_name
                all_records.append(record)

            file_processed += 1

            print(f"обработна: {os.path.basename(file_path)} ({len(daily_records)} записей)" )
        
        except Exception as e:
            print(f"Ошибка при чтении {file_path}: {str(e)}")
    
    print(f"Всего записеи прочитано: {len(all_records)}")
    return all_records


def clean_temperature(temp):
    """Очистка и валидация температуры"""

    try:
        # Преобразование в число
        temp_float = float(temp)

        # Проверка диапазона
        if temp_float < MIN_TEMP:
            return MIN_TEMP , "adjusted_min"
        elif temp_float > MAX_TEMP:
            return MAX_TEMP , "adjusted_max"
        else:
            return int (round(temp_float)), "valid"
        
    except (ValueError, TypeError):
        return None, "invalid"
    
def clean_feels_like(temp):
    """Очистка температуры 'ощущает как' """
    # Используем ту же логику, что и для температуры

    return clean_temperature(temp)
    
def clean_humidity(humidity):
    """Очистка и влидация влажности"""
    try:
        hum = int(humidity)
        if hum < MIN_NUMIDITY:
            return MIN_NUMIDITY , "adjusted_min"
        elif hum >MAX_NUIFITY:
            return MAX_NUIFITY , "adjusted_max"
        else:
            return hum , "valid"
    except (ValueError , TypeError):
        return None, "invalid"

def clean_pressure(pressure):
    """Очистка и валидация давления"""
    try:
        pres = int(pressure)
        if pres < MIN_PRESSURE:
            return MIN_PRESSURE, "adjusted_min"
        elif pres > MAX_PRESSURE:
            return MAX_PRESSURE, "adjusted_max"
        else:
            return pres, "valid"
    except (ValueError, TypeError):
        return None, "invalid"

def clean_wind_speed(wind_speed):
    """Очистка и валидация скорости ветра"""
    try:
        speed = float(wind_speed) 
        if speed < MIN_WIND_SPEED:
            return MIN_WIND_SPEED , "adjusted_min"
        elif speed > MAX_WIND_SPEED:
            return MAX_WIND_SPEED , "adjusted_max"
        else:
            # Округление до одного знака после запятой
            return round(speed, 1) , "valid"
    except (ValueError , TypeError):
        return None , "invalid"

def convert_date_to_iso(date_str):
     """Преобразование даты в ISO формфт"""
     try:
        # Преобразование из "YYYY-MM-DD" в "YYYY-MM-DDT00:00"
        if "T" not in date_str:
            return f"{date_str}T00:00:00"
        return date_str
     except Exception:
         return None
     

def clean_data(raw_records):
    """
    Основная функция очистки данных 
    Возвращает DataFrame с очищенными данными и иинформацсю для лога
    """
    print("Начало очистки данных...")
    
    cleaned_data = []
    log_info = {
                "total_records": len(raw_records),
        "cleaned_records": 0,
        "adjusted_records": 0,
        "invalid_records": 0,
        "temperature_adjustments": 0,
        "humidity_adjustments": 0,
        "pressure_adjustments": 0,
        "wind_speed_adjustments": 0,
        "problems_found": []

    }

    for record in raw_records:
        try:
            # Перевод названия города на руссктий
            english_city = record.get("_city", "Unknown")
            city_name = CITY_TRANSLATION.get(english_city , english_city)

            # Очистка температуры
            temp_value, temp_status = clean_temperature(record.get("temperature"))
            if temp_value is None:
                log_info['invalid_records'] += 1
                log_info['problems_found'].append(f"Invalid temperature: {record.get('temperature')}")
                continue
    
            # Очистка "ощущается как"
            feels_like_value, feels_like_status = clean_feels_like(record.get("feels_like"))
            if feels_like_value is None:
                feels_like_value = temp_value  # Используем температуру как fallback
            
            # Очистка влажности
            humidity_value, humidity_status = clean_humidity(record.get("humidity"))
            if humidity_value is None:
                log_info["invalid_records"] += 1
                log_info["problems_found"].append(f"Invalid humidity: {record.get('humidity')}")
                continue

            # Очистка давления
            pressure_value , pressure_status = clean_pressure(record.get("pressure"))
            if pressure_value is None:
                log_info["invalid_records"] += 1
                log_info["problems_found"].append(f"Invalid pressure: {record.get('pressure')}")
                continue

            # Очистка скорости ветра
            wind_speed_value, wind_speed_status = clean_wind_speed(record.get("wind_speed"))
            if wind_speed_value is None:
                log_info["invalid_records"] += 1
                log_info["problems_found"].append(f"Invalid wind speed: {record.get('wind_speed')}")
                continue

            # Преобразование даты
            iso_date = convert_date_to_iso(record.get("date", ""))
            if iso_date is None:
                log_info["invalid_records"] += 1
                log_info["problems_found"].append(f"Invalid date: {record.get('date')}")
                continue

            # Описание погоды 
            weather_desc = record.get("weather_desc", "")
            
            # Подсчет корректировок
            if temp_status != "valid":
                log_info["temperature_adjustments"] += 1
                log_info["adjusted_records"] += 1
            if humidity_status != "valid":
                log_info["humidity_adjustments"] += 1
                log_info["adjusted_records"] += 1
            if pressure_status != "valid":
                log_info["pressure_adjustments"] += 1
                log_info["adjusted_records"] += 1
            if wind_speed_status != "valid":
                log_info["wind_speed_adjustments"] += 1
                log_info["adjusted_records"] += 1

            # Добавление очищенной записи
            cleaned_record = {
                "city_name": city_name,
                "temperature": temp_value,
                "feels_like": feels_like_value,
                "humidity": humidity_value,
                "pressure": pressure_value,
                "wind_speed": wind_speed_value,
                "weather_description": weather_desc,
                "collection_time": iso_date
            }

            cleaned_data.append(cleaned_record)
            log_info["cleaned_records"] += 1

        except Exception as e:
            log_info["invalid_records"] += 1
            log_info["problems_found"].append(f"Processing error: {str(e)}")
            continue

    print(f"Очищено записей: {log_info['cleaned_records']} из {log_info['total_records']}")
    return pd.DataFrame(cleaned_data), log_info

def save_cleaned_data(df , cleaned_path):
    """Сохранение очищенных данных в CSV файл"""
    today = datetime.now().strftime("%Y%m%d")
    csv_filename = f"weather_cleaned_{today}.csv"
    csv_path = os.path.join(cleaned_path , csv_filename)

    # Сохранение в CSV
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
    print(f"CSV файл сохранен: {csv_path}")
    print(f"Размер данных: {len(df)} строк, {len(df.columns)} столбцов")
    
    return csv_path

def save_cleaning_log(log_info , cleaned_path):
    """Сохранение лога очистки в TXT файл"""
    today = datetime.now().strftime("%Y%m%d")
    log_filename = f"cleaning_log_{today}.txt"
    log_path = os.path.join(cleaned_path, log_filename)

    with open(log_path, 'w', encoding='utf-8-sig') as f:
        f.write(f"ЛОГ ОЧИСТКИ ДАННЫХ - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        # f.write("=" * 60 + "\n\n")
        
        f.write("-" * 40 + "\n")
        f.write("СТАТИСТИКА ОЧИСТКИ:\n")
        f.write(f" Всего исходных записей: {log_info['total_records']}\n")
        f.write(f"Успешно очищено записей: {log_info['cleaned_records']}\n")
        f.write(f"Записей с корректировками: {log_info['adjusted_records']}\n")
        f.write(f"Невалидных записей: {log_info['invalid_records']}\n")
        
        f.write("-" * 40 + "\n")
        f.write("\n КОРРЕКТИРОВКИ ПО ПОЛЯМ:\n")
        f.write(f"Корректировки температуры: {log_info['temperature_adjustments']}\n")
        f.write(f"Корректировки влажности: {log_info['humidity_adjustments']}\n")
        f.write(f"Корректировки давления: {log_info['pressure_adjustments']}\n")
        f.write(f"Корректировки скорости ветра: {log_info['wind_speed_adjustments']}\n")
        
        f.write("-" * 40 + "\n")
        f.write("\n ОБНАРУЖЕННЫЕ ПРОБЛЕМЫ:\n")
        if log_info['problems_found']:
            for i, problem in enumerate(log_info['problems_found'][:50], 1):
                f.write(f"{i}. {problem}\n")
            if len(log_info['problems_found']) > 50:
                f.write(f"... и еще {len(log_info['problems_found']) - 50} проблем\n")
        else:
            f.write("Проблем не обнаружено\n")

        f.write("-" * 40 + "\n")
        f.write("\n ПРИМЕНЕННЫЕ ПРАВИЛА ОЧИСТКИ:\n")
        f.write("1. Температура: округление до целых чисел, диапазон -50°C до +60°C\n")
        f.write("2. Названия городов: перевод на русский язык\n")
        f.write("3. Время: преобразование в ISO формат (YYYY-MM-DDT00:00:00)\n")
        f.write("4. Валидация диапазонов:\n")
        f.write("   - Влажность: 0% до 100%\n")
        f.write("   - Давление: 870 до 1085 мм рт.ст.\n")
        f.write("   - Скорость ветра: 0 до 150 м/с\n")
        f.write("5. Невалидные значения корректируются до границ диапазона\n")
    
    print(f" Лог очистки сохранен: {log_path}")
    return log_path


def main():
    """Основная функция"""

    print("CLEANED LAYER: ОЧИСТКА ДАННЫХ О ПОГОДЕ")

    # Определение путей
    project_root = "E:/Semester 2/New Techonlogy/lecture 1/weather_tourism_pipeline"
    raw_path = os.path.join(project_root, "data", "raw", f"openweather_api/{year}/{month:02d}/{day:02d}")
    cleaned_path = os.path.join(project_root, "data", "cleaned")
    
    # Создание папки cleaned если не существует
    os.makedirs(cleaned_path, exist_ok=True)
    
    # Чтение данных из RAW
    raw_records = read_raw_json_files(raw_path)
    if not raw_records:
        print("Нет данных для очистки")
        return
    
    # Очистка данных
    cleaned_df, log_info = clean_data(raw_records)
    
    if cleaned_df.empty:
        print("Нет данных после очистки")
        return
    
    # Сохранение результатов
    csv_path = save_cleaned_data(cleaned_df, cleaned_path)
    log_path = save_cleaning_log(log_info, cleaned_path)


if __name__ == "__main__":
    main()   
    
    

























         

    


    


            
