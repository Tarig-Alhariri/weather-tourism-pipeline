"""
Генератор имитационных данных о погоде за 10 лет 
Создает данных для 5 российских городов (2011-2020)
Формат: 5 JSON файлов (один файл на город)

"""
import json
import os 
import random
from datetime import datetime , timedelta
import uuid


current_date = datetime.now()
year = current_date.year
month = current_date.month
day = current_date.day

# Города (английские названия как в API)
CITIES = ["Moscow" , "Samara" , "Penza", "Sochi", "Novosibirsk"]

# Географическая информация о городах
CITY_INFO = {
    "Moscow": {"lat": 55.7558 , "lon": 37.6176 , "country": "RU"},
    "Samara": {"lat": 53.1959 , "lon": 50.1002 , "country": "RU"},
    "Penza": {"lat": 53.2007, "lon": 45.0046 , "country": "RU"},
    "Sochi": {"lat": 43.5855 , "lon": 39.7273 , "country": "RU"},
    "Novosibirsk": {"lat" : 55.0084 , "lon": 82.9357 , "country": "RU"}

}

# Сезонные температуры для каждого города
SEASONAL_TEMPS = {
    "Moscow" : {"winter": -10 , "spring": 10 , "summer" : 22 , "fall": 8},
    "Samara": {"winter" : -12 , "spring": 12 , "summer": 23 , "fall": 10},
    "Penza": {"winter": -14 , "spring": 11, "summer": 24, "fall": 9},
    "Sochi": {"winter": 8 , "spring": 15, "summer": 27, "fall": 15},
    "Novosibirsk": {"winter": -18 , "spring": 5, "summer": 20, "fall": 4}
}

# Погодые условия
WEATHER_CONDITIONS = [
    {"id": 800 , "main": "Clear" , "description": "ясно", "icon": "01d"},
    {"id": 801 , "main": "Clouds", "description": "небольшая облачность", "icon":"02d"},
    {"id": 802 , "main": "Clouds", "description": "переменная облачность", "icon":"03d"},
    {"id": 803 , "main": "Clouds", "description": "облачно с прояснениями", "icon":"04d"},
    {"id": 804 , "main": "Clouds", "description": "посмурно", "icon":"02d"},
    {"id": 500 , "main": "Rain", "description": "небольшой дождь", "icon":"10d"},
    {"id": 501 , "main": "Rain", "description": "умеренный дождь", "icon":"10d"},
    {"id": 600 , "main": "Snow", "description": "небольшой снег ", "icon":"13d"},
    {"id": 601 , "main": "Snow", "description": "снег", "icon":"13d"},
    {"id": 701 , "main": "Mist", "description": "туман", "icon":"50d"},
    {"id": 741 , "main": "Fog", "description": "дымка", "icon":"50d"},

]

def get_season(month):
    """Определение сезона по месяцу"""
    if month in [12 , 1 , 2]:
        return "winter"
    elif month in [3 ,4 ,5]:
        return "spring"
    elif month in [6 ,7 ,8]:
        return "summer"
    else:
        return "fall"
    
def generate_daily_weather(city , date):
    """Генерация ежедневных данных погоде"""
    month = date.month
    season = get_season(month)

    # базовая темперратнра
    base_temp = SEASONAL_TEMPS[city][season]

    # Реалистичные вариации
    temp = round(base_temp + random.uniform(-7 , 7), 1)
    feels_like = round(temp + random.uniform(-3 , 2) ,1)

    # Выбор погодных условий по сезону
    if season == "winter":
        weather_options = [cond for cond in WEATHER_CONDITIONS
                          if cond["main"] in ["Snow" , "Clouds", "Clear"]]
    elif season == "summer":
        weather_options = [cond for cond in WEATHER_CONDITIONS
                          if cond ["main"] in ["Clear", "Clouds", "Rain"]]
    else:
        weather_options = WEATHER_CONDITIONS
    
    weather_condition = random.choice(weather_options)

    # Ежедневные данные
    daily_data = {
        "date": date.strftime("%Y-%m-%d"),
        "temperature": temp,
        "feels_like": feels_like,
        "pressure": random.randint(980, 1030),
        "humidity": random.randint(40, 95),
        "wind_speed": round(random.uniform(0.5, 12.0), 1),
        "weather_main": weather_condition["main"],
        "weather_desc": weather_condition["description"],
    }

    return daily_data

def create_city_weather_file(city , start_date , end_date):
    """Создание файла погоды для одного города за 10 лет"""
    print(f"Начало обработки города: {city}")
    print(f"Период: {start_date.date()} по {end_date.date()}")

    # Основные данные файла
    city_data = {
        "_metadata": {
            "collection_time": datetime.now().isoformat(),
            "source": "mock_weather_generator",
            "city": city
        }
        ,
        "city_info": {
            "name": city,
            "coordinates": CITY_INFO[city],
            "country": "RU",
            "timezone": "UTC+3"
        }
        , "data_period": {
            "start_date": start_date.strftime("%Y-%m-%d"),
            "end_date": end_date.strftime("%Y-%m-%d"),
            "total_days": (end_date - start_date).days + 1
        },
        "daily_records": []
    }

    # Генерация данных для каждого дня
    current_date = start_date 
    records_count = 0

    while current_date <= end_date:
        daily_data = generate_daily_weather(city, current_date)
        city_data["daily_records"].append(daily_data)
        records_count += 1
        current_date += timedelta(days=1)

    # Сохранение файла

    # Создаем путь с датой: openweather_api/2026/03/01
    raw_path = f"E:/Semester 2/New Techonlogy/lecture 1/weather_tourism_pipeline/data/raw/openweather_api/{year}/{month:02d}/{day:02d}"
    os.makedirs(raw_path, exist_ok=True)

    filename = f"weather_{city.lower()}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}_{datetime.now().strftime('%H%M')}.json"
    filepath = os.path.join(raw_path, filename)

    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(city_data, f, ensure_ascii=False, indent=2)
    

     
    print(f"Создано {records_count} ежедневных записей")
    print(f"Файл сохранен: {filename}")

    return records_count


def main():

    """Основная функция"""

    start_date = datetime(2011 , 1, 1)
    end_date = datetime(2020 , 12 ,31)

    print(f"Временной период: {start_date.date()} - {end_date.date()}")
    print(f"Количество городов: {len(CITIES)}")
    
    total_records = 0

    # Обработка каждого города
    for city in CITIES:
        records = create_city_weather_file(city, start_date , end_date)
        total_records += records

    # Создание файла журнала
    log_data = {
        "generated_at": datetime.now().isoformat(),
        "period": f"{start_date.date()} по {end_date.date()}",
        "cities": CITIES,
        "total_daily_records": total_records,
        "average_records_per_city": total_records // len(CITIES),
    }

    log_path = f"E:/Semester 2/New Techonlogy/lecture 1/weather_tourism_pipeline/data/raw/openweather_api/{year}/{month:02d}/{day:02d}/generation_log.json"
    with open(log_path , 'w', encoding= 'utf-8') as f:
        json.dump(log_data, f, ensure_ascii=False, indent=2)
      
    print(f"Всего ежедневных записей: {total_records}")
    print(f"Количество обработанных городов: {len(CITIES)}")

    
    return total_records

if __name__ == "__main__":
    main()


      







