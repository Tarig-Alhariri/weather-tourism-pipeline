from fastapi import FastAPI , HTTPException
import pandas as pd 
import os 
from datetime import datetime

# create API App
app = FastAPI(
    title = "Weather Tourism API",
    description = "API для доступа к данным о погоде и туристическим рекомендациям",
    version = "1.0.0"
)

# paths 
PROJECT_ROOT = "E:/Semester 2/New Techonlogy/lecture 1/weather_tourism_pipeline"
AGGREGATED_PATH = os.path.join(PROJECT_ROOT , "data" , "aggregated")
ENRICHED_PATH = os.path.join(PROJECT_ROOT , "data" , "enriched")

@app.get("/")
def root():
    """ Главная страница"""
    return {
        "message": "Weather Tourism API",
        "status": "running",
        "endpoints": [
            "/cities-rating",
            "/districts-summary", 
            "/today-recommendations",
            "/city/{city_name}",
            "/docs"
        ]
    }

@app.get("/cities-rating")
def get_cities_rating():
    """Возвращает рейтинг городов по комфортности"""
    try:
        file_path = os.path.join(AGGREGATED_PATH, "city_tourism_rating.csv")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Файл не найден: {str(e)}")

@app.get("/districts-summary")
def get_districts_summary():
    """Сводка по федеральным округам"""
    try:
        file_path = os.path.join(AGGREGATED_PATH, "federal_districts_summary.csv")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Файл не найден: {str(e)}")

@app.get("/today-recommendations")
def get_today_recommendations():
    """Рекомендации на сегодня"""
    try:
        file_path = os.path.join(AGGREGATED_PATH, "travel_recommendations.csv")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Файл не найден: {str(e)}")

@app.get("/city/{city_name}")
def get_city_info(city_name: str):
    """Информация о конкретном городе"""
    try:
        # Поиск в рейтинге городов
        file_path = os.path.join(AGGREGATED_PATH, "city_tourism_rating.csv")
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        
        city_data = df[df['city_name'] == city_name]
        
        if city_data.empty:
            raise HTTPException(status_code=404, detail=f"Город {city_name} не найден")
        
        return city_data.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
