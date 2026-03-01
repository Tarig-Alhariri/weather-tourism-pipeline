"""
Агрегация данных о погоде - AGGREGATED Layer
Создание трех витрин данных для туристической компанииэ
"""

import pandas as pd
import os 
from datetime import datetime
import numpy as np


# Константы для расчетов 
COMFORTABLE_THRESHOLD = 7 # confort_index > 7 = Комфортно

# Правил для специальных советов 
def get_special_advice(row):
    """
    Определение специальных советов на основе погоды
    """
    advice = []

    # Дождь

    if 'дождь' in str(row['weather_description']).lower():
        advice.append("возьмите зонт")

    # Снег
    if 'Снег' in str(row['weather_description']).lower():
        advice.append("теплая одежда")

    # Сильный ветер
    if row['wind_speed'] > 10:
        advice.append('ветровка')

    # Сильный мороз
    if row['temperature'] < -15:
        advice.append("очень тепло одевайтесь")

    # Сильный жара 
    if row['temperature'] > 30:
        advice.append("вода и головной уроб")
    
    # Тман 
    if 'туман' in str(row['weather_description']).lower():
        advice.append("будьте осторожны на дорогах")
    
    if not advice:
        return "нет особых рекомендаций"
    
    return ", ".join(advice)

def get_tour_type_by_season(season):
    """
    Определение типа тура по сезону
    """
    tour_type = {
        'winter': 'горнолыжный отдых',
        'spring': 'экскурсионные туры',
        'summer': 'пляжный отдых',
        'fall': 'культурно-познавательные туры'
    
    }

    return tour_type.get(season , 'экскурсионный тур')

def create_city_tourism_rating(df_enriched , reference_df, output_path):

    """
    Витрина: Рейтинг городов для туризма
    """
    print(" Создание рейтинга городов...") 

    # Группировка по городам 
    city_stats = df_enriched.groupby('city_name').agg({
        'comfort_index': 'mean',
        'temperature': 'mean',
        'season': lambda x: x.mode()[0] if not x.mode().empty else 'unknown'

    }).reset_index()

    # Округление
    city_stats['avg_comfort_index'] = city_stats['comfort_index'].round(1)
    city_stats['avg_temperature'] = city_stats['temperature'].round(1)


    # Добавление информации из справочника
    city_stats = city_stats.merge(
        reference_df[['city_name', 'tourism_season']], 
        on='city_name', 
        how='left'
    )
    
    # Определение типа тура
    city_stats['recommended_tour_type'] = city_stats['season'].apply(get_tour_type_by_season)

        # Сортировка по комфортности (от лучшего к худшему)
    city_stats = city_stats.sort_values('avg_comfort_index', ascending=False)
    
    # Добавление ранга
    city_stats['ranking'] = range(1, len(city_stats) + 1)
    
    # Выбор нужных колонок
    result = city_stats[[
        'ranking', 'city_name', 'avg_comfort_index', 'avg_temperature',
        'tourism_season', 'recommended_tour_type'
    ]]

    # Сохранение
    output_file = os.path.join(output_path, 'city_tourism_rating.csv')
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"   Сохранено: {output_file}")
    print(f"   Обработано городов: {len(result)}")
    
    return result



def create_federal_districts_summary(df_enriched , reference_df , output_path):
    """
    Витрина: Сводка по федеральным округам
    """
    print(" Создание сводки по федеральным округам...")
    
    # التحقق من وجود العمود
    print(f" Проверка колонок: {df_enriched.columns.tolist()}")
    
    if 'federal_district' not in df_enriched.columns:
        print(" ОШИБКА: Колонка 'federal_district' не найдена в данных!")
        print(" Доступные колонки:", df_enriched.columns.tolist())
        return None
    
    # Группировка по округам
    district_stats = df_enriched.groupby('federal_district').agg({
        'temperature': 'mean',
        'city_name': lambda x: len(x.unique()),  # количество уникальных городов
        'comfort_index': lambda x: (x > COMFORTABLE_THRESHOLD).sum()  # количество комфортных дней
    }).reset_index()
    
    # Переименование колонок
    district_stats.columns = ['federal_district', 'avg_temperature', 'cities_count', 'comfortable_days_count']
    
    # Округление
    district_stats['avg_temperature'] = district_stats['avg_temperature'].round(1)
    
    # Определение лучшего города в каждом округе
    best_cities = []
    for district in district_stats['federal_district'].unique():
        district_data = df_enriched[df_enriched['federal_district'] == district]
        city_avg_comfort = district_data.groupby('city_name')['comfort_index'].mean()
        if not city_avg_comfort.empty:
            best_city = city_avg_comfort.idxmax()
            best_cities.append(best_city)
        else:
            best_cities.append("нет данных")
    
    district_stats['best_city'] = best_cities
    
    # Сохранение
    output_file = os.path.join(output_path, 'federal_districts_summary.csv')
    district_stats.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"  Сохранено: {output_file}")
    print(f"  Обработано округов: {len(district_stats)}")
    print(district_stats)
    
    return district_stats  



def create_travel_recommendations(df_enriched , output_path):
    """
    Витрина : Отчет для турагентств (рекомендации на сегодня)
    """
    print(" Создание отчета для турагентств...")
    
    # Берем последнюю дату в данных
    latest_date = df_enriched['collection_time'].max()
    today_data = df_enriched[df_enriched['collection_time'] == latest_date].copy()
    
    if today_data.empty:
        print(" Нет данных за последнюю дату")
        return None
    
    # التحقق من وجود العمود
    print(f" Колонки для отчета: {today_data.columns.tolist()}")
    
    # تعريف الدوال المساعدة
    def get_weather_status(row):
        if row['comfort_category'] == 'комфортно':
            return 'отличная'
        elif row['comfort_category'] == 'приемлемо':
            return 'приемлемая'
        else:
            return 'неблагоприятная'
    
    def get_recommendation(row):
        if row['comfort_category'] in ['комфортно', 'приемлемо']:
            return 'рекомендуется к посещению'
        else:
            return 'лучше остаться дома'
    
    today_data['weather_status'] = today_data.apply(get_weather_status, axis=1)
    today_data['recommendation'] = today_data.apply(get_recommendation, axis=1)
    today_data['special_advice'] = today_data.apply(get_special_advice, axis=1)
    
    # اختيار الأعمدة المطلوبة
    columns_to_keep = ['city_name', 'federal_district', 'collection_time', 
                      'temperature', 'weather_description', 'comfort_category',
                      'weather_status', 'recommendation', 'special_advice']
    
    # التأكد من وجود جميع الأعمدة
    available_columns = [col for col in columns_to_keep if col in today_data.columns]
    
    result = today_data[available_columns].copy()
    
    # Переименование для понятности
    rename_map = {
        'city_name': 'город',
        'federal_district': 'федеральный_округ',
        'collection_time': 'дата',
        'temperature': 'температура',
        'weather_description': 'погода',
        'comfort_category': 'комфортность',
        'weather_status': 'статус_погоды',
        'recommendation': 'рекомендация',
        'special_advice': 'особые_советы'
    }
    
    # إعادة تسمية الأعمدة الموجودة فقط
    result.rename(columns={k: v for k, v in rename_map.items() if k in result.columns}, inplace=True)
    
    # Сортировка
    result = result.sort_values('рекомендация', ascending=True)
    
    # Сохранение
    output_file = os.path.join(output_path, 'travel_recommendations.csv')
    result.to_csv(output_file, index=False, encoding='utf-8-sig')
    
    print(f"   Сохранено: {output_file}")
    print(f"   Данные за дату: {latest_date}")
    print(f"   Первые 5 записей:")
    print(result.head())
    
    # Дополнительный текстовый отчет
    create_text_report(result, output_path, latest_date)
    
    return result



def create_text_report(recommendations_df, output_path, report_date):
    """
    Создание текстового отчета для легкого чтения
    """
    report_file = os.path.join(output_path, f'travel_report_{datetime.now().strftime("%Y%m%d")}.txt')
    
    with open(report_file, 'w', encoding='utf-8-sig') as f:
        f.write(f"ОТЧЕТ ДЛЯ ТУРАГЕНТСТВ\n")
        f.write(f"Дата: {report_date}\n")
        
        # Топ-3 города для посещения
        f.write("ТОП-3 ГОРОДА ДЛЯ ПОСЕЩЕНИЯ СЕГОДНЯ:\n")
        top_cities = recommendations_df[recommendations_df['рекомендация'] == 'рекомендуется к посещению'].head(3)
        for idx, row in top_cities.iterrows():
            f.write(f"{idx+1}. {row['город']}\n")
            f.write(f"   Температура: {row['температура']}°C\n")
            f.write(f"   Погода: {row['погода']}\n")
            f.write(f"   Совет: {row['особые_советы']}\n\n")
        
        # Города, где лучше остаться дома
        f.write("ГОРОДА, ГДЕ ЛУЧШЕ ОСТАТЬСЯ ДОМА:\n")
        bad_cities = recommendations_df[recommendations_df['рекомендация'] == 'лучше остаться дома']
        if not bad_cities.empty:
            for idx, row in bad_cities.iterrows():
                f.write(f" {row['город']}: {row['температура']}°C, {row['погода']}\n")
        else:
            f.write("Нет таких городов сегодня\n")
        
        f.write("Специальные рекомендации:\n")
        
        # Специальные советы
        for idx, row in recommendations_df.iterrows():
            if row['особые_советы'] != 'нет особых рекомендаций':
                f.write(f" {row['город']}: {row['особые_советы']}\n")
    
    print(f"   Текстовый отчет: {report_file}")


def load_latest_enriched_file(enriched_path):
    """
    Загрузка самого свежего enriched файла
    """
    enriched_files = [f for f in os.listdir(enriched_path)
                      if f.startswith("weather_enriched_") and f.endswith(".csv")]
    
    if not enriched_files:
        print("Не найдены enriched файлы")
        return None
    
    latest_file = sorted(enriched_files)[-1]
    file_path = os.path.join(enriched_path , latest_file)

    print(f"Загрузка: {latest_file}")
    df = pd.read_csv(file_path, encoding='utf-8-sig')
    print(f"Загружено записей: {len(df)}")
    
    return df


def main():

    """Основная функция"""
    print("AGGREGATED LAYER: СОЗДАНИЕ ВИТРИН ДАННЫХ")


    # 1. Определение путей
    project_root = "E:/Semester 2/New Techonlogy/lecture 1/weather_tourism_pipeline"
    enriched_path = os.path.join(project_root, "data", "enriched")
    aggregated_path = os.path.join(project_root, "data", "aggregated")
    
    # Создание папки aggregated
    os.makedirs(aggregated_path, exist_ok=True)
    
    # 2. Загрузка данных
    print("\n Загрузка данных...")
    
    # Загрузка enriched данных
    df_enriched = load_latest_enriched_file(enriched_path)
    if df_enriched is None:
        return
    
    # Загрузка справочника городов
    reference_path = os.path.join(enriched_path, "cities_reference.csv")
    if not os.path.exists(reference_path):
        print(f" Файл справочника не найден: {reference_path}")
        return
    
    reference_df = pd.read_csv(reference_path, encoding='utf-8-sig')
    print(f" Загружен справочник: {len(reference_df)} городов")
    
    # Создание витрин    
    # Витрина 1
    rating_df = create_city_tourism_rating(df_enriched, reference_df, aggregated_path)
    
    # Витрина 2
    districts_df = create_federal_districts_summary(df_enriched, reference_df, aggregated_path)
    
    # Витрина 3
    recommendations_df = create_travel_recommendations(df_enriched, aggregated_path)
    

    print("АГРЕГАЦИЯ ЗАВЕРШЕНА УСПЕШНО!")

    print(f"Папка aggregated: {aggregated_path}")

if __name__ == "__main__":
    main()

