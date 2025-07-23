import pandas as pd
import datetime
from typing import Tuple, Dict, List
import matplotlib.pyplot as plt
import io
import logging
from aiogram.types import BufferedInputFile
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    filename='wb_bot.log'
)

class WBAnalytics:
    def __init__(self, data_file: str):
        self.data_file = data_file
        self.df = self._load_and_validate_data()

    def _load_data(self) -> pd.DataFrame:
        """Загрузка данных из CSV файла"""
        df = pd.read_csv(self.data_file)
        df['Дата'] = pd.to_datetime(df['Дата'])
        return df
    def article_exists(self, article: str) -> bool:
        """Проверить существование артикула (с учетом разных форматов)"""
        try:
            # Ищем как строку и как число
            str_articles = self.df['Артикул'].astype(str).tolist()
            num_articles = self.df['Артикул'].astype(int).astype(str).tolist()
            return article in str_articles or article in num_articles
        except Exception as e:
            logger.error(f"Ошибка проверки артикула {article}: {e}")
            return False
    def get_position_dynamics(self, article: str) -> Tuple[pd.DataFrame, Dict]:
        """Анализ динамики позиций для конкретного товара"""
        product_data = self.df[self.df['Артикул'] == article].copy()
        if product_data.empty:
            return None, None
        
        product_data = product_data.sort_values('Дата')
        stats = {
            'name': product_data['Название'].iloc[0],
            'category': product_data['Категория'].iloc[0],
            'first_check': product_data['Дата'].min().strftime('%d.%m.%Y'),
            'last_check': product_data['Дата'].max().strftime('%d.%m.%Y'),
            'queries_count': product_data['Запрос'].nunique(),
            'avg_position': round(product_data['Позиция'].mean(), 1),
            'best_position': product_data['Позиция'].min(),
            'worst_position': product_data['Позиция'].max(),
            'promo_percentage': round((product_data['Промо'] == 'Да').mean() * 100, 1)
        }
        
        return product_data, stats

    def generate_position_graph(self, article: str) -> BufferedInputFile:
        """Генерация графика динамики позиций"""
        product_data, stats = self.get_position_dynamics(article)
        if product_data is None:
            return None

        plt.figure(figsize=(10, 6))
        for query in product_data['Запрос'].unique():
            query_data = product_data[product_data['Запрос'] == query]
            plt.plot(
                query_data['Дата'], 
                query_data['Позиция'], 
                marker='o', 
                label=f"{query} (avg: {round(query_data['Позиция'].mean(), 1)})"
            )

        plt.gca().invert_yaxis()
        plt.title(f"Динамика позиций для {stats['name']}\nАртикул: {article}")
        plt.xlabel('Дата проверки')
        plt.ylabel('Позиция в выдаче')
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.grid(True)
        plt.tight_layout()

        buf = io.BytesIO()
        plt.savefig(buf, format='png', dpi=80)
        buf.seek(0)
        plt.close()
        
        return BufferedInputFile(buf.read(), filename=f"graph_{article}.png")
    def _load_and_validate_data(self) -> pd.DataFrame:
        """Загрузка и проверка данных"""
        try:
            df = pd.read_csv(self.data_file)
            
            # Проверка обязательных колонок
            required_columns = ['Артикул', 'Название', 'Категория', 'Позиция', 'Дата']
            for col in required_columns:
                if col not in df.columns:
                    raise ValueError(f"Отсутствует обязательная колонка: {col}")
            
            # Преобразование типов
            df['Дата'] = pd.to_datetime(df['Дата'])
            df['Артикул'] = df['Артикул'].astype(str)
            
            # Логирование информации о данных
            logger.info(f"Загружено {len(df)} записей. Пример данных:")
            logger.info(df.head(2).to_dict('records'))
            
            return df
            
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
            raise

    def get_available_articles(self) -> List[str]:
        """Получить список всех доступных артикулов из данных (как строки)"""
        try:
            return sorted(self.df['Артикул'].astype(str).unique().tolist())
        except Exception as e:
            logger.error(f"Ошибка получения списка артикулов: {e}")
            return []
    
    def get_available_categories(self) -> List[str]:
        """Получить список доступных категорий"""
        return self.df['Категория'].unique().tolist()
    
    def get_available_queries(self) -> List[str]:
        """Получить список доступных запросов"""
        return self.df['Запрос'].unique().tolist()

    def get_competition_analysis(self, category: str) -> Dict:
        """Анализ конкурентов в категории"""
        category_data = self.df[self.df['Категория'] == category]
        if category_data.empty:
            return None

        # Топ брендов в категории
        top_brands = category_data.groupby('Бренд').agg({
            'Артикул': 'nunique',
            'Позиция': 'mean'
        }).sort_values('Позиция').head(5).reset_index()
        top_brands['Позиция'] = top_brands['Позиция'].round(1)

        # Топ товаров в категории
        top_products = category_data.groupby(['Артикул', 'Название']).agg({
            'Позиция': 'mean',
            'Запрос': 'nunique'
        }).sort_values('Позиция').head(5).reset_index()
        top_products['Позиция'] = top_products['Позиция'].round(1)

        return {
            'category': category,
            'brands_count': category_data['Бренд'].nunique(),
            'products_count': category_data['Артикул'].nunique(),
            'avg_position': round(category_data['Позиция'].mean(), 1),
            'top_brands': top_brands.to_dict('records'),
            'top_products': top_products.to_dict('records')
        }

    def get_query_analysis(self, query: str) -> Dict:
        """Анализ позиций по конкретному запросу"""
        query_data = self.df[self.df['Запрос'] == query]
        if query_data.empty:
            return None

        # Динамика средней позиции по дням
        daily_stats = query_data.groupby(query_data['Дата'].dt.date).agg({
            'Позиция': 'mean',
            'Артикул': 'nunique'
        }).reset_index()
        daily_stats['Позиция'] = daily_stats['Позиция'].round(1)

        # Лучшие товары по этому запросу
        top_products = query_data.groupby(['Артикул', 'Название']).agg({
            'Позиция': 'mean',
            'Промо': lambda x: (x == 'Да').sum()
        }).sort_values('Позиция').head(5).reset_index()
        top_products['Позиция'] = top_products['Позиция'].round(1)

        return {
            'query': query,
            'first_check': query_data['Дата'].min().strftime('%d.%m.%Y'),
            'last_check': query_data['Дата'].max().strftime('%d.%m.%Y'),
            'products_count': query_data['Артикул'].nunique(),
            'avg_position': round(query_data['Позиция'].mean(), 1),
            'best_position': query_data['Позиция'].min(),
            'promo_percentage': round((query_data['Промо'] == 'Да').mean() * 100, 1),
            'daily_stats': daily_stats.to_dict('records'),
            'top_products': top_products.to_dict('records')
        }
    def get_product_data(self, article: str) -> Tuple[pd.DataFrame, Dict]:
        """Получить данные по артикулу с учетом всех возможных форматов"""
        try:
            # Пробуем найти артикул в разных форматах
            article_str = str(article).strip()
            article_int = int(article_str) if article_str.isdigit() else None
            
            # Ищем в данных
            product_data = self.df[
                (self.df['Артикул'].astype(str) == article_str) | 
                (self.df['Артикул'].astype(int).astype(str) == article_str)
            ]
            
            if product_data.empty and article_int is not None:
                product_data = self.df[self.df['Артикул'] == article_int]
            
            if product_data.empty:
                logger.error(f"Артикул {article} не найден в данных")
                return None, None
                
            # Собираем статистику
            stats = {
                'name': product_data['Название'].iloc[0],
                'category': product_data['Категория'].iloc[0],
                'first_check': product_data['Дата'].min().strftime('%d.%m.%Y'),
                'last_check': product_data['Дата'].max().strftime('%d.%m.%Y'),
                'queries_count': product_data['Запрос'].nunique(),
                'avg_position': round(product_data['Позиция'].mean(), 1),
                'best_position': product_data['Позиция'].min(),
                'worst_position': product_data['Позиция'].max(),
                'promo_percentage': round((product_data['Промо'] == 'Да').mean() * 100, 1)
            }
            
            return product_data, stats
            
        except Exception as e:
            logger.error(f"Ошибка в get_product_data для артикула {article}: {e}")
            return None, None
    def get_promo_effectiveness(self) -> Dict:
        """Анализ эффективности промо-позиций"""
        promo_data = self.df[self.df['Промо'] == 'Да']
        if promo_data.empty:
            return None

        # Сравнение средних позиций с промо и без
        promo_stats = self.df.groupby('Промо').agg({
            'Позиция': 'mean',
            'Артикул': 'nunique'
        }).reset_index()
        promo_stats['Позиция'] = promo_stats['Позиция'].round(1)

        # Эффективность промо по товарам
        product_promo = self.df.groupby(['Артикул', 'Название', 'Промо']).agg({
            'Позиция': 'mean'
        }).unstack().reset_index()
        product_promo.columns = ['Артикул', 'Название', 'Позиция_без_промо', 'Позиция_с_промо']
        product_promo['Разница'] = product_promo['Позиция_без_промо'] - product_promo['Позиция_с_промо']
        product_promo = product_promo.dropna().sort_values('Разница', ascending=False)

        return {
            'promo_products_count': promo_data['Артикул'].nunique(),
            'avg_promo_position': round(promo_data['Позиция'].mean(), 1),
            'avg_non_promo_position': round(self.df[self.df['Промо'] == 'Нет']['Позиция'].mean(), 1),
            'most_effective_promo': product_promo.head(5).to_dict('records'),
            'least_effective_promo': product_promo.tail(5).to_dict('records')
        }