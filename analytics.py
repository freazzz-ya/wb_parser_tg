import pandas as pd
import datetime
from typing import Tuple, Dict, List, Optional
import matplotlib.pyplot as plt
import io
import os
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
        self.avg_positions_file = 'avg_positions_data.csv'
        self.df = self._load_data()
        self.avg_df = self._load_avg_data()


    def _load_data(self) -> pd.DataFrame:
        """Загрузка основных данных"""
        try:
            if os.path.exists(self.data_file):
                df = pd.read_csv(self.data_file)
                df['Дата'] = pd.to_datetime(df['Дата'])
                df['Артикул'] = df['Артикул'].astype(str)
                logger.info(f"Данные загружены: {len(df)} записей")
                return df
        except Exception as e:
            logger.error(f"Ошибка загрузки данных: {e}")
        return pd.DataFrame(columns=['Артикул', 'Название', 'Категория', 'Позиция', 'Дата', 'Запрос', 'Промо'])


    def _initialize_data(self):
        """Инициализация данных"""
        try:
            self.df = self._load_and_validate_data()
            self.avg_df = self._load_avg_positions_data()
        except Exception as e:
            logger.error(f"Ошибка инициализации данных: {e}")
            self.df = pd.DataFrame(columns=['Артикул', 'Название', 'Категория', 'Позиция', 'Дата', 'Запрос', 'Промо'])
            self.avg_df = pd.DataFrame(columns=['Артикул', 'Средняя_позиция', 'Дата'])

    def _load_and_validate_data(self) -> pd.DataFrame:
        """Загрузка и валидация данных"""
        try:
            if not os.path.exists(self.data_file):
                logger.warning(f"Файл данных {self.data_file} не найден")
                return pd.DataFrame(columns=['Артикул', 'Название', 'Категория', 'Позиция', 'Дата', 'Запрос', 'Промо'])
            
            df = pd.read_csv(self.data_file)
            
            required_columns = ['Артикул', 'Название', 'Категория', 'Позиция', 'Дата', 'Запрос', 'Промо']
            for col in required_columns:
                if col not in df.columns:
                    logger.error(f"Отсутствует обязательная колонка: {col}")
                    return pd.DataFrame(columns=required_columns)
            
            df['Дата'] = pd.to_datetime(df['Дата'])
            df['Артикул'] = df['Артикул'].astype(str)
            self.update_avg_positions()
            
            logger.info(f"Успешно загружено {len(df)} записей")
            return df
            
        except Exception as e:
            logger.error(f"Критическая ошибка загрузки данных: {e}")
            return pd.DataFrame(columns=['Артикул', 'Название', 'Категория', 'Позиция', 'Дата', 'Запрос', 'Промо'])

    def _load_avg_data(self) -> pd.DataFrame:
        """Загрузка данных средних позиций"""
        try:
            if os.path.exists(self.avg_positions_file):
                df = pd.read_csv(self.avg_positions_file)
                df['Дата'] = pd.to_datetime(df['Дата'])
                return df
        except Exception as e:
            logger.error(f"Ошибка загрузки средних позиций: {e}")
        return pd.DataFrame(columns=['Артикул', 'Средняя_позиция', 'Дата'])


    def get_product_data(self, article: str) -> Tuple[Optional[pd.DataFrame], Optional[Dict]]:
        """Получение данных по товару"""
        try:
            article_str = str(article)
            product_data = self.df[self.df['Артикул'] == article_str]
            
            if product_data.empty:
                return None, None

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
            logger.error(f"Ошибка получения данных товара: {e}")
            return None, None
    def update_avg_positions(self, new_data: pd.DataFrame):
        """Обновление средних позиций БЕЗ очистки файла"""
        try:
            if new_data.empty:
                return

            # Рассчитываем средние для новых данных
            current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            new_avg = new_data.groupby('Артикул').agg({
                'Позиция': 'mean'
            }).reset_index()
            new_avg['Средняя_позиция'] = new_avg['Позиция'].round(1)
            new_avg['Дата'] = current_time
            new_avg = new_avg[['Артикул', 'Средняя_позиция', 'Дата']]

            # Загружаем существующие данные (если файл есть)
            if os.path.exists(self.avg_positions_file):
                existing_data = pd.read_csv(self.avg_positions_file)
            else:
                existing_data = pd.DataFrame(columns=['Артикул', 'Средняя_позиция', 'Дата'])

            # Просто добавляем новые строки в конец файла
            updated_data = pd.concat([existing_data, new_avg], ignore_index=True)
            
            # Сохраняем обновленные данные
            updated_data.to_csv(self.avg_positions_file, index=False)
            logger.info(f"Добавлены новые средние позиции. Всего записей: {len(updated_data)}")
            
        except Exception as e:
            logger.error(f"Ошибка обновления средних позиций: {e}")

    def generate_position_graph(self, article: str) -> Optional[BufferedInputFile]:
        """Генерация графика позиций по запросам"""
        try:
            product_data, _ = self.get_product_data(article)
            if product_data is None or product_data.empty:
                return None

            plt.figure(figsize=(12, 6))
            for query in product_data['Запрос'].unique():
                query_data = product_data[product_data['Запрос'] == query]
                plt.plot(
                    query_data['Дата'], 
                    query_data['Позиция'], 
                    marker='o',
                    label=f"{query} (avg: {round(query_data['Позиция'].mean(), 1)})"
                )

            plt.gca().invert_yaxis()
            plt.title(f"Динамика позиций по запросам\nАртикул: {article}")
            plt.xlabel('Дата')
            plt.ylabel('Позиция')
            plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
            plt.grid(True)
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            plt.close()
            return BufferedInputFile(buf.getvalue(), filename=f"positions_{article}.png")
        except Exception as e:
            logger.error(f"Ошибка генерации графика позиций: {e}")
            return None

    def generate_avg_position_graph(self, article: str) -> Optional[BufferedInputFile]:
        """Генерация графика с полной историей позиций"""
        try:
            # Проверяем наличие файла
            if not os.path.exists(self.avg_positions_file):
                logger.warning("Файл средних позиций не найден")
                return None
                
            # Загружаем все данные
            all_data = pd.read_csv(self.avg_positions_file)
            article_data = all_data[all_data['Артикул'].astype(str) == str(article)]
            
            if article_data.empty:
                logger.warning(f"Нет данных для артикула {article}")
                return None

            # Сортируем по дате и добавляем номер проверки
            article_data = article_data.sort_values('Дата')
            article_data['Проверка'] = range(1, len(article_data)+1)
            
            # Строим график
            plt.figure(figsize=(14, 7))
            
            # Основная линия с точками
            line = plt.plot(article_data['Проверка'], 
                        article_data['Средняя_позиция'], 
                        marker='o', 
                        linestyle='-', 
                        linewidth=2,
                        markersize=8,
                        color='blue')
            
            # Добавляем подписи
            for i, row in article_data.iterrows():
                date_str = pd.to_datetime(row['Дата']).strftime('%d.%m %H:%M')
                plt.text(
                    row['Проверка'], 
                    row['Средняя_позиция'], 
                    f"{row['Средняя_позиция']}\n({date_str})",
                    ha='center', 
                    va='bottom' if i%2 else 'top',
                    fontsize=9
                )

            plt.gca().invert_yaxis()
            plt.title(f"Полная история позиций\nАртикул: {article}", pad=20)
            plt.xlabel('Номер проверки')
            plt.ylabel('Средняя позиция')
            plt.xticks(article_data['Проверка'])
            plt.grid(True, linestyle='--', alpha=0.5)
            plt.tight_layout()

            # Сохраняем график
            buf = io.BytesIO()
            plt.savefig(buf, format='png', dpi=100)
            buf.seek(0)
            plt.close()
            
            return BufferedInputFile(buf.getvalue(), filename=f"full_history_{article}.png")
            
        except Exception as e:
            logger.error(f"Ошибка генерации графика: {e}")
            return None

    def get_available_articles(self) -> List[str]:
        """Получение списка доступных артикулов"""
        return sorted(self.df['Артикул'].astype(str).unique().tolist()) if not self.df.empty else []

    def get_product_data(self, article: str) -> Tuple[pd.DataFrame, Dict]:
        """Получение данных по товару"""
        try:
            article_str = str(article).strip()
            product_data = self.df[
                (self.df['Артикул'].astype(str) == article_str) | 
                (self.df['Артикул'].astype(int).astype(str) == article_str)
            ]
            
            if product_data.empty:
                return None, None
                
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
            logger.error(f"Ошибка получения данных товара: {e}")
            return None, None


    def get_available_categories(self) -> List[str]:
        """Получение списка категорий"""
        try:
            return self.df['Категория'].unique().tolist()
        except Exception as e:
            logger.error(f"Ошибка получения категорий: {e}")
            return []

    def get_available_queries(self) -> List[str]:
        """Получение списка запросов"""
        try:
            return self.df['Запрос'].unique().tolist()
        except Exception as e:
            logger.error(f"Ошибка получения запросов: {e}")
            return []

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