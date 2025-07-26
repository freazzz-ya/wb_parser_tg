import os
import time
import requests
import logging
import datetime
from tqdm import tqdm
import pandas as pd
from geopy.geocoders import Nominatim
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from analytics import WBAnalytics
import matplotlib
matplotlib.use('Agg')

# Конфигурация
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# Настройки парсера
CONFIG = {
    'CITIES': ['Москва'],
    'QUERIES_FILE': 'queries.txt',
    'MAX_PAGE': 10,
    'BRANDS': ['YalowShop'],
    'SUPPLIERS': ['YalowShop'],
    'USER_AGENT': "WB-Tracker-Bot",
    'GEO_FALLBACK_DEST': 123589415,
    'REQUEST_DELAY': 1,
    'TOKEN': '7862175726:AAFM6Ctd2dOYTZV-mZG8S-WwRs_uMuzHlqI',
    'DATA_FILE': 'positions_data.csv'
}

# Московский часовой пояс
MOSCOW_TZ = datetime.timezone(datetime.timedelta(hours=3))

# Состояния FSM
class Form(StatesGroup):
    waiting_for_article = State()
    waiting_for_category = State()
    waiting_for_query = State()

class WBParser:
    def __init__(self):
        self.geolocator = Nominatim(user_agent=CONFIG['USER_AGENT'])
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': CONFIG['USER_AGENT']})

    def load_queries(self):
        """Загрузка запросов из файла"""
        if not os.path.exists(CONFIG['QUERIES_FILE']):
            raise FileNotFoundError(f"Файл с запросами '{CONFIG['QUERIES_FILE']}' не найден")
        
        with open(CONFIG['QUERIES_FILE'], 'r', encoding='utf-8') as f:
            queries = [line.strip() for line in f if line.strip()]
        
        if not queries:
            raise ValueError(f"Файл '{CONFIG['QUERIES_FILE']}' не содержит запросов")
        
        return queries

    def get_city_params(self, city):
        """Получение геопараметров города"""
        try:
            location = self.geolocator.geocode(city)
            if not location:
                logger.warning(f"Город {city} не найден.")
                return None

            response = self.session.get(
                'https://user-geo-data.wildberries.ru/get-geo-info',
                params={
                    'latitude': location.latitude,
                    'longitude': location.longitude,
                    'address': city
                },
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Ошибка получения geo-info для {city}: {e}")
            return None

    def parse_products(self, query, dest_id):
        """Парсинг товаров по запросу"""
        results = []
        
        for page in range(1, CONFIG['MAX_PAGE'] + 1):
            try:
                response = self.session.get(
                    'https://search.wb.ru/exactmatch/ru/common/v13/search',
                    params={
                        'ab_testid': 'no_action',
                        'appType': 1,
                        'curr': 'rub',
                        'dest': dest_id,
                        'hide_dtype': 13,
                        'lang': 'ru',
                        'page': page,
                        'query': query,
                        'resultset': 'catalog',
                        'sort': 'popular',
                        'spp': 30,
                        'suppressSpellcheck': 'false'
                    },
                    timeout=15
                )
                response.raise_for_status()
                products = response.json()['data']['products']
            except Exception as e:
                logger.error(f"Ошибка запроса для '{query}' (стр. {page}): {e}")
                continue
            
            for idx, product in enumerate(products):
                if self.is_target_product(product):
                    results.append(self.process_product(product, query, page, idx))
            
            time.sleep(CONFIG['REQUEST_DELAY'])
        
        return results

    def is_target_product(self, product):
        """Проверка, является ли товар целевым"""
        brand = product.get('brand', '').strip().lower()
        supplier = product.get('supplier', '').strip()
        return brand in CONFIG['BRANDS'] or supplier in CONFIG['SUPPLIERS']

    def process_product(self, product, query, page, idx):
        """Обработка данных товара"""
        log_data = product.get('log', {})
        global_idx = (page - 1) * 100 + idx + 1
        
        return {
            'Название': product.get('name', ''),
            'CPM': log_data.get('cpm', 0),
            'Позиция': global_idx,
            'Промо позиция': log_data.get('promoPosition'),
            'Орг. позиция': log_data.get('position', idx + 1),
            'Тип': log_data.get('tp', '-'),
            'Запрос': query,
            'Дата': datetime.datetime.now(MOSCOW_TZ),
            'Промо': 'Да' if log_data.get('promoPosition') is not None else 'Нет',
            'Город': 'Калуга',
            'Артикул': product['id'],
            'Бренд': product.get('brand', ''),
            'Поставщик': product.get('supplier', ''),
            'Категория': product['entity']
        }

class TelegramBot:
    def __init__(self):
        self.bot = Bot(token=CONFIG['TOKEN'])
        self.dp = Dispatcher()
        self.parser = WBParser()
        self.setup_handlers()
        self.previous_data = None
        self.current_data = None
        self.last_check_time = None
        self.last_compare_time = None

    def setup_handlers(self):
        """Настройка обработчиков команд"""
        # Команды
        self.dp.message(Command("start"))(self.start_handler)
        self.dp.message(Command("info"))(self.info_handler)
        
        # Кнопки меню
        self.dp.message(F.text == '📊 Проверить позиции')(self.check_positions_handler)
        self.dp.message(F.text == '📈 Сравнить с предыдущим')(self.compare_results_handler)
        self.dp.message(F.text == '📌 Топ товаров')(self.top_products_handler)
        self.dp.message(F.text == '📉 Динамика товара')(self.product_dynamics_handler)
        self.dp.message(F.text == '🏆 Анализ категории')(self.category_analysis_handler)
        self.dp.message(F.text == '🔍 Анализ запроса')(self.query_analysis_handler)
        self.dp.message(F.text == '📢 Эффективность промо')(self.promo_analysis_handler)
        self.dp.message(F.text == 'ℹ️ Информация')(self.info_handler)
        
        # Обработчики состояний
        self.dp.message(StateFilter(Form.waiting_for_article))(self.handle_article_input)
        self.dp.message(StateFilter(Form.waiting_for_category))(self.handle_category_input)
        self.dp.message(StateFilter(Form.waiting_for_query))(self.handle_query_input)
        
        # Обработчик неизвестных команд
        self.dp.message()(self.unknown_command_handler)

    async def start_handler(self, message: types.Message, state: FSMContext):
        """Обработчик команды /start"""
        await state.clear()
        await self.show_main_menu(message)

    async def show_main_menu(self, message: types.Message):
        """Показать главное меню"""
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="📊 Проверить позиции"))
        builder.row(KeyboardButton(text="📈 Сравнить с предыдущим"))
        builder.row(KeyboardButton(text="📌 Топ товаров"))
        builder.row(
            KeyboardButton(text="📉 Динамика товара"),
            KeyboardButton(text="🏆 Анализ категории")
        )
        builder.row(
            KeyboardButton(text="🔍 Анализ запроса"),
            KeyboardButton(text="📢 Эффективность промо")
        )
        builder.row(KeyboardButton(text="ℹ️ Информация"))
        
        await message.answer(
            f"👋 Привет, {message.from_user.first_name}!\n"
            "Я твой персональный аналитик Wildberries!\n"
            "Выбери действие из меню ниже:",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )

    async def info_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Информация'"""
        await state.clear()
        await message.answer(
            "📚 <b>Информация о боте:</b>\n\n"
            "Я помогаю отслеживать позиции товаров на Wildberries и анализировать эффективность рекламных кампаний.\n\n"
            "🔹 <b>Основные функции:</b>\n"
            "• Проверка текущих позиций товаров\n"
            "• Сравнение с предыдущими результатами\n"
            "• Анализ эффективности продвижения\n"
            "• Рекомендации по оптимизации\n\n"
            "📅 Все временные данные отображаются в московском часовом поясе (МСК).",
            parse_mode="HTML"
        )

    async def unknown_command_handler(self, message: types.Message, state: FSMContext):
        """Обработчик неизвестных команд"""
        current_state = await state.get_state()
        if current_state:
            await message.answer("Пожалуйста, завершите текущее действие или нажмите /cancel")
        else:
            await message.answer("🤔 Я не понимаю эту команду. Пожалуйста, используйте кнопки меню.")
            await self.show_main_menu(message)

    # Обработчики кнопок
    async def product_dynamics_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Динамика товара' с улучшенной обработкой ошибок"""
        try:
            # Проверяем существование файла данных
            if not os.path.exists(CONFIG['DATA_FILE']):
                await message.answer("❌ Файл с данными не найден. Сначала выполните проверку позиций.")
                return

            # Инициализируем аналитику
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            
            # Проверяем, есть ли данные
            if analytics.df.empty:
                await message.answer("❌ Нет данных для анализа. Файл может быть пуст или поврежден.")
                return

            articles = analytics.get_available_articles()
            
            if not articles:
                await message.answer("❌ В данных нет артикулов для анализа.")
                return

            # Создаем клавиатуру
            builder = ReplyKeyboardBuilder()
            for article in articles[:50]:  # Ограничиваем количество кнопок
                builder.add(KeyboardButton(text=str(article)))
            builder.adjust(4)
            builder.add(KeyboardButton(text="❌ Отмена"))
            
            await state.set_state(Form.waiting_for_article)
            await message.answer(
                "📊 <b>Выберите артикул из списка:</b>\n"
                f"Всего товаров в базе: {len(articles)}\n"
                "Или введите артикул вручную",
                reply_markup=builder.as_markup(resize_keyboard=True),
                parse_mode="HTML"
            )
            
        except Exception as e:
            logger.error(f"Ошибка в product_dynamics_handler: {str(e)}", exc_info=True)
            await message.answer(
                "⚠️ Произошла ошибка при загрузке данных. Попробуйте позже или проверьте файл данных."
            )
            await state.clear()

    async def handle_article_input(self, message: types.Message, state: FSMContext):
        """Обработка ввода артикула с правильной проверкой DataFrame"""
        try:
            article = message.text.strip()
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            
            # Получаем данные и статистику (исправленная проверка)
            product_data, stats = analytics.get_product_data(article)
            if product_data is None or product_data.empty or stats is None:
                await message.answer(f"❌ Товар с артикулом {article} не найден")
                return

            # Генерируем оба графика
            position_graph = analytics.generate_position_graph(article)
            avg_position_graph = analytics.generate_avg_position_graph(article)
            
            # Проверяем, что хотя бы один график создан
            if position_graph is None and avg_position_graph is None:
                await message.answer("❌ Не удалось сгенерировать графики")
                return

            # Формируем текстовый ответ
            response_text = (
                f"📊 <b>Аналитика по товару:</b> {stats['name']}\n"
                f"🔹 Артикул: {article}\n"
                f"📁 Категория: {stats['category']}\n"
                f"📅 Период: {stats['first_check']} - {stats['last_check']}\n\n"
                f"📌 <b>Статистика:</b>\n"
                f"• Средняя позиция: {stats['avg_position']}\n"
                f"• Лучшая позиция: {stats['best_position']}\n"
                f"• Худшая позиция: {stats['worst_position']}\n"
                f"• Отслеживается по {stats['queries_count']} запросам\n"
                f"• Промо-позиций: {stats['promo_percentage']}%\n\n"
                f"<i>Прокрутите вниз чтобы увидеть графики</i>"
            )

            # Отправляем текстовое сообщение
            await message.answer(response_text, parse_mode="HTML")

            # Отправляем графики
            if position_graph and avg_position_graph:
                # Если есть оба графика - отправляем медиагруппой
                media_group = [
                    types.InputMediaPhoto(media=position_graph, caption="📈 Динамика позиций по запросам"),
                    types.InputMediaPhoto(media=avg_position_graph, caption="📊 Динамика средних позиций")
                ]
                await message.answer_media_group(media_group)
            elif position_graph:
                await message.answer_photo(
                    photo=position_graph,
                    caption="📈 Динамика позиций по запросам"
                )
            elif avg_position_graph:
                await message.answer_photo(
                    photo=avg_position_graph,
                    caption="📊 Динамика средних позиций"
                )

        except Exception as e:
            logger.error(f"Ошибка в handle_article_input: {str(e)}", exc_info=True)
            await message.answer("⚠️ Произошла ошибка при обработке запроса")
        finally:
            await state.clear()
            await self.show_main_menu(message)

    async def category_analysis_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Анализ категории'"""
        await state.set_state(Form.waiting_for_category)
        await message.answer(
            "Введите название категории для анализа:",
            reply_markup=types.ReplyKeyboardRemove()
        )

    async def handle_category_input(self, message: types.Message, state: FSMContext):
        """Обработка выбора категории"""
        if message.text == "❌ Отмена":
            await state.clear()
            await self.show_main_menu(message)
            return

        category = message.text.strip()
        await state.clear()

        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            available_categories = analytics.get_available_categories()
            
            if category not in available_categories:
                await message.answer(
                    f"❌ Категория '{category}' не найдена в данных.\n"
                    "Попробуйте выбрать категорию из списка:",
                    reply_markup=types.ReplyKeyboardRemove()
                )
                await self.category_analysis_handler(message, state)
                return

            analysis = analytics.get_competition_analysis(category)
            
            if analysis is None:
                await message.answer(f"❌ Не удалось проанализировать категорию '{category}'")
                await self.show_main_menu(message)
                return

            response = (
                f"📊 <b>Анализ категории:</b> {analysis['category']}\n\n"
                f"📌 <b>Общая статистика:</b>\n"
                f"• Количество брендов: {analysis['brands_count']}\n"
                f"• Количество товаров: {analysis['products_count']}\n"
                f"• Средняя позиция: {analysis['avg_position']}\n\n"
                f"🏆 <b>Топ-5 брендов:</b>\n"
            )
            
            for brand in analysis['top_brands']:
                response += f"• {brand['Бренд']} - ср.поз. {brand['Позиция']} (товаров: {brand['Артикул']})\n"
            
            response += "\n🛍️ <b>Топ-5 товаров:</b>\n"
            for product in analysis['top_products']:
                response += f"• {product['Название']} (арт. {product['Артикул']}) - ср.поз. {product['Позиция']}\n"
            
            await message.answer(
                response, 
                parse_mode="HTML",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await self.show_main_menu(message)
            
        except Exception as e:
            logger.error(f"Ошибка в handle_category_input: {e}")
            await message.answer(
                f"⚠️ Произошла ошибка: {str(e)}",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await self.show_main_menu(message)

    async def query_analysis_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Анализ запроса'"""
        await state.set_state(Form.waiting_for_query)
        await message.answer(
            "Введите поисковый запрос для анализа:",
            reply_markup=types.ReplyKeyboardRemove()
        )

    async def handle_query_input(self, message: types.Message, state: FSMContext):
        """Обработка выбора поискового запроса"""
        if message.text == "❌ Отмена":
            await state.clear()
            await self.show_main_menu(message)
            return

        query = message.text.strip()
        await state.clear()

        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            available_queries = analytics.get_available_queries()
            
            if query not in available_queries:
                await message.answer(
                    f"❌ Запрос '{query}' не найден в данных.\n"
                    "Попробуйте выбрать запрос из списка:",
                    reply_markup=types.ReplyKeyboardRemove()
                )
                await self.query_analysis_handler(message, state)
                return

            analysis = analytics.get_query_analysis(query)
            
            if analysis is None:
                await message.answer(f"❌ Не удалось проанализировать запрос '{query}'")
                await self.show_main_menu(message)
                return

            response = (
                f"🔍 <b>Анализ запроса:</b> {analysis['query']}\n\n"
                f"📌 <b>Общая статистика:</b>\n"
                f"• Первая проверка: {analysis['first_check']}\n"
                f"• Последняя проверка: {analysis['last_check']}\n"
                f"• Количество товаров: {analysis['products_count']}\n"
                f"• Средняя позиция: {analysis['avg_position']}\n"
                f"• Лучшая позиция: {analysis['best_position']}\n"
                f"• Процент промо-позиций: {analysis['promo_percentage']}%\n\n"
                f"🏆 <b>Топ-5 товаров по этому запросу:</b>\n"
            )
            
            for product in analysis['top_products']:
                promo_info = f" (промо: {product['Промо']} раз)" if product['Промо'] > 0 else ""
                response += f"• {product['Название']} (арт. {product['Артикул']}) - ср.поз. {product['Позиция']}{promo_info}\n"
            
            await message.answer(
                response, 
                parse_mode="HTML",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await self.show_main_menu(message)
            
        except Exception as e:
            logger.error(f"Ошибка в handle_query_input: {e}")
            await message.answer(
                f"⚠️ Произошла ошибка: {str(e)}",
                reply_markup=types.ReplyKeyboardRemove()
            )
            await self.show_main_menu(message)

    async def promo_analysis_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Эффективность промо'"""
        await state.clear()
        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            analysis = analytics.get_promo_effectiveness()
            
            if analysis is None:
                await message.answer("❌ Нет данных о промо-позициях")
                await self.show_main_menu(message)
                return

            response = (
                "📢 <b>Анализ эффективности промо-позиций:</b>\n\n"
                f"• Товаров с промо: {analysis['promo_products_count']}\n"
                f"• Средняя позиция с промо: {analysis['avg_promo_position']}\n"
                f"• Средняя позиция без промо: {analysis['avg_non_promo_position']}\n\n"
                "🚀 <b>Самые эффективные промо:</b>\n"
            )
            
            for product in analysis['most_effective_promo']:
                improvement = round(product['Разница'], 1)
                response += f"• {product['Название']} (арт. {product['Артикул']}) - улучшение на {improvement} позиций\n"
            
            response += "\n⚠️ <b>Наименее эффективные промо:</b>\n"
            for product in analysis['least_effective_promo']:
                improvement = round(product['Разница'], 1)
                response += f"• {product['Название']} (арт. {product['Артикул']}) - улучшение на {improvement} позиций\n"
            
            await message.answer(response, parse_mode="HTML")
            await self.show_main_menu(message)
            
        except Exception as e:
            logger.error(f"Ошибка при анализе промо: {e}")
            await message.answer(f"⚠️ Произошла ошибка: {e}")
            await self.show_main_menu(message)














    async def show_main_menu(self, message: types.Message):
        """Показать главное меню"""
        builder = ReplyKeyboardBuilder()
        builder.row(KeyboardButton(text="📊 Проверить позиции"))
        builder.row(KeyboardButton(text="📈 Сравнить с предыдущим"))
        builder.row(KeyboardButton(text="📌 Топ товаров"))
        builder.row(
            KeyboardButton(text="📉 Динамика товара"),
            KeyboardButton(text="🏆 Анализ категории")
        )
        builder.row(
            KeyboardButton(text="🔍 Анализ запроса"),
            KeyboardButton(text="📢 Эффективность промо")
        )
        builder.row(KeyboardButton(text="ℹ️ Информация"))
        
        await message.answer(
            f"👋 Привет, {message.from_user.first_name}!\n"
            "Я твой персональный аналитик Wildberries!\n"
            "Выбери действие из меню ниже:",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
    async def product_dynamics_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Динамика товара' с улучшенной проверкой данных"""
        try:
            if not os.path.exists(CONFIG['DATA_FILE']):
                await message.answer("❌ Файл с данными не найден. Сначала выполните проверку позиций.")
                return

            # Логирование для отладки
            logger.info(f"Запущен product_dynamics_handler, размер файла: {os.path.getsize(CONFIG['DATA_FILE'])} байт")
            
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            articles = analytics.get_available_articles()
            
            if not articles:
                await message.answer("❌ В файле данных нет артикулов. Возможно файл пуст или поврежден.")
                return

            # Проверяем первые 5 артикулов для отладки
            logger.info(f"Первые 5 артикулов в данных: {articles[:5]}")
            
            # Создаем клавиатуру
            builder = ReplyKeyboardBuilder()
            for article in articles[:50]:
                builder.add(KeyboardButton(text=str(article)))
            builder.adjust(4)
            builder.add(KeyboardButton(text="❌ Отмена"))
            
            await state.set_state(Form.waiting_for_article)
            await message.answer(
                "📊 <b>Выберите артикул из списка:</b>\n"
                f"Всего товаров в базе: {len(articles)}\n"
                "Или введите артикул вручную",
                reply_markup=builder.as_markup(resize_keyboard=True),
                parse_mode="HTML"
            )
            
        except Exception as e:
            logger.error(f"Ошибка в product_dynamics_handler: {e}")
            await message.answer(
                "⚠️ Не удалось загрузить список артикулов. "
                "Проверьте файл данных или выполните новую проверку позиций."
            )
            await state.clear()

    async def category_analysis_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Анализ категории' с клавиатурой выбора"""
        if not os.path.exists(CONFIG['DATA_FILE']):
            await message.answer("❌ Нет данных для анализа. Сначала выполните проверку позиций.")
            return

        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            categories = analytics.get_available_categories()
            
            if not categories:
                await message.answer("❌ В данных нет информации о категориях.")
                return

            builder = ReplyKeyboardBuilder()
            for category in categories[:50]:  # Ограничиваем количество кнопок
                builder.add(KeyboardButton(text=category))
            builder.adjust(3)
            
            cancel_button = KeyboardButton(text="❌ Отмена")
            builder.add(cancel_button)
            
            await state.set_state(Form.waiting_for_category)
            await message.answer(
                "📁 <b>Выберите категорию для анализа:</b>\n"
                "Или введите название категории вручную",
                reply_markup=builder.as_markup(resize_keyboard=True),
                parse_mode="HTML"
            )
            
        except Exception as e:
            logger.error(f"Ошибка в category_analysis_handler: {e}")
            await message.answer("⚠️ Произошла ошибка при загрузке данных")
            await state.clear()
        
    async def show_query_analysis(self, message: types.Message, query: str):
        """Показать анализ запроса"""
        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            analysis = analytics.get_query_analysis(query)
            
            if analysis is None:
                await message.answer(f"❌ Запрос '{query}' не найден в данных")
                return

            response = (
                f"🔍 <b>Анализ запроса:</b> {analysis['query']}\n\n"
                f"📌 <b>Общая статистика:</b>\n"
                f"• Первая проверка: {analysis['first_check']}\n"
                f"• Последняя проверка: {analysis['last_check']}\n"
                f"• Количество товаров: {analysis['products_count']}\n"
                f"• Средняя позиция: {analysis['avg_position']}\n"
                f"• Лучшая позиция: {analysis['best_position']}\n"
                f"• Процент промо-позиций: {analysis['promo_percentage']}%\n\n"
                f"🏆 <b>Топ-5 товаров по этому запросу:</b>\n"
            )
            
            for product in analysis['top_products']:
                promo_info = f" (промо: {product['Промо']} раз)" if product['Промо'] > 0 else ""
                response += f"• {product['Название']} (арт. {product['Артикул']}) - ср.поз. {product['Позиция']}{promo_info}\n"
            
            await message.answer(response, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Ошибка при анализе запроса: {e}")
            await message.answer(f"⚠️ Произошла ошибка: {e}")


    async def show_product_dynamics(self, message: types.Message, article: str):
        """Показать динамику товара"""
        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            product_data, stats = analytics.get_position_dynamics(article)
            
            if product_data is None:
                await message.answer(f"❌ Товар с артикулом {article} не найден в данных")
                return

            graph = analytics.generate_position_graph(article)
            
            response = (
                f"📊 <b>Аналитика по товару:</b> {stats['name']}\n"
                f"🔹 Артикул: {article}\n"
                f"📁 Категория: {stats['category']}\n"
                f"🕒 Первая проверка: {stats['first_check']}\n"
                f"🕒 Последняя проверка: {stats['last_check']}\n\n"
                f"📌 <b>Статистика:</b>\n"
                f"• Средняя позиция: {stats['avg_position']}\n"
                f"• Лучшая позиция: {stats['best_position']}\n"
                f"• Худшая позиция: {stats['worst_position']}\n"
                f"• Количество отслеживаемых запросов: {stats['queries_count']}\n"
                f"• Процент промо-позиций: {stats['promo_percentage']}%\n\n"
                f"<i>График динамики позиций:</i>"
            )
            
            await message.answer_photo(graph, caption=response, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Ошибка при анализе динамики товара: {e}")
            await message.answer(f"⚠️ Произошла ошибка: {e}")

    async def query_analysis_handler(self, message: types.Message, state: FSMContext):
        """Обработчик кнопки 'Анализ запроса' с клавиатурой выбора"""
        if not os.path.exists(CONFIG['DATA_FILE']):
            await message.answer("❌ Нет данных для анализа. Сначала выполните проверку позиций.")
            return

        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            queries = analytics.get_available_queries()
            
            if not queries:
                await message.answer("❌ В данных нет информации о запросах.")
                return

            builder = ReplyKeyboardBuilder()
            for query in queries[:50]:  # Ограничиваем количество кнопок
                builder.add(KeyboardButton(text=query))
            builder.adjust(2)
            
            cancel_button = KeyboardButton(text="❌ Отмена")
            builder.add(cancel_button)
            
            await state.set_state(Form.waiting_for_query)
            await message.answer(
                "🔍 <b>Выберите поисковый запрос для анализа:</b>\n"
                "Или введите запрос вручную",
                reply_markup=builder.as_markup(resize_keyboard=True),
                parse_mode="HTML"
            )
            
        except Exception as e:
            logger.error(f"Ошибка в query_analysis_handler: {e}")
            await message.answer("⚠️ Произошла ошибка при загрузке данных")
            await state.clear()
    
    async def show_category_analysis(self, message: types.Message, category: str):
        """Показать анализ категории"""
        try:
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            analysis = analytics.get_competition_analysis(category)
            
            if analysis is None:
                await message.answer(f"❌ Категория '{category}' не найдена в данных")
                return

            response = (
                f"📊 <b>Анализ категории:</b> {analysis['category']}\n\n"
                f"📌 <b>Общая статистика:</b>\n"
                f"• Количество брендов: {analysis['brands_count']}\n"
                f"• Количество товаров: {analysis['products_count']}\n"
                f"• Средняя позиция: {analysis['avg_position']}\n\n"
                f"🏆 <b>Топ-5 брендов:</b>\n"
            )
            
            for brand in analysis['top_brands']:
                response += f"• {brand['Бренд']} - ср.поз. {brand['Позиция']} (товаров: {brand['Артикул']})\n"
            
            response += "\n🛍️ <b>Топ-5 товаров:</b>\n"
            for product in analysis['top_products']:
                response += f"• {product['Название']} (арт. {product['Артикул']}) - ср.поз. {product['Позиция']}\n"
            
            await message.answer(response, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Ошибка при анализе категории: {e}")
            await message.answer(f"⚠️ Произошла ошибка: {e}")


    async def top_products_handler(self, message: types.Message):
        """Обработчик кнопки 'Топ товаров'"""
        if not os.path.exists(CONFIG['DATA_FILE']):
            await message.answer("❌ Нет данных для анализа. Сначала выполните проверку позиций.")
            return
        
        try:
            await message.answer("⏳ Анализирую топ товаров...")
            
            df = pd.read_csv(CONFIG['DATA_FILE'])
            
            # Топ 5 товаров с лучшими средними позициями
            top_products = df.groupby(['Артикул', 'Название']).agg({
                'Позиция': 'mean',
                'Запрос': 'count'
            }).sort_values(by='Позиция').head(5)
            
            # Топ 5 запросов с лучшими позициями
            top_queries = df.groupby('Запрос').agg({
                'Позиция': 'mean',
                'Артикул': 'count'
            }).sort_values(by='Позиция').head(5)
            
            response_message = (
                "🏆 <b>Топ 5 товаров с лучшими позициями:</b>\n\n"
            )
            
            for (article, name), row in top_products.iterrows():
                response_message += (
                    f"🔹 <b>{name}</b>\n"
                    f"Артикул: {article}\n"
                    f"Средняя позиция: {round(row['Позиция'], 1)}\n"
                    f"Количество запросов: {row['Запрос']}\n\n"
                )
            
            response_message += (
                "\n🔍 <b>Топ 5 запросов с лучшими позициями:</b>\n\n"
            )
            
            for query, row in top_queries.iterrows():
                response_message += (
                    f"🔹 <i>{query}</i>\n"
                    f"Средняя позиция: {round(row['Позиция'], 1)}\n"
                    f"Количество товаров: {row['Артикул']}\n\n"
                )
            
            await message.answer(response_message, parse_mode="HTML")
            
        except Exception as e:
            logger.error(f"Ошибка при анализе топ товаров: {e}")
            await message.answer(f"⚠️ Произошла ошибка: {e}")

    def generate_analytics(self, comparison_df):
        """Генерация аналитики по изменениям позиций"""
        total_changes = len(comparison_df)
        improved = len(comparison_df[comparison_df['Изменение'] > 0])
        worsened = len(comparison_df[comparison_df['Изменение'] < 0])
        unchanged = len(comparison_df[comparison_df['Изменение'] == 0])
        
        avg_improvement = round(comparison_df[comparison_df['Изменение'] > 0]['Изменение'].mean(), 1) if improved > 0 else 0
        avg_worsening = round(abs(comparison_df[comparison_df['Изменение'] < 0]['Изменение'].mean()), 1) if worsened > 0 else 0
        
        promo_improved = len(comparison_df[(comparison_df['Изменение'] > 0) & (comparison_df['Промо_после'] == 'Да')])
        promo_total = len(comparison_df[comparison_df['Промо_после'] == 'Да'])
        
        analytics = (
            "📈 <b>Аналитика изменений:</b>\n\n"
            f"• Улучшили позиции: {improved} ({round(improved/total_changes*100)}%)\n"
            f"• Ухудшили позиции: {worsened} ({round(worsened/total_changes*100)}%)\n"
            f"• Без изменений: {unchanged} ({round(unchanged/total_changes*100)}%)\n\n"
            f"• Среднее улучшение: +{avg_improvement} позиций\n"
            f"• Среднее ухудшение: -{avg_worsening} позиций\n\n"
        )
        
        if promo_total > 0:
            analytics += (
                f"📢 <b>Эффективность промо:</b>\n"
                f"• Товаров с промо: {promo_total}\n"
                f"• Из них улучшили позиции: {promo_improved} ({round(promo_improved/promo_total*100)}%)\n\n"
            )
        
        # Рекомендации
        if improved / total_changes > 0.7:
            analytics += "🎉 <b>Отличные результаты!</b> Большинство позиций улучшилось. Продолжайте в том же духе!\n"
        elif worsened / total_changes > 0.5:
            analytics += "⚠️ <b>Внимание!</b> Больше половины позиций ухудшилось. Рекомендуется пересмотреть стратегию продвижения.\n"
        
        if (promo_total > 0) and (promo_improved / promo_total < 0.3):
            analytics += "📢 <b>Совет:</b> Промо-кампании работают неэффективно. Попробуйте изменить настройки рекламы.\n"
        
        return analytics

    def format_timedelta(self, td):
        """Форматирование временного интервала в читаемый вид"""
        days = td.days
        hours, remainder = divmod(td.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days > 0:
            parts.append(f"{days} д.")
        if hours > 0:
            parts.append(f"{hours} ч.")
        if minutes > 0:
            parts.append(f"{minutes} мин.")
        if seconds > 0 and not parts:
            parts.append(f"{seconds} сек.")
        
        return " ".join(parts) if parts else "менее минуты"

    async def check_positions_handler(self, message: types.Message, state: FSMContext):
        """Проверка позиций с сохранением данных для сравнения"""
        await message.answer("⏳ Начинаю сбор данных... Это может занять несколько минут.")
        
        try:
            # Сохраняем текущие данные как предыдущие перед обновлением
            if os.path.exists(CONFIG['DATA_FILE']):
                self.previous_data = pd.read_csv(CONFIG['DATA_FILE'])
                self.last_check_time = datetime.datetime.now()
            
            # Собираем новые данные
            data = []
            queries = self.parser.load_queries()
            
            for city in CONFIG['CITIES']:
                geo_info = self.parser.get_city_params(city)
                dest_id = geo_info.get('dest', CONFIG['GEO_FALLBACK_DEST']) if geo_info else CONFIG['GEO_FALLBACK_DEST']
                
                for query in queries:
                    data.extend(self.parser.parse_products(query, dest_id))
                    time.sleep(CONFIG['REQUEST_DELAY'])
            
            if not data:
                await message.answer("❌ Не удалось собрать данные. Попробуйте позже.")
                return
                
            # Сохраняем новые данные
            self.current_data = pd.DataFrame(data)
            self.current_data.to_csv(CONFIG['DATA_FILE'], index=False)
            
            # Обновляем средние позиции
            analytics = WBAnalytics(CONFIG['DATA_FILE'])
            analytics.update_avg_positions(self.current_data)
            
            await self.send_results(message, self.current_data)
            
        except Exception as e:
            logger.error(f"Ошибка при проверке позиций: {e}", exc_info=True)
            await message.answer(f"⚠️ Произошла ошибка: {e}")

    async def compare_results_handler(self, message: types.Message, state: FSMContext):
        """Сравнение текущих позиций с предыдущими"""
        try:
            # Проверяем наличие данных для сравнения
            if not hasattr(self, 'previous_data') or self.previous_data is None:
                await message.answer("❌ Нет данных предыдущей проверки. Сначала выполните проверку позиций.")
                return
                
            if not hasattr(self, 'current_data') or self.current_data is None:
                await message.answer("❌ Нет текущих данных. Сначала выполните проверку позиций.")
                return

            # Объединяем данные для сравнения
            merged = pd.merge(
                self.previous_data, 
                self.current_data,
                on=['Артикул', 'Название', 'Запрос'],
                suffixes=('_prev', '_curr'),
                how='inner'
            )

            if merged.empty:
                await message.answer("❌ Нет общих товаров для сравнения")
                return

            # Формируем сообщение
            response = ["📊 <b>Сравнение с предыдущей проверкой:</b>\n"]
            
            for art in merged['Артикул'].unique():
                art_data = merged[merged['Артикул'] == art]
                product_name = art_data['Название'].iloc[0]
                
                response.append(f"\n🔹 <b>{product_name}</b> (арт. {art})")
                
                for _, row in art_data.iterrows():
                    change = row['Позиция_prev'] - row['Позиция_curr']
                    if change > 0:
                        emoji = "🟢"
                        change_text = f"+{change}"
                    elif change < 0:
                        emoji = "🔴"
                        change_text = f"{change}"
                    else:
                        emoji = "⚪"
                        change_text = "0"
                    
                    response.append(
                        f"{emoji} <i>{row['Запрос']}</i>\n"
                        f"   Было: {row['Позиция_prev']} → Стало: {row['Позиция_curr']}\n"
                        f"   Изменение: {change_text}"
                    )

            # Разбиваем сообщение на части, если слишком длинное
            msg_parts = []
            current_part = ""
            
            for line in response:
                if len(current_part + line) > 4000:
                    msg_parts.append(current_part)
                    current_part = line
                else:
                    current_part += "\n" + line
                    
            if current_part:
                msg_parts.append(current_part)
            
            # Отправляем сообщения
            for part in msg_parts:
                await message.answer(part, parse_mode="HTML")
                await asyncio.sleep(1)
                
            await message.answer("✅ Сравнение завершено")

        except Exception as e:
            logger.error(f"Ошибка при сравнении: {e}", exc_info=True)
            await message.answer("⚠️ Произошла ошибка при сравнении данных")


    async def send_results(self, message: types.Message, data):
        """Отправка результатов пользователю"""
        df = pd.DataFrame(data)
        grouped = df.groupby(['Категория', 'Артикул', 'Название'])
        
        messages = []
        current_message = (
            f"🕒 <b>Время проверки:</b> {datetime.datetime.now(MOSCOW_TZ).strftime('%d.%m.%Y %H:%M')} (МСК)\n\n"
        )
        
        for (category, article, name), group in grouped:
            message_part = (
                f"📌 <b>Категория:</b> {category}\n"
                f"🔹 <b>Артикул:</b> {article}\n"
                f"<b>Название:</b> {name}\n"
            )
            
            positions = []
            for _, row in group.iterrows():
                pos = f"   • <i>{row['Запрос']}</i> - позиция {row['Позиция']}"
                if row['Промо'] == 'Да':
                    pos += f" (промо: {row['Промо позиция']})"
                positions.append(pos)
            
            message_part += "\n".join(positions) + "\n\n"
            
            if len(current_message + message_part) > 4000:
                messages.append(current_message)
                current_message = message_part
            else:
                current_message += message_part
        
        if current_message:
            messages.append(current_message)
        
        for i, msg in enumerate(messages):
            await message.answer(msg, parse_mode="HTML")
            if i < len(messages) - 1:
                time.sleep(1)
        
        await message.answer("✅ Проверка завершена!")

    async def run(self):
        """Запуск бота"""
        await self.dp.start_polling(self.bot)

async def main():
    bot = TelegramBot()
    await bot.run()

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())