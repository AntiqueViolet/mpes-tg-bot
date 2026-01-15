import re
import asyncio
import os
import logging
import traceback
import contextlib
import sys
import html
from datetime import datetime, time, timedelta
import pytz  # Добавляем pytz для работы с часовыми поясами

from telethon import TelegramClient, events
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command

from dotenv import load_dotenv
import pymysql
from pymysql import Error as PyMysqlError

from telethon.sessions import StringSession

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# Добавляем глобальные переменные для кэша данных по кассам
cached_cashboxes_data = []  # Будет хранить данные, полученные в 00:00
cached_cashboxes_timestamp = None  # Время последнего обновления кэша
cashboxes_cache_lock = asyncio.Lock()  # Блокировка для безопасного доступа к кэшу

api_id = int(os.getenv("TG_API_ID"))
api_hash = os.getenv("TG_API_HASH")
bot_token = os.getenv("BOT_TOKEN")

ALLOWED_START_IDS = set(
    int(x.strip()) for x in os.getenv("ALLOWED_START_IDS").split(",") if x.strip()
)

bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

target_chat_id = int(os.getenv("OWNER_CHAT_ID"))
_owner_d = os.getenv("OWNER_CHAT_ID_D")
_owner_n = os.getenv("OWNER_CHAT_ID_N")
_owner_f = os.getenv("OWNER_CHAT_ID_FINDIR")
target_chat_id_D = int(_owner_d) if (_owner_d and _owner_d.isdigit()) else target_chat_id
target_chat_id_N = int(_owner_n) if (_owner_n and _owner_n.isdigit()) else target_chat_id
target_chat_id_F = int(_owner_f) if (_owner_f and _owner_f.isdigit()) else target_chat_id

session_str = os.getenv("TG_SESSION")
if not session_str:
    raise RuntimeError("Нет TG_SESSION. Сгенерируйте StringSession и положите в .env")
client = TelegramClient(StringSession(session_str), api_id, api_hash)

# Кэш последних данных для кнопки "Подробно счета"
parsed_data = []

# Текст последнего сводного сообщения для "Назад"
last_summary_text = ""


# ---------- Работа с БД ----------
def _db_params():
    return {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", 3366) or 3366),
        "database": os.getenv("DB_NAME"),
        "charset": 'utf8mb4',
    }


async def obrabotchik():
    """Возвращает сумму по кассам. Безопасно: при любой ошибке вернёт 0.0"""
    logging.debug("Запуск obrabotchik()")
    params = _db_params()
    conn = None
    try:
        if any(v in (None, "") for v in params.values()):
            logging.error("Параметры подключения к БД неполные")
            return 0.0

        conn = pymysql.connect(**params, cursorclass=pymysql.cursors.DictCursor)
        logging.debug("Успешное подключение к БД")

        with conn.cursor() as cur:
            logging.debug("Выполнение SQL-запроса для суммы кассы")
            cur.execute(
                """
                SELECT SUM(afoc.balance) AS total_balance
                FROM algon_finance_online_cashbox afoc
                WHERE afoc.type <> "disabled"
                """
            )
            row = cur.fetchone() or {}
            kassa = row.get("total_balance")
            return float(kassa or 0.0)
    except PyMysqlError as e:
        logging.error(f"Ошибка подключения/запроса к БД: {e}")
        logging.error(traceback.format_exc())
        return 0.0
    except Exception as e:
        logging.error(f"Ошибка в obrabotchik(): {e}")
        logging.error(traceback.format_exc())
        return 0.0
    finally:
        with contextlib.suppress(Exception):
            if conn:
                conn.close()


async def fetch_cashboxes_data():
    """Получает данные по кассам из БД для кэширования"""
    logging.info("Получение данных по кассам для кэша")
    params = _db_params()
    conn = None
    try:
        if any(v in (None, "") for v in params.values()):
            logging.error("Параметры подключения к БД неполные")
            return []

        conn = pymysql.connect(**params, cursorclass=pymysql.cursors.DictCursor)

        with conn.cursor() as cur:
            # Запрос 1: кассы по организациям
            cur.execute(
                """
                SELECT o.name, SUM(afoc.balance) as Kassa
                FROM algon_finance_online_cashbox afoc
                         INNER JOIN oto o ON o.id = afoc.oto_id
                WHERE afoc.`type` <> "disabled"
                  AND afoc.balance <> 0
                  AND afoc.oto_id IS NOT NULL
                GROUP BY o.name
                ORDER BY Kassa DESC
                """
            )
            rows_1 = cur.fetchall()

            # Запрос 2: кассы по рег. организациям
            cur.execute(
                """
                SELECT afoc.name, afoc.balance as Kassa
                FROM algon_finance_online_cashbox afoc
                WHERE (afoc.`type` = "reg" OR afoc.`type` = "manage_company")
                  AND afoc.balance <> 0
                ORDER BY Kassa DESC
                """
            )
            rows_2 = cur.fetchall()

        result = []
        for row in rows_1 + rows_2:
            name = (row.get("name") or "").strip()
            balance = float(row.get("Kassa") or 0.0)
            if balance != 0:
                result.append({
                    "name": name,
                    "balance": balance,
                    "type": "org" if row in rows_1 else "reg"
                })

        logging.info(f"Получено {len(result)} записей для кэша касс")
        return result
    except Exception as e:
        logging.error(f"Ошибка при получении данных по кассам: {e}")
        logging.error(traceback.format_exc())
        return []
    finally:
        with contextlib.suppress(Exception):
            if conn:
                conn.close()


async def update_cashboxes_cache():
    """Обновляет кэш данных по кассам"""
    global cached_cashboxes_data, cached_cashboxes_timestamp

    async with cashboxes_cache_lock:
        data = await fetch_cashboxes_data()
        cached_cashboxes_data = data
        cached_cashboxes_timestamp = datetime.now(pytz.timezone('Europe/Moscow'))
        logging.info(f"Кэш касс обновлен в {cached_cashboxes_timestamp.strftime('%H:%M:%S')}")


async def get_cashboxes_from_cache():
    """Возвращает данные по кассам из кэша с временем обновления"""
    async with cashboxes_cache_lock:
        return cached_cashboxes_data.copy(), cached_cashboxes_timestamp


async def scheduled_cache_update():
    """Фоновая задача для обновления кэша каждый день в 00:00 по Москве"""
    moscow_tz = pytz.timezone('Europe/Moscow')

    while True:
        try:
            now_moscow = datetime.now(moscow_tz)
            target_time = time(0, 0, 0)  # 00:00:00

            # Вычисляем время до следующего 00:00
            target_datetime = datetime.combine(now_moscow.date(), target_time)
            target_datetime = moscow_tz.localize(target_datetime)

            # Если уже прошло 00:00 сегодня, планируем на завтра
            if now_moscow >= target_datetime:
                target_datetime += timedelta(days=1)

            wait_seconds = (target_datetime - now_moscow).total_seconds()

            logging.info(f"Следующее обновление кэша касс в {target_datetime.strftime('%H:%M:%S %d.%m.%Y')}")
            await asyncio.sleep(wait_seconds)

            # Выполняем обновление
            await update_cashboxes_cache()

            # Ждем 1 секунду перед началом следующего цикла
            await asyncio.sleep(1)

        except Exception as e:
            logging.error(f"Ошибка в scheduled_cache_update: {e}")
            logging.error(traceback.format_exc())
            # В случае ошибки ждем 5 минут перед повторной попыткой
            await asyncio.sleep(300)


# ---------- Парсинг входящих сообщений ----------
def parse_financial_message(text):
    """Парсит строки вида: ^Название$12345.67$ и суммирует значения"""
    global parsed_data
    logging.debug("Запуск parse_financial_message()")
    logging.debug(f"Текст для парсинга: {text[:500]}")
    parsed_data = []
    total = 0.0
    pattern = re.compile(r"^\^(.+?)\$(\-?[\d\s.,]+)\$", re.MULTILINE)

    for match in pattern.finditer(text):
        name = match.group(1).strip()
        value = float(match.group(2).replace("\xa0", "").replace(" ", "").replace(",", "."))
        parsed_data.append((name, value))
        total += value

    parsed_data.sort(key=lambda x: x[1], reverse=True)
    logging.debug(f"Спарсенные данные: {parsed_data}")
    logging.debug(f"Общая сумма: {total}")
    return total


# ---------- Обработчики ----------
@client.on(events.NewMessage)
async def handler(event):
    """Принимаем новые сообщения, считаем, отправляем сводку с кнопками."""
    global last_summary_text
    text = event.text
    logging.debug(f"Новое сообщение в Telegram: {text[:500] if text else ''}")
    if not text or "^" not in text or "$" not in text:
        return

    try:
        total = parse_financial_message(text)
        total_str = f"{total:,.2f}".replace(",", " ").replace(".", ",")

        now = datetime.now().strftime("%d.%m.%Y")

        # Вместо obrabotchik() используем данные из кэша касс
        cached_data, cache_time = await get_cashboxes_from_cache()

        # Вычисляем общую сумму из кэшированных данных
        kassa = 0.0
        for item in cached_data:
            kassa += item["balance"]

        # Если кэш пустой (например, при первом запуске), используем старую функцию
        if kassa == 0.0:
            kassa = await obrabotchik()
            cache_time_str = "(данные из БД в реальном времени)"
        else:
            # Форматируем время кэширования
            time_str = cache_time.strftime("%H:%M %d.%m.%Y") if cache_time else "время неизвестно"
            cache_time_str = f""

        kassa_str = f"{kassa:,.2f}".replace(",", " ").replace(".", ",")
        itog_sum = total + kassa
        itog_str = f"{itog_sum:,.2f}".replace(",", " ").replace(".", ",")

        last_summary_text = (
            f"<b>\U0001F4C5 Баланс Экосмотр на {now}</b>\n\n"
            f"\U0001F4B3 <b>1. Р/с:</b> {total_str} ₽\n"
            f"\U0001F3E6 <b>2. Кассы Драйв:</b> {kassa_str} ₽ {cache_time_str}\n\n"
            f"\U0001F9FE <b>Итого:</b> {itog_str} ₽"
        )

        # Добавляем кнопку для показа данных из кэша
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\U0001F4E8 Подробно счета", callback_data="show_raw")],
            [InlineKeyboardButton(text="\U0001F4C8 Подробно кассы", callback_data="show_cached_cashboxes")]
        ])

        logging.debug("Отправка сообщения в Telegram (основной и дополнительный чат)")
        await bot.send_message(chat_id=target_chat_id, text=last_summary_text, reply_markup=keyboard)
        if target_chat_id_D != target_chat_id:
            await bot.send_message(chat_id=target_chat_id_D, text=last_summary_text, reply_markup=keyboard)

        if target_chat_id_N != target_chat_id:
            await bot.send_message(chat_id=target_chat_id_N, text=last_summary_text, reply_markup=keyboard)

        if target_chat_id_F != target_chat_id:
            await bot.send_message(chat_id=target_chat_id_F, text=last_summary_text, reply_markup=keyboard)
    except Exception as e:
        logging.error(f"Ошибка в handler: {e}")
        logging.error(traceback.format_exc())
        with contextlib.suppress(Exception):
            await bot.send_message(chat_id=target_chat_id, text=f"❌ Ошибка при обработке: {e}")


@dp.callback_query(lambda c: c.data == "show_details")
async def handle_callback(callback: CallbackQuery):
    """Кнопка: показать детализацию по кассам (берём из БД заново, чтобы не зависеть от кеша)."""
    logging.debug("Обработка callback: show_details")
    conn = None
    try:
        params = _db_params()
        if any(v in (None, "") for v in params.values()):
            await callback.answer("База недоступна после перезапуска. Отправьте новое сообщение для обновления.",
                                  show_alert=True)
            return

        conn = pymysql.connect(**params, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.name, SUM(afoc.balance) as Kassa
                FROM algon_finance_online_cashbox afoc
                         INNER JOIN oto o ON o.id = afoc.oto_id
                WHERE afoc.`type` <> "disabled"
                  AND afoc.balance <> 0
                  AND afoc.oto_id IS NOT NULL
                GROUP BY o.name
                ORDER BY Kassa DESC
                """
            )
            rows_1 = cur.fetchall()

            cur.execute(
                """
                SELECT afoc.name, afoc.balance as Kassa
                FROM algon_finance_online_cashbox afoc
                WHERE (afoc.`type` = "reg" OR afoc.`type` = "manage_company")
                  AND afoc.balance <> 0
                ORDER BY Kassa DESC
                """
            )
            rows_2 = cur.fetchall()

        if not rows_1 and not rows_2:
            await callback.answer("Нет данных для показа. Отправьте новое сообщение.", show_alert=True)
            return

        lines = []
        for row in rows_1 + rows_2:
            name = (row.get("name") or "").strip()
            balance = float(row.get("Kassa") or 0.0)
            balance_str = f"{balance:,.2f}".replace(",", " ").replace(".", ",")
            bullet = "▪️" if row in rows_1 else "▫️"
            lines.append(f"{bullet} {name}\n{balance_str} ₽\n")

        message = "\n".join(lines) or "Нет данных"

        keyboard_buttons = [
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
            [InlineKeyboardButton(text="\U0001F4C8 Подробно кассы", callback_data="show_cached_cashboxes")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await bot.edit_message_text(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            text=message,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback.answer()
    except TelegramBadRequest as e:
        logging.warning(f"TelegramBadRequest: {e}")
        await callback.answer("Сообщение устарело. Отправьте новое, и я покажу актуальные данные.", show_alert=True)
    except Exception as e:
        logging.error(f"Ошибка в handle_callback: {e}")
        logging.error(traceback.format_exc())
        with contextlib.suppress(Exception):
            await callback.answer(f"Ошибка: {e}", show_alert=True)
    finally:
        with contextlib.suppress(Exception):
            if conn:
                conn.close()


@dp.callback_query(lambda c: c.data == "show_cached_cashboxes")
async def handle_show_cached_cashboxes(callback: CallbackQuery):
    """Кнопка: показать данные по кассам из кэша (обновленного в 00:00)"""
    logging.debug("Обработка callback: show_cached_cashboxes")

    try:
        # Получаем данные из кэша
        cached_data, cache_time = await get_cashboxes_from_cache()

        if not cached_data:
            keyboard = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]]
            )

            await bot.edit_message_text(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                text="Данные по кассам на 00:00 еще не собраны или отсутствуют",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await callback.answer()
            return

        # Формируем сообщение с данными из кэша
        lines = []
        total = 0.0

        for item in cached_data:
            name = item["name"]
            balance = item["balance"]
            balance_str = f"{balance:,.2f}".replace(",", " ").replace(".", ",")
            bullet = "▪️" if item["type"] == "org" else "▫️"
            lines.append(f"{bullet} {name}\n{balance_str} ₽\n")
            total += balance

        # Добавляем итоговую сумму
        total_str = f"{total:,.2f}".replace(",", " ").replace(".", ",")

        # Формируем время обновления
        time_str = "время неизвестно"
        if cache_time:
            time_str = cache_time.strftime("%H:%M %d.%m.%Y")

        message = (
                f"<b>Данные по кассам (обновлено {time_str})</b>\n\n"
                + "\n".join(lines)
                + f"\n<b>Итого:</b> {total_str} ₽"
        )

        keyboard_buttons = [
            [InlineKeyboardButton(text="🔄 Актуальные кассы", callback_data="show_details")],
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        await bot.edit_message_text(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            text=message,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback.answer()
    except TelegramBadRequest as e:
        logging.warning(f"TelegramBadRequest: {e}")
        try:
            await callback.answer("Сообщение устарело. Отправьте новое сообщение.", show_alert=True)
        except Exception:
            pass
    except Exception as e:
        logging.error(f"Ошибка в handle_show_cached_cashboxes: {e}")
        logging.error(traceback.format_exc())
        with contextlib.suppress(Exception):
            await callback.answer(f"Ошибка: {e}", show_alert=True)


@dp.callback_query(lambda c: c.data == "show_raw")
async def handle_show_raw(callback: CallbackQuery):
    """Кнопка: показать сырые счета из последнего распарсенного сообщения (кеш).
    Если кеша нет (после рестарта), показываем текст "Данные устарели" прямо в сообщении.
    """
    logging.debug("Обработка callback: show_raw")
    global parsed_data
    try:
        keyboard_buttons = [
            [InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")],
            [InlineKeyboardButton(text="\U0001F4C8 Кассы", callback_data="show_cached_cashboxes")]
        ]
        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        if not parsed_data:
            await bot.edit_message_text(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                text="Данные устарели",
                reply_markup=keyboard,
                parse_mode="HTML"
            )
            await callback.answer()
            return

        lines = []
        for name, value in parsed_data:
            value_str = f"{value:,.2f}".replace(",", " ").replace(".", ",")
            lines.append(f"▫️ {html.escape(name)}\n{value_str} ₽\n")

        message = "\n".join(lines) or "Данные устарели"
        await bot.edit_message_text(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            text=message,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback.answer()
    except TelegramBadRequest as e:
        logging.warning(f"TelegramBadRequest: {e}")
        try:
            await callback.answer("Сообщение устарело. Отправьте новое сообщение.", show_alert=True)
        except Exception:
            pass
    except Exception as e:
        logging.error(f"Ошибка в handle_show_raw: {e}")
        logging.error(traceback.format_exc())
        with contextlib.suppress(Exception):
            await callback.answer(f"Ошибка: {e}", show_alert=True)


@dp.callback_query(lambda c: c.data == "back_to_main")
async def handle_back(callback: CallbackQuery):
    """Кнопка: вернуться к сводному сообщению. Если сводки нет (после рестарта) — показываем 'Данные устарели' вместо алерта."""
    logging.debug("Обработка callback: back_to_main")
    global last_summary_text
    try:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="\U0001F4E8 Подробно счета", callback_data="show_raw")],
                [InlineKeyboardButton(text="\U0001F4C8 Подробно кассы", callback_data="show_cached_cashboxes")]
            ]
        )

        text = last_summary_text if last_summary_text else "Данные устарели"
        await bot.edit_message_text(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            text=text,
            reply_markup=keyboard,
            parse_mode="HTML"
        )
        await callback.answer()
    except TelegramBadRequest as e:
        logging.warning(f"TelegramBadRequest: {e}")
        with contextlib.suppress(Exception):
            await callback.answer("Сообщение устарело. Отправьте новое.", show_alert=True)
    except Exception as e:
        logging.error(f"Ошибка в handle_back: {e}")
        logging.error(traceback.format_exc())
        with contextlib.suppress(Exception):
            await callback.answer(f"Ошибка: {e}", show_alert=True)


def _format_taxes_table(rows):
    """
    Возвращает список текстовых 'страниц' с таблицей в <pre>, чтобы не превышать ~3500-3800 символов.
    Колонки: Код региона | Получатель | Остаток
    """
    if not rows:
        return ["Данных не найдено."]

    headers = ["Код региона", "Получатель", "Остаток"]

    str_rows = []
    col_widths = [len(h) for h in headers]
    for r in rows:
        region_code = str(r.get("Код региона", "") or "")
        recipient = str(r.get("Получатель", "") or "")
        count = str(r.get("Остаток", "") or "0")

        row = [region_code, recipient, count]
        str_rows.append(row)
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def fmt_row(cells):
        return "  ".join(cell.ljust(col_widths[i]) for i, cell in enumerate(cells))

    header_line = fmt_row(headers)
    sep_line = "-" * len(header_line)

    pages = []
    current = ["<b>Актуальные гос.пошлины по регионам</b>", "<pre>", header_line, sep_line]
    current_len = sum(len(x) for x in current) + 50  # запас на теги

    for row in str_rows:
        line = fmt_row(row)
        if current_len + len(line) + 7 > 3500:
            current.append("</pre>")
            pages.append("\n".join(current))
            current = ["<b>Актуальные гос.пошлины по регионам (продолжение)</b>", "<pre>", header_line, sep_line]
            current_len = sum(len(x) for x in current) + 50

        current.append(line)
        current_len += len(line) + 1

    current.append("</pre>")
    pages.append("\n".join(current))
    return pages


@dp.message(Command("start"))
async def cmd_start(message):
    try:
        user_id = int(message.from_user.id)
    except Exception:
        user_id = None

    if user_id not in ALLOWED_START_IDS:
        return

    text = (
        'Привет! По кнопке "<b>Проверить пошлины</b>" можно посмотреть актуальное '
        'количество гос пошлин по регионам!'
    )

    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Проверить пошлины", callback_data="check_taxes:1")]  # ← Добавлено :1
    ])

    await bot.send_message(chat_id=message.chat.id, text=text, reply_markup=keyboard)


@dp.callback_query(lambda c: c.data.startswith("check_taxes"))
async def handle_check_taxes(callback: CallbackQuery):
    """Обработка кнопки 'Проверить пошлины' с постраничной навигацией"""
    conn = None

    # Извлекаем номер страницы из callback_data
    data_parts = callback.data.split(":")
    current_page = int(data_parts[1]) if len(data_parts) > 1 else 1

    try:
        params = _db_params()
        if any(v in (None, "") for v in params.values()):
            await callback.answer("База недоступна. Проверьте настройки подключения.", show_alert=True)
            return

        conn = pymysql.connect(**params, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute("""
                        SELECT dpr.id as 'Id', dpr.region_code as 'Код региона', dpr.recipient_name as 'Получатель', IF(COUNT(t.upno) = 0, 0, COUNT(t.upno)) as 'Остаток'
                        FROM duty_payment_requisites dpr
                                 LEFT JOIN webto_user_region_list wurl ON wurl.code = dpr.region_code
                                 LEFT JOIN tax t ON t.region_id = wurl.id AND t.active = 1
                        GROUP BY dpr.id, dpr.region_code, dpr.recipient_name
                        ORDER BY COUNT(t.upno) DESC, dpr.region_code
                        """)
            rows = cur.fetchall()

        # Разбиваем на страницы
        pages = _format_taxes_table(rows)
        total_pages = len(pages)

        if total_pages == 0:
            await bot.edit_message_text(
                chat_id=callback.message.chat.id,
                message_id=callback.message.message_id,
                text="Данных не найдено.",
                parse_mode="HTML"
            )
            await callback.answer()
            return

        # Ограничиваем номер страницы в допустимых пределах
        current_page = max(1, min(current_page, total_pages))

        # Создаем клавиатуру с навигацией
        keyboard_buttons = []

        # Кнопки навигации (предыдущая/следующая)
        nav_buttons = []
        if current_page > 1:
            nav_buttons.append(InlineKeyboardButton(
                text="⬅️ Назад",
                callback_data=f"check_taxes:{current_page - 1}"
            ))

        nav_buttons.append(InlineKeyboardButton(
            text=f"{current_page}/{total_pages}",
            callback_data="ignore"
        ))

        if current_page < total_pages:
            nav_buttons.append(InlineKeyboardButton(
                text="Вперёд ➡️",
                callback_data=f"check_taxes:{current_page + 1}"
            ))

        if nav_buttons:
            keyboard_buttons.append(nav_buttons)

        # Кнопка обновления
        keyboard_buttons.append([
            InlineKeyboardButton(
                text="🔄 Обновить",
                callback_data="check_taxes:1"
            )
        ])

        keyboard = InlineKeyboardMarkup(inline_keyboard=keyboard_buttons)

        # Отправляем/редактируем сообщение
        await bot.edit_message_text(
            chat_id=callback.message.chat.id,
            message_id=callback.message.message_id,
            text=pages[current_page - 1],
            reply_markup=keyboard,
            parse_mode="HTML"
        )

        await callback.answer()

    except TelegramBadRequest as e:
        if "message is not modified" in str(e):
            # Сообщение не изменилось, просто подтверждаем нажатие
            await callback.answer()
        else:
            logging.warning(f"TelegramBadRequest: {e}")
            try:
                await callback.answer("Сообщение устарело. Нажмите ещё раз «Проверить пошлины».", show_alert=True)
            except Exception:
                pass
    except Exception as e:
        logging.error(f"Ошибка в handle_check_taxes: {e}")
        logging.error(traceback.format_exc())
        with contextlib.suppress(Exception):
            await callback.answer(f"Ошибка: {e}", show_alert=True)
    finally:
        with contextlib.suppress(Exception):
            if conn:
                conn.close()


# Добавляем обработчик для игнорирования кнопки "ignore"
@dp.callback_query(lambda c: c.data == "ignore")
async def handle_ignore(callback: CallbackQuery):
    """Обработка кнопки, которая ничего не делает (например, номер страницы)"""
    await callback.answer()


@dp.error()
async def on_error(update, error):
    logging.error(f"Ошибка в обработчике: {error}\nUpdate: {update}")


async def main():
    logging.debug("Запуск main()")

    # Запускаем фоновую задачу для обновления кэша
    cache_task = asyncio.create_task(scheduled_cache_update())

    # Выполняем первоначальное обновление кэша
    logging.info("Выполняем первоначальное обновление кэша касс...")
    await update_cashboxes_cache()

    while True:
        try:
            await client.start()
            await asyncio.gather(
                dp.start_polling(bot, allowed_updates=["message", "callback_query"]),
                client.run_until_disconnected()
            )
        except Exception as e:
            logging.error(f"Главный цикл упал: {e}")
            logging.error(traceback.format_exc())
            await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Остановлено пользователем")