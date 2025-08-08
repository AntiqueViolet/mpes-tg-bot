import re
import asyncio
import os
import logging
import traceback
import contextlib
import sys
import html
from datetime import datetime

from telethon import TelegramClient, events
from aiogram import Bot, Dispatcher
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram.exceptions import TelegramBadRequest

from dotenv import load_dotenv
import pymysql
from pymysql import Error as PyMysqlError

load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

api_id = int(os.getenv("TG_API_ID"))
api_hash = os.getenv("TG_API_HASH")
bot_token = os.getenv("BOT_TOKEN")

bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

target_chat_id = int(os.getenv("OWNER_CHAT_ID"))
_owner_d = os.getenv("OWNER_CHAT_ID_D")
target_chat_id_D = int(_owner_d) if (_owner_d and _owner_d.isdigit()) else target_chat_id

client = TelegramClient('anon', api_id, api_hash)

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
        kassa = await obrabotchik()
        kassa_str = f"{kassa:,.2f}".replace(",", " ").replace(".", ",")
        itog_sum = total + kassa
        itog_str = f"{itog_sum:,.2f}".replace(",", " ").replace(".", ",")

        last_summary_text = (
            f"<b>\U0001F4C5 Баланс Экосмотр на {now}</b>\n\n"
            f"\U0001F4B3 <b>1. Р/с:</b> {total_str} ₽\n"
            f"\U0001F3E6 <b>2. Кассы Драйв:</b> {kassa_str} ₽\n\n"
            f"\U0001F9FE <b>Итого:</b> {itog_str} ₽"
        )

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="\U0001F4CB Подробно кассы", callback_data="show_details")],
            [InlineKeyboardButton(text="\U0001F4E8 Подробно счета", callback_data="show_raw")]
        ])

        logging.debug("Отправка сообщения в Telegram (основной и дополнительный чат)")
        await bot.send_message(chat_id=target_chat_id, text=last_summary_text, reply_markup=keyboard)
        if target_chat_id_D != target_chat_id:
            await bot.send_message(chat_id=target_chat_id_D, text=last_summary_text, reply_markup=keyboard)

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
            await callback.answer("База недоступна после перезапуска. Отправьте новое сообщение для обновления.", show_alert=True)
            return

        conn = pymysql.connect(**params, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT o.name, SUM(afoc.balance) as Kassa
                FROM algon_finance_online_cashbox afoc 
                INNER JOIN oto o ON o.id = afoc.oto_id 
                WHERE afoc.`type` <> "disabled" AND afoc.balance <> 0 AND afoc.oto_id IS NOT NULL
                GROUP BY o.name
                ORDER BY Kassa DESC
                """
            )
            rows_1 = cur.fetchall()

            cur.execute(
                """
                SELECT afoc.name, afoc.balance as Kassa
                FROM algon_finance_online_cashbox afoc
                WHERE (afoc.`type` = "reg" OR afoc.`type` = "manage_company") AND afoc.balance <> 0 
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
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]]
        )

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


@dp.callback_query(lambda c: c.data == "show_raw")
async def handle_show_raw(callback: CallbackQuery):
    """Кнопка: показать сырые счета из последнего распарсенного сообщения (кеш).
    Если кеша нет (после рестарта), показываем текст "Данные устарели" прямо в сообщении.
    """
    logging.debug("Обработка callback: show_raw")
    global parsed_data
    try:
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="back_to_main")]]
        )

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
                [InlineKeyboardButton(text="\U0001F4CB Подробно кассы", callback_data="show_details")],
                [InlineKeyboardButton(text="\U0001F4E8 Подробно счета", callback_data="show_raw")]
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


@dp.error()
async def on_error(update, error):
    logging.error(f"Ошибка в обработчике: {error}\nUpdate: {update}")


async def main():
    logging.debug("Запуск main()")
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
