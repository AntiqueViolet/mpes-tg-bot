import re
import asyncio
import os
import logging
import traceback
from telethon import TelegramClient, events
from aiogram import Bot
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from dotenv import load_dotenv
from datetime import datetime
import pymysql
import sys
import html
from pymysql import Error as PyMysqlError
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery
from aiogram import Dispatcher

load_dotenv()

logging.basicConfig(
    filename='/var/log/worker.log',
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
)

api_id = int(os.getenv("TG_API_ID"))
api_hash = os.getenv("TG_API_HASH")

bot_token = os.getenv("BOT_TOKEN")
bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
target_chat_id = int(os.getenv("OWNER_CHAT_ID"))
target_chat_id_D = int(os.getenv("OWNER_CHAT_ID_D"))

client = TelegramClient('anon', api_id, api_hash)

parsed_data = []
last_summary_text = ""

async def obrabotchik():
    logging.debug("Запуск obrabotchik()")
    db_params = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", 3366)),
        "database": os.getenv("DB_NAME"),
        "charset": 'utf8mb4',
    }
    if None in db_params.values():
        logging.error("Параметры подключения к БД неполные")

    try:
        conn = pymysql.connect(**db_params, cursorclass=pymysql.cursors.DictCursor)
        logging.debug("Успешное подключение к БД")
    except PyMysqlError as e:
        logging.error(f"Ошибка подключения к БД: {e}")

    try:
        with conn.cursor() as cur:
            logging.debug("Выполнение SQL-запроса для суммы кассы")
            cur.execute("""
                SELECT 
                    SUM(afoc.balance)
                FROM algon_finance_online_cashbox afoc 
                WHERE afoc.type <> "disabled"
            """)

            db_data = cur.fetchone()
            logging.debug(f"Результат из БД: {db_data}")
            if not db_data:
                return
            kassa = float(db_data['SUM(afoc.balance)'])
            return float(kassa)
    except Exception as e:
        logging.error(f"Ошибка в obrabotchik: {e}")
        logging.error(traceback.format_exc())
        raise
    finally:
        conn.close()


def parse_financial_message(text):
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


@client.on(events.NewMessage)
async def handler(event):
    global last_summary_text

    text = event.text
    logging.debug(f"Новое сообщение в Telegram: {text[:500]}")
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

        logging.debug("Отправка сообщения в Telegram")
        await bot.send_message(chat_id=target_chat_id, text=last_summary_text, reply_markup=keyboard)
        await bot.send_message(chat_id=target_chat_id_D, text=last_summary_text, reply_markup=keyboard)

    except Exception as e:
        logging.error(f"Ошибка в handler: {e}")
        logging.error(traceback.format_exc())
        await bot.send_message(chat_id=target_chat_id, text=f"❌ Ошибка при обработке: {e}")


@dp.callback_query(lambda c: c.data == "show_details")
async def handle_callback(callback: CallbackQuery):
    logging.debug("Обработка callback: show_details")
    try:
        db_params = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASS"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", 3366)),
            "database": os.getenv("DB_NAME"),
            "charset": 'utf8mb4',
        }
        conn = pymysql.connect(**db_params, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT o.name, SUM(afoc.balance) as Kassa
                FROM algon_finance_online_cashbox afoc 
                INNER JOIN oto o ON o.id = afoc.oto_id 
                WHERE afoc.`type` <> "disabled" AND afoc.balance <> 0 AND afoc.oto_id IS NOT NULL
                GROUP BY o.name
                ORDER BY Kassa DESC
            """)
            rows_1 = cur.fetchall()

            cur.execute("""
                SELECT afoc.name, afoc.balance as Kassa
                FROM algon_finance_online_cashbox afoc
                WHERE (afoc.`type` = "reg" OR afoc.`type` = "manage_company") AND afoc.balance <> 0 
                ORDER BY Kassa DESC
            """)
            rows_2 = cur.fetchall()

        lines = []
        for row in rows_1 + rows_2:
            name = row["name"].strip()
            balance = float(row["Kassa"])
            balance_str = f"{balance:,.2f}".replace(",", " ").replace(".", ",")
            bullet = "▪️" if row in rows_1 else "▫️"
            lines.append(f"{bullet} {name}\n{balance_str} ₽\n")

        message = "\n".join(lines)
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
    except Exception as e:
        logging.error(f"Ошибка в handle_callback: {e}")
        logging.error(traceback.format_exc())
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@dp.callback_query(lambda c: c.data == "show_raw")
async def handle_show_raw(callback: CallbackQuery):
    logging.debug("Обработка callback: show_raw")
    global parsed_data
    try:
        if not parsed_data:
            await callback.answer("Нет данных", show_alert=True)
            return

        lines = []
        for name, value in parsed_data:
            value_str = f"{value:,.2f}".replace(",", " ").replace(".", ",")
            lines.append(f"▫️ {html.escape(name)}\n{value_str} ₽\n")

        message = "\n".join(lines)
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
    except Exception as e:
        logging.error(f"Ошибка в handle_show_raw: {e}")
        logging.error(traceback.format_exc())
        await callback.answer(f"Ошибка: {e}", show_alert=True)


@dp.callback_query(lambda c: c.data == "back_to_main")
async def handle_back(callback: CallbackQuery):
    logging.debug("Обработка callback: back_to_main")
    global last_summary_text
    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="\U0001F4CB Подробно кассы", callback_data="show_details")],
            [InlineKeyboardButton(text="\U0001F4E8 Подробно счета", callback_data="show_raw")]
        ]
    )
    await bot.edit_message_text(
        chat_id=callback.message.chat.id,
        message_id=callback.message.message_id,
        text=last_summary_text,
        reply_markup=keyboard,
        parse_mode="HTML"
    )
    await callback.answer()


async def main():
    logging.debug("Запуск main()")
    await client.start()
    await asyncio.gather(
        dp.start_polling(bot),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
