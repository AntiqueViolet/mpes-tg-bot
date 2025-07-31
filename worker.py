import re
import asyncio
import os
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

api_id = int(os.getenv("TG_API_ID"))
api_hash = os.getenv("TG_API_HASH")

bot_token = os.getenv("BOT_TOKEN")
bot = Bot(token=bot_token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()
target_chat_id = int(os.getenv("OWNER_CHAT_ID"))

client = TelegramClient('anon', api_id, api_hash)

parsed_data = []


async def obrabotchik():
    db_params = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", 3366)),
        "database": os.getenv("DB_NAME"),
        "charset": 'utf8mb4',
    }
    if None in db_params.values():
        sys.exit(1)

    try:
        conn = pymysql.connect(**db_params, cursorclass=pymysql.cursors.DictCursor)
    except PyMysqlError as e:
        sys.exit(1)

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    SUM(afoc.balance)
                FROM algon_finance_online_cashbox afoc 
                WHERE afoc.type <> "disabled"
            """)

            db_data = cur.fetchone()

            if not db_data:
                return
            now_naked = datetime.now()
            now = now_naked.strftime("%Y-%m-%d")
            kassa = float(db_data['SUM(afoc.balance)'])
            return float(kassa)
    except Exception as e:
        raise
    finally:
        conn.close()

def parse_financial_message(text):
    global parsed_data
    parsed_data = []

    total = 0.0
    pattern = re.compile(r"^\^(.+?)\$(\-?[\d\s.,]+)\$$", re.MULTILINE)

    for match in pattern.finditer(text):
        name = match.group(1).strip()
        value = float(match.group(2).replace(" ", "").replace(",", "."))
        parsed_data.append((name, value))
        total += value

    parsed_data.sort(key=lambda x: x[1], reverse=True)
    return total

@client.on(events.NewMessage)
async def handler(event):
    text = event.text
    if not text or "^" not in text or "$" not in text:
        return

    try:
        total = parse_financial_message(text)
        total_str = f"{total:,.2f}".replace(",", " ").replace(".", ",")

        now_naked = datetime.now()
        now = now_naked.strftime("%d.%m.%Y")

        kassa = await obrabotchik()
        kassa_str = f"{kassa:,.2f}".replace(",", " ").replace(".", ",")
        itog_sum = total + kassa
        itog_str = f"{itog_sum:,.2f}".replace(",", " ").replace(".", ",")

        text_s = (
            f"<b>📅 Баланс Экосмотр на {now}</b>\n\n"
            f"💳 <b>1. Р/с:</b> {total_str} ₽\n"
            f"🏦 <b>2. Кассы Драйв:</b> {kassa_str} ₽\n\n"
            f"🧾 <b>Итого:</b> {itog_str} ₽"
        )

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="📋 Подробный список", callback_data="show_details")]
            ]
        )

        await bot.send_message(chat_id=target_chat_id, text=text_s, reply_markup=keyboard)

    except Exception as e:
        await bot.send_message(chat_id=target_chat_id, text=f"❌ Ошибка при обработке: {e}")

@dp.callback_query(lambda c: c.data == "show_details")
async def handle_callback(callback: CallbackQuery):
    db_params = {
        "user": os.getenv("DB_USER"),
        "password": os.getenv("DB_PASS"),
        "host": os.getenv("DB_HOST"),
        "port": int(os.getenv("DB_PORT", 3366)),
        "database": os.getenv("DB_NAME"),
        "charset": 'utf8mb4',
    }

    try:
        conn = pymysql.connect(**db_params, cursorclass=pymysql.cursors.DictCursor)
        with conn.cursor() as cur:
            cur.execute("""
            SELECT
                o.name,
                SUM(afoc.balance) as Kassa
            FROM algon_finance_online_cashbox afoc 
            inner join oto o ON o.id = afoc.oto_id 
            WHERE afoc.`type` <> "disabled" and afoc.balance <> 0 and afoc.oto_id is not null
            group by o.name
            ORDER by Kassa desc
            """)
            rows_1 = cur.fetchall()

            cur.execute("""
            SELECT
                afoc.name,
                afoc.balance as Kassa
            FROM algon_finance_online_cashbox afoc  
            WHERE (afoc.`type` = "reg" or afoc.`type` = "manage_company") and afoc.balance <> 0 
            ORDER by Kassa desc
            """)
            rows_2 = cur.fetchall()

        lines = []
        for row in rows_1:
            name = row["name"].strip()
            balance = float(row["Kassa"])
            balance_str = f"{balance:,.2f}".replace(",", " ").replace(".", ",")
            line = f"▪️ {name:<35}{balance_str:>15} ₽"
            lines.append(f"{line}")

        for row in rows_2:
            name = row["name"].strip()
            balance = float(row["Kassa"])
            balance_str = f"{balance:,.2f}".replace(",", " ").replace(".", ",")
            line = f"▫️ {name:<35}{balance_str:>15} ₽"
            lines.append(f"{line}")

        message = "\n".join(lines)
        chunks = [message[i:i+4000] for i in range(0, len(message), 4000)]

        for chunk in chunks:
            await bot.send_message(chat_id=callback.from_user.id, text=chunk, parse_mode="HTML")

        await callback.answer()
    except Exception as e:
        print(e)
        await callback.answer(f"Ошибка при получении данных {e}")

async def main():
    await client.start()
    await asyncio.gather(
        dp.start_polling(bot),
        client.run_until_disconnected()
    )

if __name__ == "__main__":
    asyncio.run(main())
