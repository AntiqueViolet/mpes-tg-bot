import pymysql
from pymysql import Error as PyMysqlError
import sys
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram import F
import asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram.utils.keyboard import ReplyKeyboardBuilder
from aiogram.types import BotCommand, BotCommandScopeDefault
from datetime import datetime, timedelta
import os
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

async def main():
    logger.info("Начался процесс обработки сообщения!")
    db_params = {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASS"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "database": os.getenv("DB_NAME"),
            "charset": 'utf8mb4',
        }
    if None in db_params.values():
        logger.error("Не заданы обязательные параметры подключения!")
        sys.exit(1)
    
    try:
        conn = pymysql.connect(**db_params, cursorclass=pymysql.cursors.DictCursor)
    except PyMysqlError as e:
        logger.error(f"Ошибка подключения к базе данных: {e}")
        sys.exit(1)

    bot = Bot(token=os.getenv("BOT_TOKEN"))
    dp = Dispatcher()

    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT 
                    SUM(afoc.balance)
                FROM algon_finance_online_cashbox afoc 
                WHERE afoc.type <> "disabled"
            """)
            
            db_data = cur.fetchone()
            logger.info(f"Получено {len(db_data)} записей из БД")
            
            if not db_data:
                logger.error("Данные не получены из БД")
                return
            now_naked = datetime.now()
            now =    now_naked.strftime("%Y-%m-%d")
            kassa = int(db_data['SUM(afoc.balance)'])
            text_s = f'''
            Баланс Экосмотр {now}\n
            1. Р/с - \n
            2. Эквайринг - \n
            3. Кассы Драйв - {kassa}\n
            Итого: {kassa}
            '''

            await bot.send_message(chat_id=366532391, text=text_s)

    except Exception as e:
        logger.exception(f"Критическая ошибка: {str(e)}")
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    asyncio.run(main())