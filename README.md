# 📊 Telegram Финансовый Бот (Telethon + Aiogram)

Бот автоматически обрабатывает финансовые сообщения в Telegram, суммирует значения и выводит итоговый баланс с учётом касс из базы данных. Поддерживает Telegram-кнопки для получения детальной информации.

---

## 🚀 Возможности

- Парсит сообщения с шаблоном: `^Название$12345.67$`
- Подсчитывает сумму всех значений
- Получает остатки по кассам из MySQL
- Отправляет итоговое сообщение владельцу в Telegram
- Поддерживает кнопки для получения:
  - подробного списка касс
  - разбивки по объектам

---

## 🧰 Технологии

- [Telethon](https://github.com/LonamiWebs/Telethon) — приём сообщений
- [Aiogram v3](https://github.com/aiogram/aiogram) — взаимодействие с Telegram API
- [PyMySQL](https://github.com/PyMySQL/PyMySQL) — подключение к MySQL
- [python-dotenv](https://github.com/theskumar/python-dotenv) — переменные окружения
- Docker + Docker Compose + GitHub Actions — деплой и публикация

---

## 📦 Установка и запуск

### 🔹 1. Клонировать репозиторий

```bash
git clone https://github.com/yourrepo/telegram-finance-bot.git
cd telegram-finance-bot
```

### 🔹 2. Создать `.env` файл

```dotenv
TG_API_ID=...
TG_API_HASH=...
BOT_TOKEN=...
OWNER_CHAT_ID=...

DB_USER=...
DB_PASS=...
DB_HOST=...
DB_PORT=3366
DB_NAME=...
```

> `.env` не должен попадать в репозиторий!

---

## 🐳 Запуск в Docker

```bash
docker compose up --build
```

---

## 🧪 Локальный запуск

```bash
pip install -r req.txt
python worker.py
```

---

## ☁️ CI/CD через GitHub Actions

Проект автоматически:

1. Восстанавливает `.env` из GitHub Secrets
2. Восстанавливает `anon.session` из base64
3. Публикует Docker-образ в Docker Hub

### Необходимые GitHub Secrets:

| Название              | Описание                          |
|-----------------------|-----------------------------------|
| `TG_API_ID`           | Telegram API ID                   |
| `TG_API_HASH`         | Telegram API Hash                 |
| `BOT_TOKEN`           | Токен Telegram-бота              |
| `OWNER_CHAT_ID`       | ID чата владельца                 |
| `DB_USER`             | MySQL логин                       |
| `DB_PASS`             | MySQL пароль                      |
| `DB_HOST`             | Хост БД                           |
| `DB_PORT`             | Порт БД                           |
| `DB_NAME`             | Имя БД                            |
| `ANON_SESSION_B64`    | anon.session, закодированный base64 |
| `DOCKER_USERNAME`     | Логин Docker Hub                  |
| `DOCKER_PASSWORD`     | Токен Docker Hub                  |

---

## 🔐 Как получить `ANON_SESSION_B64` (Windows):

```powershell
[Convert]::ToBase64String([IO.File]::ReadAllBytes("anon.session")) > anon.session.b64
```

Добавь содержимое файла `anon.session.b64` в GitHub Secrets.

---

## 🖼 Пример вывода

```
📅 Баланс Экосмотр на 01.08.2025

💳 1. Р/с: 450 000,00 ₽
🏦 2. Кассы Драйв: 720 450,00 ₽

🧾 Итого: 1 170 450,00 ₽
```

Кнопки:
- 📋 Подробно кассы
- 📨 Подробно счета

---

## 📂 Структура проекта

```
.
├── worker.py
├── Dockerfile
├── docker-compose.yml
├── docker-publish.yml
├── req.txt
└── .env.example
```

---

## 📄 Лицензия

MIT — свободно используйте и адаптируйте под свои задачи.