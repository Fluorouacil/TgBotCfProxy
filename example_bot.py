"""
Пример: бот на aiogram 3.x с автоматическим fallback на Cloudflare Worker.

Никакого ручного деплоя не требуется — FailoverAiohttpSession сама
поднимет дефолтный Worker при первом запросе, если заданы
CF_API_TOKEN / CF_ACCOUNT_ID. Дополнительные резервные адреса (например,
свой VPS с WS-туннелем) можно дописать через extra_fallback_urls
или session.add_fallback(...).

Переменные окружения:
    BOT_TOKEN      - токен бота
    CF_API_TOKEN   - Cloudflare API token
    CF_ACCOUNT_ID  - Cloudflare Account ID
    CF_ZONE_ID     - (опц.) Zone ID, если хотите свой домен вместо workers.dev
    CF_ROUTE_PATTERN - (опц.) напр. "proxy.example.com/*"
"""

import asyncio
import logging
import os

from aiogram import Bot, Dispatcher
from aiogram.filters import CommandStart
from aiogram.types import Message

from tgbotcfproxy import FailoverAiohttpSession
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)


async def main() -> None:
    load_dotenv()
    bot_token = os.environ["BOT_TOKEN"]
    cf_api_token = os.environ["CF_API_TOKEN"]
    cf_account_id = os.environ["CF_ACCOUNT_ID"]

    session = FailoverAiohttpSession(
        cf_account_id=cf_account_id,
        cf_api_token=cf_api_token,
        extra_fallback_urls=[
            # сюда можно дописать свой VPS/WS-прокси как доп. резерв
            # "https://my-vps-proxy.example.com",
        ],
        request_timeout=20.0,
        recovery_check_interval=60.0,
    )

    # Резервный адрес можно добавить и в рантайме, в любой момент:
    # session.add_fallback("https://backup2.example.com")

    bot = Bot(token=bot_token, session=session)
    dp = Dispatcher()

    @dp.message(CommandStart())
    async def start_handler(message: Message) -> None:
        await message.answer(
            "Бот работает, при необходимости прокси переключается автоматически."
        )

    try:
        await dp.start_polling(bot)
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
