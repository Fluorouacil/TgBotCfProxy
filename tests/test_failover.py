"""Тесты failover-сценариев FailoverAiohttpSession с mock-сервером Bot API."""
from __future__ import annotations

import asyncio

import pytest
from aiogram import Bot
from aiogram.client.telegram import TelegramAPIServer
from aiogram.methods import GetMe

from tgbotcfproxy import FailoverAiohttpSession

from conftest import free_port

pytestmark = pytest.mark.asyncio


def make_session(**kwargs) -> FailoverAiohttpSession:
    """Сессия с отключённым автодеплоем и короткими таймаутами."""
    kwargs.setdefault("auto_fallback", False)
    kwargs.setdefault("request_timeout", 5)
    kwargs.setdefault("connect_timeout", 2)
    return FailoverAiohttpSession(**kwargs)


def set_primary(session: FailoverAiohttpSession, base: str) -> None:
    """Заменить primary-сервер (по умолчанию api.telegram.org)."""
    session._primary_server = TelegramAPIServer.from_base(base)
    session.api = session._primary_server


async def test_failover_to_fallback(mock_api_factory):
    """Primary недоступен -> запрос уходит через fallback."""
    fallback = await mock_api_factory()

    session = make_session(fallback_urls=[f"http://127.0.0.1:{fallback.port}"])
    set_primary(session, f"http://127.0.0.1:{free_port()}")

    bot = Bot(token="123456:TEST", session=session)
    user = await bot(GetMe())

    assert user.username == "test_bot"
    assert session._active_index == 0
    assert session.stats["active"] == "fallback[0]"
    assert session.stats["requests"]["fallback[0]"] == 1
    assert session.stats["switches"] == 1

    await session.close()


async def test_stays_on_primary_when_available(mock_api_factory):
    """Primary доступен -> fallback не используется."""
    primary = await mock_api_factory()
    fallback = await mock_api_factory()

    session = make_session(fallback_urls=[f"http://127.0.0.1:{fallback.port}"])
    set_primary(session, f"http://127.0.0.1:{primary.port}")

    bot = Bot(token="123456:TEST", session=session)
    user = await bot(GetMe())

    assert user.username == "test_bot"
    assert session._active_index == -1
    assert session.stats["requests"]["primary"] == 1
    assert session.stats["switches"] == 0

    await session.close()


async def test_recovery_back_to_primary(mock_api_factory):
    """Primary снова доступен -> фоновая recovery-проба возвращает на него."""
    primary = await mock_api_factory()
    primary_port = primary.port
    await primary.stop()  # порт освобождён -> connection refused
    fallback = await mock_api_factory()

    session = make_session(
        fallback_urls=[f"http://127.0.0.1:{fallback.port}"],
        recovery_check_interval=0.1,
    )
    set_primary(session, f"http://127.0.0.1:{primary_port}")

    bot = Bot(token="123456:TEST", session=session)

    # 1) primary мёртв -> переключаемся на fallback
    await bot(GetMe())
    assert session._active_index == 0

    # 2) primary ожил (новый сервер на том же порту) -> ждём recovery-пробу
    await mock_api_factory(port=primary_port)
    for _ in range(100):
        if session._active_index == -1:
            break
        await bot(GetMe())
        await asyncio.sleep(0.05)

    assert session._active_index == -1, "recovery не переключил на primary"
    # после переключения запрос снова идёт через primary
    user = await bot(GetMe())
    assert user.username == "test_bot"
    assert session.stats["requests"]["primary"] >= 1

    await session.close()


async def test_all_endpoints_down_raises(mock_api_factory):
    """Все эндпоинты недоступны -> сетевая ошибка пробрасывается наружу."""
    session = make_session(
        fallback_urls=[f"http://127.0.0.1:{free_port()}"])
    set_primary(session, f"http://127.0.0.1:{free_port()}")

    bot = Bot(token="123456:TEST", session=session)
    with pytest.raises(Exception) as exc_info:
        await bot(GetMe())
    # aiogram оборачивает сетевые ошибки в TelegramNetworkError
    assert "Network" in type(exc_info.value).__name__

    await session.close()


async def test_health_check_marks_dead_fallback(mock_api_factory):
    """Health-check помечает мёртвый fallback cooldown'ом."""
    session = make_session(
        fallback_urls=[f"http://127.0.0.1:{free_port()}"],
        health_check_interval=0.1,
        fallback_dead_cooldown=10,
    )
    set_primary(session, f"http://127.0.0.1:{free_port()}")

    bot = Bot(token="123456:TEST", session=session)
    # health-check стартует при первом make_request; ждём его завершения
    for _ in range(100):
        try:
            await bot(GetMe())
        except Exception:
            pass
        if session._fallback_dead_until:
            break
        await asyncio.sleep(0.05)

    assert 0 in session._fallback_dead_until
    assert session._is_fallback_dead(0)

    await session.close()


async def test_first_request_waits_for_auto_deploy(monkeypatch, mock_api_factory):
    """Primary недоступен, а автодеплой Worker'а ещё выполняется в фоне ->
    первый запрос ждёт завершения деплоя и уходит через fallback,
    а не падает с TelegramNetworkError (регрессия: бот падал на getMe)."""
    fallback = await mock_api_factory()

    async def fake_ensure_deployed(self, route_pattern=None):
        await asyncio.sleep(0.5)  # имитируем медленный деплой
        return f"http://127.0.0.1:{fallback.port}"

    monkeypatch.setattr(
        "tgbotcfproxy.session.CloudflareWorkerDeployer.ensure_deployed",
        fake_ensure_deployed,
    )

    session = FailoverAiohttpSession(
        auto_fallback=True,
        cf_api_token="test-token",
        cf_account_id="test-account",
        request_timeout=5,
        connect_timeout=2,
        deploy_wait_timeout=5,
    )
    set_primary(session, f"http://127.0.0.1:{free_port()}")

    bot = Bot(token="123456:TEST", session=session)
    user = await bot(GetMe())

    assert user.username == "test_bot"
    assert session._active_index == 0
    assert session.stats["active"] == "fallback[0]"
    assert session.stats["requests"]["fallback[0]"] == 1

    await session.close()


async def test_add_fallback_dedup_and_stats():
    """add_fallback не дублирует URL; stats/active_endpoint работают."""
    session = make_session(fallback_urls=["http://127.0.0.1:9999/"])
    session.add_fallback("http://127.0.0.1:9999")  # дубль (с/без слэша)
    assert len(session._fallback_servers) == 1
    assert session.fallback_urls == ["http://127.0.0.1:9999"]
    assert session.active_endpoint == "https://api.telegram.org"
    assert session.stats == {
        "requests": {},
        "switches": 0,
        "active": "primary",
    }
    await session.close()
