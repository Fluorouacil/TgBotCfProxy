"""Общие фикстуры: mock-сервер Telegram Bot API на aiohttp."""
from __future__ import annotations

import json
import socket

import pytest
from aiohttp import web


def free_port() -> int:
    """Свободный порт (никто на нём не слушает) — для имитации
    недоступного эндпоинта."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

GET_ME_RESULT = {
    "id": 123456,
    "is_bot": True,
    "first_name": "Test",
    "username": "test_bot",
}


class MockBotApi:
    """Локальный сервер, имитирующий Telegram Bot API.

    - POST /bot{token}/{method} -> {"ok": true, "result": ...}
    - любой GET (включая "/") -> 404 (для health-check/recovery-проб)
    - enabled=False -> 503 (сервер жив, но "отказывает")
    """

    def __init__(self) -> None:
        self.enabled = True
        self.app = web.Application()
        self.app.router.add_route("*", "/{tail:.*}", self._handle)
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self.port: int | None = None

    async def _handle(self, request: web.Request) -> web.Response:
        if not self.enabled:
            return web.Response(status=503, text="unavailable")
        if request.method == "POST":
            return web.json_response({"ok": True, "result": GET_ME_RESULT})
        return web.Response(status=404, text="Not Found")

    async def start(self, port: int | None = None) -> int:
        # ВАЖНО: port=0 -> ОС выдаёт случайный свободный порт.
        # port=None в aiohttp TCPSite означает 8080 — нельзя!
        self._runner = web.AppRunner(self.app)
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, "127.0.0.1", port or 0)
        await self._site.start()
        # фактический порт берём из socket'а (для port=0 он известен только
        # после bind)
        sockets = self._runner.addresses
        self.port = sockets[0][1] if sockets else (port or 0)
        return self.port

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
            self._site = None


@pytest.fixture
async def mock_api_factory():
    """Фабрика mock-серверов Bot API; все созданные серверы
    закрываются по окончании теста."""
    servers: list[MockBotApi] = []

    async def create(port: int | None = None) -> MockBotApi:
        server = MockBotApi()
        await server.start(port)
        servers.append(server)
        return server

    yield create

    for server in servers:
        await server.stop()


@pytest.fixture
def get_me_result() -> dict:
    return json.loads(json.dumps(GET_ME_RESULT))
