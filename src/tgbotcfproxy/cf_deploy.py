"""
TgBotCfProxy — CloudflareWorkerDeployer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Программно "поднимает" Cloudflare Worker, проксирующий запросы к
api.telegram.org, используя официальный Cloudflare REST API.
Не требует установки wrangler — только API-токен.

Как получить токен:
    Cloudflare Dashboard -> My Profile -> API Tokens -> Create Token
    Права: Account.Workers Scripts:Edit, Zone.Workers Routes:Edit,
           Zone.DNS:Edit (если понадобится создавать поддомен)

Важно про формат загрузки: Worker написан в синтаксисе ES Modules
(`export default {...}`), поэтому загружать его нужно через
multipart/form-data с частью metadata (main_module) и частью скрипта
с типом application/javascript+module — иначе Cloudflare пытается
распарсить его как старый Service Worker формат и падает с
"Uncaught SyntaxError: Unexpected token 'export'".

Документация: https://developers.cloudflare.com/api/operations/worker-script-upload-worker-module
Multipart metadata: https://developers.cloudflare.com/workers/configuration/multipart-upload-metadata/

Использование:

    deployer = CloudflareWorkerDeployer(
        api_token="...",
        account_id="...",
        zone_id="...",         # опционально, только если нужен route на своём домене
        script_name="tgbotcfproxy",
    )
    worker_url = await deployer.ensure_deployed(
        route_pattern="proxy.example.com/*",  # опционально
    )
    # worker_url -> "https://tgbotcfproxy.<subdomain>.workers.dev"
    #            или "https://proxy.example.com" если указали route_pattern
"""

from __future__ import annotations

import json
import logging
from typing import Optional

import aiohttp

logger = logging.getLogger("tgbotcfproxy.cf_deploy")

CF_API_BASE = "https://api.cloudflare.com/client/v4"

WORKER_ENTRYPOINT = "worker.js"

WORKER_SCRIPT = """
export default {
  async fetch(request) {
    const url = new URL(request.url);
    const targetUrl = "https://api.telegram.org" + url.pathname + url.search;

    const init = {
      method: request.method,
      headers: request.headers,
    };
    if (request.method !== "GET" && request.method !== "HEAD") {
      init.body = await request.arrayBuffer();
    }

    const resp = await fetch(targetUrl, init);
    const headers = new Headers(resp.headers);
    headers.set("Access-Control-Allow-Origin", "*");
    return new Response(resp.body, { status: resp.status, headers });
  },
};
""".strip()


class CloudflareAPIError(RuntimeError):
    """Ошибка запроса к Cloudflare API."""


class CloudflareWorkerDeployer:
    def __init__(
        self,
        *,
        api_token: str,
        account_id: str,
        zone_id: Optional[str] = None,
        script_name: str = "tgbotcfproxy",
        compatibility_date: str = "2024-09-23",
    ) -> None:
        self._token = api_token
        self._account_id = account_id
        self._zone_id = zone_id
        self._script_name = script_name
        self._compatibility_date = compatibility_date

    def _auth_header(self) -> dict:
        """Заголовок авторизации для запросов к Cloudflare API."""
        return {"Authorization": f"Bearer {self._token}"}

    async def _get_subdomain(self, session: aiohttp.ClientSession) -> str:
        """Получить workers.dev subdomain аккаунта."""
        url = f"{CF_API_BASE}/accounts/{self._account_id}/workers/subdomain"
        async with session.get(url, headers=self._auth_header()) as resp:
            data = await resp.json()
            if not data.get("success"):
                raise CloudflareAPIError(f"Не удалось получить workers.dev subdomain: {data}")
            return data["result"]["subdomain"]

    async def _script_exists(self, session: aiohttp.ClientSession) -> bool:
        """Проверить, существует ли Worker-скрипт с заданным именем.

        Cloudflare API возвращает success: false в JSON даже при 200
        (например, 403 с телом ошибки), поэтому проверяем оба поля.
        """
        url = f"{CF_API_BASE}/accounts/{self._account_id}/workers/scripts/{self._script_name}"
        async with session.get(url, headers=self._auth_header()) as resp:
            if resp.status != 200:
                return False
            data = await resp.json()
            return bool(data.get("success"))

    async def _upload_script(self, session: aiohttp.ClientSession) -> None:
        """
        Загружает Worker в формате ES Modules через multipart/form-data.

        Структура запроса (см. документацию Cloudflare):
        - часть "metadata": JSON с main_module (имя part'а со скриптом)
          и compatibility_date;
        - часть с именем файла из main_module: сам JS-код, Content-Type
          обязательно "application/javascript+module" — именно эта деталь
          говорит Cloudflare парсить файл как ES-модуль, а не как старый
          service-worker формат (в котором "export" считается синтаксической
          ошибкой).
        """
        url = f"{CF_API_BASE}/accounts/{self._account_id}/workers/scripts/{self._script_name}"

        metadata = {
            "main_module": WORKER_ENTRYPOINT,
            "compatibility_date": self._compatibility_date,
        }

        form = aiohttp.FormData()
        form.add_field(
            "metadata",
            json.dumps(metadata),
            content_type="application/json",
        )
        form.add_field(
            WORKER_ENTRYPOINT,
            WORKER_SCRIPT,
            filename=WORKER_ENTRYPOINT,
            content_type="application/javascript+module",
        )

        # ВАЖНО: Content-Type для multipart (с boundary) выставляет сам
        # aiohttp при сериализации FormData — вручную его задавать нельзя,
        # иначе boundary потеряется и Cloudflare не распарсит части.
        async with session.put(
            url, headers=self._auth_header(), data=form
        ) as resp:
            data = await resp.json()
            if not data.get("success", False):
                raise CloudflareAPIError(f"Ошибка загрузки Worker-скрипта: {data}")
        logger.info("Worker-скрипт '%s' успешно загружен (ES module)", self._script_name)

    async def _enable_workers_dev(self, session: aiohttp.ClientSession) -> None:
        """Включить публичный workers.dev subdomain для Worker'а.

        Если subdomain не включился — Worker задеплоен, но недоступен
        по workers.dev, поэтому ошибку логируем (не бросаем: деплой
        скрипта уже прошёл, повторный запуск попробует снова).
        """
        url = (
            f"{CF_API_BASE}/accounts/{self._account_id}/workers/scripts/"
            f"{self._script_name}/subdomain"
        )
        async with session.post(
            url,
            headers={**self._auth_header(), "Content-Type": "application/json"},
            json={"enabled": True},
        ) as resp:
            data = await resp.json()
            if not data.get("success", False):
                logger.warning(
                    "Не удалось включить workers.dev subdomain для '%s': %s "
                    "(Worker задеплоен, но может быть недоступен по "
                    "workers.dev)",
                    self._script_name, data,
                )

    async def _create_route(
        self, session: aiohttp.ClientSession, route_pattern: str
    ) -> None:
        """Создать Workers Route на своём домене (нужен zone_id)."""
        if not self._zone_id:
            raise ValueError("Для route_pattern нужен zone_id (домен должен быть на Cloudflare)")
        url = f"{CF_API_BASE}/zones/{self._zone_id}/workers/routes"
        async with session.post(
            url,
            headers={**self._auth_header(), "Content-Type": "application/json"},
            json={"pattern": route_pattern, "script": self._script_name},
        ) as resp:
            data = await resp.json()
            if not data.get("success", False):
                # маршрут может уже существовать — это не фатально
                logger.warning("Не удалось создать route (возможно уже существует): %s", data)

    async def ensure_deployed(
        self, route_pattern: Optional[str] = None
    ) -> str:
        """
        Гарантирует, что Worker задеплоен, и возвращает его публичный URL.
        Если Worker уже существует — просто переиспользует его (без
        повторной загрузки кода), если нет — создаёт с нуля.
        """
        async with aiohttp.ClientSession() as session:
            if not await self._script_exists(session):
                logger.info("Worker '%s' не найден, деплою заново", self._script_name)
                await self._upload_script(session)
                await self._enable_workers_dev(session)
            else:
                logger.info("Worker '%s' уже задеплоен, пропускаю загрузку", self._script_name)

            if route_pattern:
                await self._create_route(session, route_pattern)
                domain = route_pattern.split("/")[0]
                return f"https://{domain}"

            subdomain = await self._get_subdomain(session)
            return f"https://{self._script_name}.{subdomain}.workers.dev"
