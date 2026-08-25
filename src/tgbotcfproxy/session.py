"""
TgBotCfProxy — FailoverAiohttpSession
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Дроп-ин замена aiogram.client.session.aiohttp.AiohttpSession:

1. По умолчанию работает как обычно (прямое соединение с api.telegram.org).
2. При сетевых сбоях (TLS handshake fail, timeout, connection refused,
   DNS-блокировка и т.п.) автоматически переключается на список резервных
   эндпоинтов (например, Cloudflare Worker, который проксирует запросы
   к api.telegram.org).
3. Если ни одного fallback-URL не передано явно — при первом запросе
   САМА разворачивает базовый Cloudflare Worker (через официальный
   Cloudflare API, в фоне, не блокируя запросы) и использует его как
   fallback по умолчанию. Для этого достаточно задать CF_API_TOKEN /
   CF_ACCOUNT_ID (через переменные окружения или аргументы конструктора).
4. В любой момент можно дописать дополнительные fallback-URL — как через
   конструктор (extra_fallback_urls), так и в рантайме через add_fallback().
5. Периодически в фоне:
   - пробует вернуться на прямое подключение (резервный канал обычно
     медленнее/дороже, используем его только пока основной недоступен);
   - проверяет живость каждого fallback'а (мёртвые временно пропускаются).
6. Ведёт статистику: сколько запросов ушло через каждый эндпоинт,
   сколько было переключений (property `stats`).

Простейшее использование (fallback Worker поднимется автоматически):

    import os
    from aiogram import Bot
    from tgbotcfproxy import FailoverAiohttpSession

    os.environ["CF_API_TOKEN"] = "..."
    os.environ["CF_ACCOUNT_ID"] = "..."

    session = FailoverAiohttpSession()          # fallback создастся сам
    bot = Bot(token="123:ABC", session=session)

С ручными дополнительными эндпоинтами:

    session = FailoverAiohttpSession(
        extra_fallback_urls=["https://my-vps-proxy.example.com"],
    )
    session.add_fallback("https://another-backup.example.com")
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from typing import TYPE_CHECKING, Optional, Sequence
from urllib.parse import urlparse

import aiohttp
from aiohttp import ClientConnectorError, ClientError, ServerTimeoutError
from aiogram.client.session.aiohttp import AiohttpSession
from aiogram.client.telegram import TelegramAPIServer
from aiogram.exceptions import ClientDecodeError, TelegramNetworkError

from .cf_deploy import CloudflareWorkerDeployer

if TYPE_CHECKING:
    from aiogram import Bot
    from aiogram.methods import TelegramMethod
    from aiogram.methods.base import TelegramType

logger = logging.getLogger("tgbotcfproxy")

# Исключения, при которых имеет смысл переключаться на резервный эндпоинт.
# Именно они обычно всплывают при DPI-блокировках: обрыв TLS handshake,
# RST-пакеты, таймауты, недоступность хоста.
#
# ВАЖНО: aiogram оборачивает ВСЕ сетевые ошибки (timeout, ClientError и т.д.)
# в TelegramNetworkError (см. AiohttpSession.make_request), поэтому ловить
# "сырые" aiohttp-исключения бесполезно — их до нас не доносят. Ловим
# TelegramNetworkError (и ClientDecodeError — ответ мог быть обрезан
# DPI-фильтром посреди тела).
_FAILOVER_EXCEPTIONS = (
    TelegramNetworkError,
    ClientDecodeError,
    ClientConnectorError,
    ServerTimeoutError,
    ConnectionError,
    OSError,
    asyncio.TimeoutError,
    ClientError,
)


class FailoverAiohttpSession(AiohttpSession):
    def __init__(
        self,
        *,
        fallback_urls: Optional[Sequence[str]] = None,
        extra_fallback_urls: Optional[Sequence[str]] = None,
        auto_fallback: bool = True,
        cf_api_token: Optional[str] = None,
        cf_account_id: Optional[str] = None,
        cf_zone_id: Optional[str] = None,
        cf_route_pattern: Optional[str] = None,
        cf_script_name: str = "tgbotcfproxy",
        request_timeout: float = 10.0,
        connect_timeout: float = 5.0,
        deploy_wait_timeout: float = 30.0,
        recovery_check_interval: float = 60.0,
        health_check_interval: float = 30.0,
        fallback_dead_cooldown: float = 30.0,
        max_attempts_per_request: Optional[int] = None,
        **kwargs,
    ) -> None:
        """
        :param fallback_urls: явный список готовых fallback-URL. Если задан —
            автодеплой Worker'а не запускается (считаем, что вы сами всё
            подготовили).
        :param extra_fallback_urls: дополнительные fallback-URL, которые
            добавляются ПОВЕРХ автоматически созданного Worker'а (если
            auto_fallback=True и fallback_urls не задан).
        :param auto_fallback: если True и fallback_urls не передан — при
            первом запросе в фоне разворачивается дефолтный Cloudflare
            Worker и используется как базовый fallback.
        :param cf_api_token / cf_account_id / cf_zone_id / cf_route_pattern:
            параметры для автодеплоя Worker'а. Если не переданы — берутся
            из переменных окружения CF_API_TOKEN / CF_ACCOUNT_ID /
            CF_ZONE_ID / CF_ROUTE_PATTERN.
        :param cf_script_name: имя Worker-скрипта в Cloudflare.
        :param request_timeout: таймаут одной попытки запроса (total), сек.
        :param connect_timeout: таймаут УСТАНОВКИ соединения (TCP+TLS), сек.
            Короткий, чтобы при блокировке primary быстро переключиться на
            fallback, не дожидаясь total-таймаута (важно для long-polling,
            где aiogram передаёт request_timeout = session.timeout + 30).
        :param deploy_wait_timeout: сколько (сек) максимум ждать завершения
            фоновой автодеплоя Worker'а, если primary упал, а fallback ещё
            не создан. Если деплой не успел — запрос падает с ошибкой,
            повторный запрос попробует снова (деплой к тому моменту обычно
            уже завершён).
        :param recovery_check_interval: как часто (сек) пробовать вернуться
            на основной сервер, если сейчас используется резервный.
        :param health_check_interval: как часто (сек) проверять живость
            fallback-эндпоинтов в фоне.
        :param fallback_dead_cooldown: сколько (сек) пропускать fallback,
            который health-check признал мёртвым.
        :param max_attempts_per_request: сколько эндпоинтов пробовать за один
            вызов make_request (по умолчанию — все доступные на момент запроса).
        """
        super().__init__(**kwargs)

        self._primary_server: TelegramAPIServer = self.api
        self._fallback_servers: list[TelegramAPIServer] = []

        # Явно переданные готовые fallback-урлы (приоритет №1)
        for url in fallback_urls or []:
            self.add_fallback(url)

        # Дополнительные урлы, которые всегда добавляются поверх
        # автоматически созданного Worker'а
        self._extra_fallback_urls: list[str] = list(extra_fallback_urls or [])
        for url in self._extra_fallback_urls:
            self.add_fallback(url)

        # Настройки автодеплоя дефолтного Worker'а
        self._auto_fallback = auto_fallback and not fallback_urls
        self._cf_api_token = cf_api_token or os.environ.get("CF_API_TOKEN")
        self._cf_account_id = cf_account_id or os.environ.get("CF_ACCOUNT_ID")
        self._cf_zone_id = cf_zone_id or os.environ.get("CF_ZONE_ID")
        self._cf_route_pattern = cf_route_pattern or os.environ.get(
            "CF_ROUTE_PATTERN")
        self._cf_script_name = cf_script_name
        self._auto_fallback_task: Optional[asyncio.Task] = None

        self._request_timeout = request_timeout
        self._connect_timeout = connect_timeout
        self._deploy_wait_timeout = deploy_wait_timeout
        self._recovery_check_interval = recovery_check_interval
        self._health_check_interval = health_check_interval
        self._fallback_dead_cooldown = fallback_dead_cooldown
        self._max_attempts_override = max_attempts_per_request

        # -1 == используем primary; 0..N-1 == индекс в _fallback_servers
        self._active_index: int = -1
        self._last_recovery_attempt: float = 0.0
        self._last_health_check: float = 0.0
        # индекс fallback'а -> monotonic-время, до которого он считается мёртвым
        self._fallback_dead_until: dict[int, float] = {}
        self._background_tasks: set[asyncio.Task] = set()

        # Статистика: "primary" / "fallback[0]" / ... -> число успешных запросов
        self._request_counts: dict[str, int] = {}
        self._switch_count: int = 0

    def add_fallback(self, url: str) -> None:
        """Добавить дополнительный fallback-URL. Можно вызывать в любой
        момент — до старта бота или прямо во время работы."""
        server = TelegramAPIServer.from_base(url.rstrip("/"))
        existing_bases = {s.base for s in self._fallback_servers}
        if server.base in existing_bases:
            logger.debug("Fallback %s уже добавлен, пропускаю", url)
            return
        self._fallback_servers.append(server)
        logger.info("Добавлен fallback-эндпоинт: %s", url)

    @property
    def fallback_urls(self) -> list[str]:
        """Текущий список fallback-URL (для отладки/логирования)."""
        return [self._probe_url(s) for s in self._fallback_servers]

    @property
    def active_endpoint(self) -> str:
        """URL текущего активного эндпоинта (для отладки/логирования)."""
        if self._active_index == -1:
            return self._probe_url(self._primary_server)
        return self._probe_url(self._fallback_servers[self._active_index])

    @property
    def stats(self) -> dict:
        """Статистика: успешные запросы по эндпоинтам и число переключений."""
        return {
            "requests": dict(self._request_counts),
            "switches": self._switch_count,
            "active": self._server_label(self._active_index),
        }

    def _maybe_start_auto_fallback(self) -> None:
        """Запустить автодеплой Worker'а в фоне (не блокируя запросы)."""
        if not self._auto_fallback or self._auto_fallback_task is not None:
            return
        self._auto_fallback_task = asyncio.create_task(
            self._deploy_auto_fallback())

    async def _deploy_auto_fallback(self) -> None:
        """Разворачивает дефолтный Cloudflare Worker (если CF_API_TOKEN /
        CF_ACCOUNT_ID заданы) и добавляет его как базовый fallback."""
        if not self._cf_api_token or not self._cf_account_id:
            logger.warning(
                "auto_fallback включён, но CF_API_TOKEN/CF_ACCOUNT_ID не "
                "заданы — автодеплой Worker'а пропущен. Работаем только "
                "с fallback'ами, переданными вручную (если есть)."
            )
            return

        try:
            deployer = CloudflareWorkerDeployer(
                api_token=self._cf_api_token,
                account_id=self._cf_account_id,
                zone_id=self._cf_zone_id,
                script_name=self._cf_script_name,
            )
            worker_url = await deployer.ensure_deployed(
                route_pattern=self._cf_route_pattern,
            )
            self.add_fallback(worker_url)
            logger.info("Базовый fallback Worker готов: %s", worker_url)
        except Exception as exc:
            logger.error(
                "Не удалось автоматически задеплоить Worker: %s", exc)

    def _all_servers(self) -> list[TelegramAPIServer]:
        """Список всех эндпоинтов: primary первым, затем fallback'и."""
        return [self._primary_server, *self._fallback_servers]

    def _server_label(self, index: int) -> str:
        """Человекочитаемое имя эндпоинта для логов/статистики."""
        return "primary" if index == -1 else f"fallback[{index}]"

    @staticmethod
    def _probe_url(server: TelegramAPIServer) -> str:
        """Базовый URL эндпоинта без шаблона (host из base-шаблона)."""
        netloc = urlparse(server.base).netloc
        scheme = urlparse(server.base).scheme or "https"
        return f"{scheme}://{netloc}"

    def _is_fallback_dead(self, index: int) -> bool:
        """Мёртв ли fallback по данным последнего health-check'а."""
        return time.monotonic() < self._fallback_dead_until.get(index, 0.0)

    def _spawn(self, coro) -> None:
        """Создать фоновую task и не дать ей исчезнуть из-за GC."""
        task = asyncio.create_task(coro)
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _maybe_try_recovery(self) -> None:
        """Если сейчас сидим на резервном сервере — периодически пробуем
        тихо вернуться на primary в фоне (не в текущем запросе, чтобы не
        тормозить пользователя)."""
        if self._active_index == -1:
            return
        now = time.monotonic()
        if now - self._last_recovery_attempt < self._recovery_check_interval:
            return
        self._last_recovery_attempt = now
        self._spawn(self._background_recovery_probe())

    async def _background_recovery_probe(self) -> None:
        """Фоновая проверка доступности primary: если TCP/TLS соединение
        с api.telegram.org снова работает — переключаемся обратно."""
        try:
            timeout = aiohttp.ClientTimeout(
                total=5, sock_connect=self._connect_timeout)
            async with aiohttp.ClientSession(timeout=timeout) as s:
                async with s.get(self._probe_url(self._primary_server)) as resp:
                    # Telegram отвечает 404 на "/", это нормально —
                    # главное, что TCP/TLS соединение установилось.
                    if resp.status:
                        if self._active_index != -1:
                            logger.info(
                                "Primary Bot API снова доступен, "
                                "переключаемся обратно с %s",
                                self._server_label(self._active_index),
                            )
                            self._active_index = -1
                            self._sync_api_server()
        except Exception:
            logger.debug("Primary Bot API всё ещё недоступен")

    async def _maybe_health_check(self) -> None:
        """Периодически в фоне проверять живость fallback-эндпоинтов.
        Мёртвые помечаются cooldown'ом и пропускаются в make_request."""
        if not self._fallback_servers:
            return
        now = time.monotonic()
        if now - self._last_health_check < self._health_check_interval:
            return
        self._last_health_check = now
        self._spawn(self._background_health_check())

    async def _background_health_check(self) -> None:
        """Пробить каждый fallback: любой HTTP-ответ (даже 404) — жив."""
        timeout = aiohttp.ClientTimeout(
            total=5, sock_connect=self._connect_timeout)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            for index, server in enumerate(self._fallback_servers):
                try:
                    async with s.get(self._probe_url(server)):
                        self._fallback_dead_until.pop(index, None)
                except Exception:
                    self._fallback_dead_until[index] = (
                        time.monotonic() + self._fallback_dead_cooldown)
                    logger.warning(
                        "fallback[%d] (%s) не отвечает, пропускаем на %d сек",
                        index, self._probe_url(server),
                        int(self._fallback_dead_cooldown),
                    )

    def _sync_api_server(self) -> None:
        """Синхронизировать self.api с активным эндпоинтом.

        self.api нужен не только для make_request (там сервер передаётся
        явно), но и для Bot.download_file — он строит URL файлов через
        session.api.file_url(...). Обновляем только при смене активного
        эндпоинта, а не на каждый запрос — это исключает гонки между
        параллельными запросами.
        """
        if self._active_index == -1:
            self.api = self._primary_server
        else:
            self.api = self._fallback_servers[self._active_index]

    async def _send(
        self,
        server: TelegramAPIServer,
        bot: "Bot",
        method: "TelegramMethod[TelegramType]",
        timeout: int,
    ) -> TelegramType:
        """Отправить один запрос к конкретному эндпоинту.

        Аналог AiohttpSession.make_request, но с явным сервером (без
        мутации self.api) и с коротким sock_connect-таймаутом: если
        соединение не устанавливается (блокировка), запрос падает за
        connect_timeout, а не за весь total.
        """
        session = await self.create_session()
        url = server.api_url(token=bot.token, method=method.__api_method__)
        form = self.build_form_data(bot=bot, method=method)
        request_timeout = aiohttp.ClientTimeout(
            total=timeout, sock_connect=self._connect_timeout)

        try:
            async with session.post(
                url, data=form, timeout=request_timeout,
            ) as resp:
                raw_result = await resp.text()
        except asyncio.TimeoutError as e:
            raise TelegramNetworkError(
                method=method, message="Request timeout error") from e
        except ClientError as e:
            raise TelegramNetworkError(
                method=method, message=f"{type(e).__name__}: {e}") from e

        response = self.check_response(
            bot=bot,
            method=method,
            status_code=resp.status,
            content=raw_result,
        )
        return response.result

    async def _try_endpoints(
        self,
        bot: "Bot",
        method: "TelegramMethod[TelegramType]",
        timeout: Optional[int],
    ) -> TelegramType:
        """Перебрать эндпоинты: сначала текущий активный, затем остальные по
        кругу (мёртвые по health-check'у уходят в конец очереди). При сетевой
        ошибке переключается на следующий эндпоинт. Если все недоступны —
        бросает последнюю ошибку (или RuntimeError, если эндпоинтов нет)."""
        servers = self._all_servers()
        max_attempts = self._max_attempts_override or len(servers)

        start = self._active_index + 1 if self._active_index >= 0 else 0
        # порядок перебора: сначала текущий активный, затем остальные по кругу
        order = [start] + [i for i in range(len(servers)) if i != start]

        # мёртвые fallback'ы уводим в конец очереди (но не выбрасываем —
        # если умрёт всё, лучше попробовать, чем не попробовать)
        order.sort(key=lambda i: self._is_fallback_dead(i - 1))
        order = order[:max_attempts]

        last_exc: Optional[BaseException] = None

        for idx in order:
            real_index = idx - 1  # -1 == primary, см. индексацию _all_servers
            try:
                result = await self._send(
                    servers[idx], bot, method,
                    timeout=timeout or int(self._request_timeout),
                )
            except _FAILOVER_EXCEPTIONS as exc:
                # TelegramNetworkError — это ВСЕГДА сетевая ошибка (aiogram
                # оборачивает в неё timeout/ClientError), значит переключение
                # на резервный эндпоинт оправдано.
                logger.warning(
                    "[%s] запрос %s провалился: %s",
                    self._server_label(real_index),
                    type(method).__name__,
                    exc,
                )
                last_exc = exc
                continue
            else:
                if real_index != self._active_index:
                    logger.warning(
                        "Bot API endpoint переключён: %s -> %s",
                        self._server_label(self._active_index),
                        self._server_label(real_index),
                    )
                    self._active_index = real_index
                    self._switch_count += 1
                    self._sync_api_server()
                self._request_counts[self._server_label(real_index)] = (
                    self._request_counts.get(self._server_label(real_index), 0)
                    + 1
                )
                return result

        # все эндпоинты недоступны (либо fallback'ов вообще нет)
        if last_exc is None:
            raise RuntimeError(
                "Нет доступных Bot API эндпоинтов: primary недоступен, "
                "а fallback-серверы не настроены (auto_fallback выключен "
                "или автодеплой не удался)."
            )
        raise last_exc

    async def make_request(
        self,
        bot: "Bot",
        method: "TelegramMethod[TelegramType]",
        timeout: Optional[int] = None,
    ) -> TelegramType:
        """Выполнить запрос к Bot API с автоматическим перебором эндпоинтов:
        сначала текущий активный, затем остальные по кругу (мёртвые по
        health-check'у пропускаются, если есть живые). При сетевой ошибке
        переключается на следующий эндпоинт.

        Особый случай первого запроса: если primary заблокирован, а
        автодеплой fallback-Worker'а ещё выполняется в фоне (деплой обычно
        медленнее первого запроса), запрос не падает сразу — мы ждём
        завершения деплоя (не дольше deploy_wait_timeout) и повторяем
        запрос уже с готовым fallback'ом."""
        self._maybe_start_auto_fallback()
        await self._maybe_try_recovery()
        await self._maybe_health_check()

        try:
            return await self._try_endpoints(bot, method, timeout)
        except _FAILOVER_EXCEPTIONS:
            # Все эндпоинты упали. Если автодеплой Worker'а ещё в работе —
            # подождём его (с ограничением) и повторим: к этому моменту
            # fallback должен появиться в списке. shield() гарантирует, что
            # при таймауте сама деплой-задача НЕ отменяется и продолжит
            # работать для следующих запросов.
            task = self._auto_fallback_task
            if task is not None and not task.done():
                logger.info(
                    "Все эндпоинты недоступны, ждём завершения автодеплоя "
                    "fallback-Worker'а (до %.0f сек)...",
                    self._deploy_wait_timeout,
                )
                try:
                    await asyncio.wait_for(
                        asyncio.shield(task),
                        timeout=self._deploy_wait_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Автодеплой Worker'а не завершился за %.0f сек — "
                        "запрос падает, повторный запрос попробует снова",
                        self._deploy_wait_timeout,
                    )
                return await self._try_endpoints(bot, method, timeout)
            raise

    async def close(self) -> None:
        """Закрыть сессию и отменить фоновые задачи."""
        for task in list(self._background_tasks):
            task.cancel()
        if self._auto_fallback_task is not None:
            self._auto_fallback_task.cancel()
        await super().close()
