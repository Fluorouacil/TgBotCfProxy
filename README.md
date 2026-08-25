# TgBotCfProxy

Failover-сессия для [aiogram](https://github.com/aiogram/aiogram) 3.x: при сбоях
прямого подключения к `api.telegram.org` (DPI-блокировки, обрыв TLS handshake,
таймауты) автоматически переключается на резервный Cloudflare Worker,
проксирующий Bot API. Резервный Worker может разворачиваться сама библиотека —
через официальный Cloudflare API — либо вы указываете свои готовые адреса.

## Возможности

- **Автоматический failover**: при сетевой ошибке запрос уходит на следующий
  эндпоинт (primary → fallback[0] → fallback[1] → ...).
- **Автодеплой Worker'а**: если fallback-URL не переданы, библиотека сама
  разворачивает Cloudflare Worker через REST API (в фоне, не блокируя запросы).
- **Быстрое переключение**: отдельный короткий `connect_timeout` — при
  блокировке primary запрос падает за 2–5 секунд, а не за total-таймаут
  (важно для long-polling, где aiogram ждёт до 90 секунд).
- **Health-check fallback'ов**: фоновая проверка живости каждого резервного
  эндпоинта; мёртвые временно пропускаются (cooldown).
- **Recovery**: периодически в фоне пробует вернуться на прямое подключение —
  резервный канал используется только пока основной недоступен.
- **Статистика**: `session.stats` — сколько запросов ушло через каждый
  эндпоинт и сколько было переключений.
- **Гонки исключены**: сервер передаётся в запрос явно, `self.api`
  обновляется только при смене активного эндпоинта (нужно для
  `Bot.download_file`).

## Установка

Из локальной копии репозитория (editable-режим, удобно для разработки):

```bash
pip install -e .
```

Обычная установка (после `pip install build` и сборки wheel):

```bash
pip install .
```

После установки в любом месте, где доступно это окружение Python, работает:

```python
from tgbotcfproxy import FailoverAiohttpSession, CloudflareWorkerDeployer
```

## Быстрый старт

```python
import os
from aiogram import Bot
from tgbotcfproxy import FailoverAiohttpSession

os.environ["CF_API_TOKEN"] = "..."
os.environ["CF_ACCOUNT_ID"] = "..."

session = FailoverAiohttpSession()          # fallback Worker создастся сам
bot = Bot(token="123:ABC", session=session)
```

Полный пример бота — в [`example_bot.py`](example_bot.py).

## Параметры `FailoverAiohttpSession`

| Параметр | По умолчанию | Описание |
|---|---|---|
| `fallback_urls` | `None` | Явный список готовых fallback-URL. Если задан — автодеплой Worker'а не запускается. |
| `extra_fallback_urls` | `None` | Дополнительные fallback-URL поверх автоматически созданного Worker'а. |
| `auto_fallback` | `True` | Автоматически разворачивать Cloudflare Worker при первом запросе. |
| `cf_api_token` / `cf_account_id` | из env | Токен и ID аккаунта Cloudflare (или env `CF_API_TOKEN` / `CF_ACCOUNT_ID`). |
| `cf_zone_id` / `cf_route_pattern` | из env | Для route на своём домене (env `CF_ZONE_ID` / `CF_ROUTE_PATTERN`). |
| `cf_script_name` | `"tgbotcfproxy"` | Имя Worker-скрипта в Cloudflare. |
| `request_timeout` | `10.0` | Таймаут одной попытки запроса (total), сек. |
| `connect_timeout` | `5.0` | Таймаут установки соединения (TCP+TLS), сек. |
| `deploy_wait_timeout` | `30.0` | Сколько максимум ждать завершения фоновой автодеплоя Worker'а, если primary упал, а fallback ещё не создан. |
| `recovery_check_interval` | `60.0` | Как часто пробовать вернуться на primary, сек. |
| `health_check_interval` | `30.0` | Как часто проверять живость fallback'ов, сек. |
| `fallback_dead_cooldown` | `30.0` | Сколько пропускать мёртвый fallback, сек. |
| `max_attempts_per_request` | `None` | Сколько эндпоинтов пробовать за один запрос (по умолчанию — все). |

Дополнительно поддерживаются все параметры `AiohttpSession` (`proxy`, `limit`,
`api`, `timeout` и т.д.).

В рантайме можно добавлять резервные адреса:

```python
session.add_fallback("https://backup2.example.com")
```

## Переменные окружения

| Переменная | Обязательна | Описание |
|---|---|---|
| `BOT_TOKEN` | да | Токен бота (в `example_bot.py`). |
| `CF_API_TOKEN` | для автодеплоя | Cloudflare API token (права: `Account.Workers Scripts:Edit`, `Zone.Workers Routes:Edit`). |
| `CF_ACCOUNT_ID` | для автодеплоя | Cloudflare Account ID. |
| `CF_ZONE_ID` | нет | Zone ID, если нужен route на своём домене. |
| `CF_ROUTE_PATTERN` | нет | Напр. `proxy.example.com/*`. |

## Структура пакета

```
src/tgbotcfproxy/
├── __init__.py     # публичные экспорты
├── session.py      # FailoverAiohttpSession
└── cf_deploy.py    # CloudflareWorkerDeployer (автодеплой Worker'а)
tests/              # pytest-тесты failover-сценариев
```

Используется src-layout — код лежит в `src/tgbotcfproxy`, а не прямо в корне
репозитория. Это исключает случайные конфликты импорта: `import tgbotcfproxy`
всегда берёт установленный пакет, а не файл, который случайно оказался
в текущей рабочей директории.

## Тесты

```bash
pip install pytest pytest-asyncio
pytest
```

Тесты поднимают локальный mock-сервер Bot API и проверяют: failover на
fallback, работу при доступном primary, recovery обратно на primary,
поведение при полном отбое, health-check мёртвых fallback'ов.

## Ограничения

- **Лимиты Cloudflare Free**: 100 000 запросов/день на Worker. Для одного бота
  polling это ~2 880 запросов/день (getUpdates раз в 30 с) плюс исходящие —
  запас огромный, но при нескольких ботах на одном Worker'е стоит считать.
- **`workers.dev` может быть заблокирован** в некоторых сетях — для этого есть
  `extra_fallback_urls` (свой VPS/прокси) или `CF_ROUTE_PATTERN` (свой домен).
- **Токен бота виден в URL на Worker'е** — inherent-ограничение проксирования
  Bot API. Трафик шифруется TLS, Worker не логирует URL; для параноидального
  случая — отдельный Worker на каждый токен.
- **`compatibility_date`** в `cf_deploy.py` захардкожен (`2024-09-23`) — при
  обновлении Cloudflare API может потребоваться его поднять.
- **Recovery-проба** проверяет только TCP/TLS доступность primary (GET на
  корень), а не работоспособность Bot API.
