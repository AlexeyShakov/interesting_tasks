import asyncio
import aiohttp


INTERVIEWED_URLS = []
THREE_SERVICES = (
    'http://service1/fillItems/',
    'http://service2/scoreItems/',
    'http://service3/logItems/',
)

"""
Асинхронно обрабатывает ~1M URL-ов с помощью aiohttp и asyncio.

Текущий подход:

* Для каждого URL создаётся отдельная asyncio.Task (примерно 1 миллион задач).
  Большинство задач большую часть времени простаивают в ожидании свободного TCP-соединения.
* Параллелизм целиком регулируется aiohttp.TCPConnector:
    - limit=500 ограничивает общее количество одновременных TCP-соединений;
    - limit_per_host=100 ограничивает число соединений к одному сервису.
  Корутин может быть много, но не более этих лимитов делают HTTP-запрос в каждый момент времени.
* Каждая задача:
    1) делает запрос к сервису getItems,
    2) получает item_ids,
    3) параллельно опрашивает три дополнительных сервиса.
* Все результаты добавляются в глобальный список RESULT.
* После завершения асинхронной стадии выполняется CPU-bound обработка RESULT
  через ProcessPoolExecutor.

Плюсы:

* Простая и наглядная модель.
* Ограничение нагрузки происходит автоматически за счёт TCPConnector.
* I/O-этап и CPU-этап разделены, что упрощает дальнейшее развитие.

Минусы:

* Создаётся слишком много asyncio.Task — большой расход памяти
  и лишняя нагрузка на планировщик.
* CPU-обработка начинается только после завершения всех сетевых операций.

Неоптимально для продакшена, но подходит как «решение в лоб» для дальнейшего сравнения
с очередями, пулами воркеров или стриминговой обработкой.
"""


# TODO: добавить backoff
async def _request(
    url: str,
    session: aiohttp.ClientSession,
    params: list[tuple[str, int]],
    timeout: float = 0.8,
) -> tuple[int, dict]:
    """
    Делает GET-запрос и возвращает (status_code, json_dict).

    Исключения aiohttp/TimeoutError наружу не гасим — вызывающая сторона решает,
    что с ними делать.
    """
    async with session.get(url, params=params, timeout=timeout) as response:
        return response.status, await response.json()


async def _interview_services(
    items: list[int],
    session: aiohttp.ClientSession,
) -> list[float]:
    """
    Для списка item_ids опрашивает три независимых сервиса
    и возвращает список найденных цен (price) для этого пользователя.

    Ошибки/таймауты по отдельным запросам не роняют всё:
    соответствующие элементы просто пропускаются.
    """
    # Можно обойтись без create_task — gather сам создаст нужные futures
    coros = [
        _request(service, session, [('item_ids', item) for item in items])
        for service in THREE_SERVICES
    ]
    results = await asyncio.gather(*coros, return_exceptions=True)

    prices: list[float] = []

    for res in results:
        if isinstance(res, Exception):
            # TODO: лог/метрики
            continue

        status, data = res
        if status == 200:
            price = data.get('price')
            if price is not None:
                prices.append(price)
        else:
            # TODO: добавить логи в зависимости от статуса
            pass

    return prices


async def _get_items(
    url: str,
    session: aiohttp.ClientSession,
) -> list[float]:
    """
    Для одного URL:
      1) Получает список item_ids с основного сервиса.
      2) Опрашивает три дополнительных сервиса.
      3) Возвращает список цен (может быть пустым).

    Любые сетевые ошибки на этом уровне приводят к возвращению пустого списка,
    чтобы не ронять общий пайплайн.
    """
    try:
        status, data = await _request(url, session, [('something', 1)])
    except (aiohttp.ClientError, asyncio.TimeoutError):
        # TODO: логируем, считаем метрику, решаем — ретраить или нет
        return []

    # TODO: также обрабатывать другие интересующие ошибки/исключения, если нужно

    if status != 200:
        # TODO: добавить логи в зависимости от статуса
        return []

    items: list[int] | None = data.get('item_ids')
    if not items:
        return []

    # Возвращаем список цен по этому URL
    return await _interview_services(items, session)


async def main() -> list[float]:
    """
    Асинхронная стадия:
      — создаёт ClientSession с ограничением на количество TCP-соединений;
      — параллельно обрабатывает все INTERVIEWED_URLS;
      — собирает и возвращает единый список цен.
    """
    connector = aiohttp.TCPConnector(
        limit=500,        # общее число одновременных соединений
        limit_per_host=100,  # максимум на один (host, port)
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        # Создаём по задаче на каждый URL
        tasks = [_get_items(url, session) for url in INTERVIEWED_URLS]
        results = await asyncio.gather(*tasks, return_exceptions=True)

    all_prices: list[float] = []

    for res in results:
        if isinstance(res, Exception):
            # TODO: лог/метрики
            continue

        # res — это список цен, который вернул _get_items
        all_prices.extend(res)

    return all_prices


def cpu_heavy_process(item: float):
    """тяжёлая CPU логика для одной цены"""
    # TODO: твоя реальная CPU-логика
    return item  # заглушка


def run_cpu_stage(items: list[float]) -> list:
    """
    Синхронная CPU-стадия: принимает список цен и обрабатывает их в пуле процессов.
    """
    from concurrent.futures import ProcessPoolExecutor

    with ProcessPoolExecutor() as pool:
        processed = list(pool.map(cpu_heavy_process, items))
    return processed


if __name__ == '__main__':
    prices = asyncio.run(main())          # асинхронная I/O-часть
    processed_result = run_cpu_stage(prices)  # синхронная CPU-часть
