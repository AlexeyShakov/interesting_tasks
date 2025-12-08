from anyio import create_task_group, create_memory_object_stream, run, to_thread
from anyio.streams.memory import MemoryObjectReceiveStream, MemoryObjectSendStream
import aiohttp


INTERVIEWED_URLS = []
THREE_SERVICES = (
    'http://service1/fillItems/',
    'http://service2/scoreItems/',
    'http://service3/logItems/',
)


# TODO: можно применить circuit breaker
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


async def save(
    rec_stream: MemoryObjectReceiveStream[list[int]],
    accumulated_results: list[int],
) -> None:
    """
    Финальный этап пайплайна: получает батчи list[int] и накапливает их в общем списке.

    Здесь можно заменить накопление на запись в БД/файл и т.п.
    """
    async with rec_stream:
        async for result_batch in rec_stream:
            # простое накопление результатов
            accumulated_results.extend(result_batch)
            # TODO: здесь можно добавить логирование, запись в БД, метрики и т.п.


def _cpu_heavy_transform(prices: list[float]) -> list[int]:
    """
    Синхронная CPU-bound функция.

    Здесь мы просто имитируем тяжёлую работу: немного бесполезной математики + преобразование
    float -> int. В реальном коде это могла бы быть, например, сложная модель/агрегация.
    """
    # искусственная нагрузка, чтобы было похоже на "тяжёлую" CPU-задачу
    acc = 0
    for p in prices:
        x = int(p * 100)
        for _ in range(1000):
            x = (x * 31 + 7) % 1_000_003
        acc += x

    # возвращаем список int; здесь просто округляем цены до центов
    return [int(p * 100) for p in prices]


async def work_cpu(
    rec_stream: MemoryObjectReceiveStream[list[float]],
    send_stream: MemoryObjectSendStream[list[int]],
) -> None:
    """
    CPU-этап: принимает list[float] (например, цены от сервисов),
    выполняет CPU-bound обработку в отдельном потоке и отдаёт list[int] дальше.

    Важно: тяжёлая логика вынесена в синхронную функцию _cpu_heavy_transform,
    а здесь мы вызываем её через to_thread.run_sync, чтобы не блокировать event loop.
    """
    async with rec_stream, send_stream:
        async for prices in rec_stream:
            # Используем поток, чтобы не блокировать event-loop.
            # Если CPU-работа станет действительно тяжёлой, стоит подумать
            # о переходе на ProcessPool для использования нескольких ядер.
            processed: list[int] = await to_thread.run_sync(
                _cpu_heavy_transform,
                prices,
            )
            processed: list[int] = await to_thread.run_sync(
                _cpu_heavy_transform,
                prices,
            )
            await send_stream.send(processed)


async def interview_one_service(
    service: str,
    session: aiohttp.ClientSession,
    items: list[int],
    prices: list[float],
) -> None:
    """
    Вызывает один из трёх внешних сервисов по списку items и,
    если всё ок, добавляет найденную цену в общий список prices.
    """
    try:
        status, data = await _request(
            service,
            session,
            [('item_ids', item) for item in items],
        )
        if status == 200:
            price = data.get('price')
            if price is not None:
                prices.append(price)
        else:
            # TODO: добавить логи в зависимости от статуса (4xx/5xx и т.п.)
            pass
    except Exception as e:
        # TODO: обработать/залогировать ошибку, возможно добавить метрики
        # Ошибка гасится здесь, чтобы не падала вся партия items
        pass


async def interview_services(
    rec_stream: MemoryObjectReceiveStream[list[int]],
    send_stream: MemoryObjectSendStream[list[float]],
    session: aiohttp.ClientSession,
) -> None:
    """
    Этап агрегации по трём сервисам.

    На вход: батч item_ids: list[int]
    На выход: батч цен: list[float] (агрегированный результат по всем трём сервисам)

    Для каждого батча items:
      * создаётся отдельная task group;
      * внутри неё параллельно вызываются три сервиса;
      * каждый успешный вызов дополняет общий список prices;
      * после завершения группы батч prices отправляется дальше по пайплайну.
    """
    async with rec_stream, send_stream:
        async for items in rec_stream:
            prices: list[float] = []

            async with create_task_group() as tg:
                for service in THREE_SERVICES:
                    tg.start_soon(interview_one_service, service, session, items, prices)

            # Здесь все три вызова либо завершились, либо были отменены (если бы не ловили исключения)
            await send_stream.send(prices)


async def get_items(
    rec_stream: MemoryObjectReceiveStream[str],
    send_stream: MemoryObjectSendStream[list[int]],
    session: aiohttp.ClientSession,
) -> None:
    """
    Этап получения item_ids по исходным URL-ам.
    На вход: URL (str)
    На выход: list[int] c item_ids.
    """
    async with rec_stream, send_stream:
        async for url in rec_stream:
            try:
                status, data = await _request(url, session, [('something', 1)])
            except Exception as e:
                # TODO: несколько отдельных except для разных групп ошибок + логирование/метрики
                continue

            if status != 200:
                # TODO: лог по статусу
                continue

            items: list[int] | None = data.get('item_ids')
            if not items:
                continue

            # await здесь НЕ блокирует event loop "жёстко", а даёт backpressure
            await send_stream.send(items)


async def produce(send_stream: MemoryObjectSendStream[str]) -> None:
    """
    Стартовый этап: пробегается по INTERVIEWED_URLS и отправляет их дальше по пайплайну.
    """
    async with send_stream:
        for url in INTERVIEWED_URLS:
            await send_stream.send(url)


async def main() -> None:
    # TODO: для разных стадий можно подобрать разные размеры буферов
    # 1) URLs
    produce_send_stream, produce_rec_stream = create_memory_object_stream[str](
        max_buffer_size=500,
    )
    # 2) item_ids
    items_send_stream, items_rec_stream = create_memory_object_stream[list[int]](
        max_buffer_size=500,
    )
    # 3) prices (float)
    service_send_stream, service_rec_stream = create_memory_object_stream[list[float]](
        max_buffer_size=500,
    )
    # 4) CPU-processed results (int)
    work_cpu_send_stream, work_cpu_rec_stream = create_memory_object_stream[list[int]](
        max_buffer_size=500,
    )

    # Здесь будем аккумулировать финальный результат (можно заменить на БД и т.п.)
    accumulated_results: list[int] = []

    connector = aiohttp.TCPConnector(
        limit=500,          # общее число одновременных соединений
        limit_per_host=100, # максимум на один (host, port)
    )

    async with aiohttp.ClientSession(connector=connector) as session:
        async with create_task_group() as tg:
            tg.start_soon(produce, produce_send_stream)
            tg.start_soon(get_items, produce_rec_stream, items_send_stream, session)
            tg.start_soon(interview_services, items_rec_stream, service_send_stream, session)
            tg.start_soon(work_cpu, service_rec_stream, work_cpu_send_stream)
            tg.start_soon(save, work_cpu_rec_stream, accumulated_results)

    # Здесь accumulated_results уже заполнен
    print(accumulated_results)  # или любая другая финальная обработка


if __name__ == '__main__':
    run(main)
