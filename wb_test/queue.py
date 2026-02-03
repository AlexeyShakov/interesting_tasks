"""
Задача

Дан список из ~1_000_000 URL (requests_samples), каждый элемент:
  (url, query_params)

Для каждого URL:
  1) сделать GET на url и получить JSON вида {"item_ids": [1,2,3]}
  2) по item_ids параллельно вызвать 3 независимых сервиса:
       service_1_url = 'http://service1/fillItems/'
       service_2_url = 'http://service2/scoreItems/'
       service_3_url = 'http://service3/logItems/'
  3) собрать агрегированный результат по данным всех 3х сервисов и сохранить в общий список storage

После завершения I/O этапа:
  4) обработать storage CPU-bound функцией (тяжёлая синхронная логика).
"""

"""
Идея решения

Используется ограниченная асинхронная очередь (asyncio.Queue) и фиксированное количество воркеров.
Producer кладёт задачи в очередь, воркеры конкурентно читают из неё, выполняют I/O-операции и сохраняют результат.
"""

import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Any, Optional


THREE_SERVICES = (
    "http://service1/fillItems/",
    "http://service2/scoreItems/",
    "http://service3/logItems/",
)

HTTP_TIMEOUT = aiohttp.ClientTimeout(
    total=5,
    connect=2,
    sock_read=3,
)


@dataclass
class FetchOk:
    data: Any
    status: int


class InvalidJsonData(Exception):
    ...


class NotOKStatus(Exception):
    ...


async def fetch(
    session: aiohttp.ClientSession,
    url: str,
    query_params: dict,
    timeout: int = 5,
) -> FetchOk:
    """
    Делает запросы на удаленные серверы
    :param session:
    :param url:
    :param query_params:
    :param timeout:
    :return:
    """
    # TODO Возможно тут стоить поставить семафор, чтобы ограничить количество запросов, которое уходит от нас, т.к.
    # TODO у нас тут кол-во воркеров * 3?
    async with session.get(url=url, params=query_params, timeout=timeout) as resp:
        status = resp.status
        if status in (200, 201):
            try:
                data = await resp.json()
                return FetchOk(data=data, status=status)
            except Exception:
                body = await resp.text()
                raise InvalidJsonData(body)
        else:
            # TODO здесь нужно сделать разделение по статусам, но пока просто пишем одну ошибку
            raise NotOKStatus(
                f"Неудачный статус {status} для url={url}, params={query_params}, body={body}"
            )


def _cpu_heavy_transform(storage: list[dict]) -> list[int]:
    """
    CPU-bound стадия: имитация тяжёлой синхронной обработки.
    В реальности тут могла бы быть агрегация/скоринг/модель и т.п.
    """
    out: list[int] = []
    for item in storage:
        # допустим, мы хотим превратить поля в числа и "покрутить"
        x = 0
        for v in item.get("values", []):
            y = int(v * 100)
            for _ in range(1000):
                y = (y * 31 + 7) % 1_000_003
            x += y
        out.append(x)
    return out


async def request_three_services(item_ids: list[int], session: aiohttp.ClientSession) -> dict:
    """
    Параллельно опрашивает 3 сервиса одним батчем item_ids и возвращает агрегированный результат.

    Важно: вместо того чтобы пушить "сырые" ответы в общий список,
    лучше вернуть один нормализованный dict, чтобы дальше было удобно CPU-обрабатывать.
    """
    # пример формата query params: item_ids=1&item_ids=2&item_ids=3
    params = [("item_ids", i) for i in item_ids]

    tasks = [asyncio.create_task(fetch(session, url, dict(params))) for url in THREE_SERVICES]

    try:
        results = await asyncio.gather(*tasks, return_exceptions=True)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        res = await asyncio.gather(*tasks, return_exceptions=True)
        # TODO обработать res
        raise

    # Агрегация результатов: учитываем только успешные ответы
    aggregated: dict = {"values": []}
    for res in results:
        if isinstance(res, BaseException):
            continue
        # условно достаём что-то полезное
        # например сервис возвращает {"price": 12.34} или {"score": 0.91}
        data = res.data
        if "price" in data and data["price"] is not None:
            aggregated["values"].append(float(data["price"]))
        if "score" in data and data["score"] is not None:
            aggregated["values"].append(float(data["score"]))
        # logItems может возвращать что-то вообще без значений — это ок

    return aggregated


async def work(
    session: aiohttp.ClientSession,
    q: asyncio.Queue,
    storage: list[dict],
):
    """
    Воркер, обрабатывающий сообщения из очереди

    1. Запускает бесконечный цикл для обработки сообщений из очереди
    2. Посылает запрос на исходный сервис
    3. Опрашивает 3 других сервиса, данными которые получил от исходного
    4. Останавливает свою работу, если приходит None
    :param session:
    :param q:
    :param storage:
    :return:
    """
    while True:
        item = await q.get()
        if item is None:
            q.task_done()
            return

        url, params = item
        try:
            result_from_url = await fetch(session, url, params)
            item_ids: Optional[list[int]] = result_from_url.data.get("item_ids")

            if not item_ids:
                continue

            aggregated = await request_three_services(item_ids, session)
            storage.append(aggregated)

        except asyncio.CancelledError:
            # TODO сделать лог
            raise
        # TODO Описать все остальные возможные ошибки
        except Exception:
            # TODO: логирование + метрики
            pass
        finally:
            q.task_done()


async def produce(request_samples: list[tuple], q: asyncio.Queue):
    """
    Наполняем очередь данными
    :param url_data:
    :param q:
    :return:
    """
    for data in request_samples:
        await q.put(data)


async def stop_workers(workers: list[asyncio.Task], q: asyncio.Queue) -> None:
    """
    Останавливаем воркеры
    :param workers:
    :param q:
    :return:
    """
    for _ in workers:
        await q.put(None)


async def cancel_tasks(tasks: list[asyncio.Task]) -> None:
    """
    Посылаем сигнал об отмене еще не завершенным воркерам
    :param tasks:
    :return:
    """
    for task in tasks:
        if not task.done():
            task.cancel()


async def process(
    session: aiohttp.ClientSession,
    q: asyncio.Queue,
    storage: list[dict],
    request_samples: list[tuple],
    workers_count: int = 20,
):
    """
    Оркестратор

    1. Запускаем воркеры
    2. Запускаем продюсер
    3. Дожидаемся обработки всех сообщений
        - если происходят ошибки, обрабатываем их
    4. Останавливаем и закрываем воркеры
    :return:
    """
    workers = [asyncio.create_task(work(session, q, storage)) for _ in range(workers_count)]
    producer_error = None

    try:
        await produce(request_samples, q)
    except Exception as e:
        producer_error = e
    finally:
        if not producer_error:
            # TODO или все равно обернуть в wait_for для надежности?
            await q.join()
        else:
            try:
                await asyncio.wait_for(q.join(), timeout=3)
            except asyncio.TimeoutError:
                print("Graceful shutdown не удался")

        await stop_workers(workers, q)

        try:
            await asyncio.wait_for(asyncio.gather(*workers), timeout=2.0)
        except asyncio.TimeoutError:
            await cancel_tasks(workers)
            await asyncio.gather(*workers, return_exceptions=True)


async def main():
    # Здесь лежит миллион URL
    requests_samples = [
        # ('http://some-service/getItems/', {'user_id': 100}),
        # ('http://some-service/getItems/', {'user_id': 101}),
    ]

    # Сюда складываем агрегированные результаты по каждому URL (I/O этап)
    storage: list[dict] = []

    q = asyncio.Queue(maxsize=1000)

    connector = aiohttp.TCPConnector(
        limit=600,
        limit_per_host=200,
    )

    async with aiohttp.ClientSession(connector=connector, timeout=HTTP_TIMEOUT) as session:
        await process(
            session=session,
            q=q,
            storage=storage,
            request_samples=requests_samples,
            workers_count=20,
        )

    # CPU-bound обработка storage после завершения I/O
    processed: list[int] = await asyncio.to_thread(_cpu_heavy_transform, storage)

    print(f"IO results: {len(storage)}")
    print(f"CPU results: {len(processed)}")


if __name__ == "__main__":
    asyncio.run(main())
