"""
Задача 3

дан список из 1 миллиона url, которые нужно опросить
из каждого url получить несколько item id и опросить 3 независимых сервиса c этими данными
для данных из всех 3х сервисов собрать итоговый ответ и сохранить результат в список

requests_samples = [
  ('http://some-service/getItems/', {'user_id': 100}),  # вернет {item_ids: [1, 2, 3]}
  ('http://some-service/getItems/', {'user_id': 101}),
    ...
]

service_1_url = 'http://service1/fillItems/'
service_2_url = 'http://service2/scoreItems/'
service_3_url = 'http://service3/logItems/'
"""

"""
Используется ограниченная асинхронная очередь (asyncio.Queue) и фиксированное количество воркеров.
Producer кладёт задачи в очередь, воркеры конкурентно читают из неё, выполняют I/O-операции и сохраняют результат.
"""


THREE_SERVICES = ('dasda', 'dassda', 'ewqeqwe')


import asyncio
import aiohttp
from dataclasses import dataclass
from typing import Any, Optional


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


#TODO добавить backoff
async def fetch(session: aiohttp.ClientSession, url: str, query_params: dict, timeout: int = 5) -> FetchOk :
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
            body = await resp.text()
            # TODO здесь нужно сделать разделение по статусам, но пока просто пишем одну ошибку
            raise NotOKStatus('Неудачный статус {} для такого url={} и данных {}, текст: {}'.format(status, url, query_params, body))


async def request_three_services(data: Any, session: aiohttp.ClientSession, storage: list) -> None:
    """
    Опрашивает три сервиса и кладет данные в storage
    :param data:
    :param session:
    :param storage:
    :return:
    """
    three_services_tasks = [asyncio.create_task(fetch(session, url, data)) for url in THREE_SERVICES]
    try:
        results = await asyncio.gather(*three_services_tasks, return_exceptions=True)
        for res in results:
            if not isinstance(res, BaseException):
                storage.append(res)
    # Если отменяют воркер
    except asyncio.CancelledError:
        await cancel_tasks(three_services_tasks)
        await asyncio.gather(*three_services_tasks, return_exceptions=True)
        raise
    # TODO обработать другие ошибки


async def work(session: aiohttp.ClientSession, q: asyncio.Queue, storage: list):
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
            # graceful shutdown
            return
        try:
            result_from_url = await fetch(session, item[0], item[1])
            await request_three_services(result_from_url.data, session, storage)
        # TODO нужно ли отлавливать ошибки? Надо расписать
        except asyncio.CancelledError:
            # TODO сделать лог
            raise
        # TODO Описать все остальные возможные ошибки
        finally:
            q.task_done()


async def produce(url_data: list[tuple], q: asyncio.Queue):
    """
    Наполняем очередь данными
    :param url_data:
    :param q:
    :return:
    """
    for data in url_data:
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


async def process(session: aiohttp.ClientSession, q: asyncio.Queue, storage: list, request_samples: list[tuple]):
    """
    Оркестратор

    1. Запускаем воркеры
    2. Запускаем продюсер
    3. Дожидаемся обработки всех сообщений
        - если происходят ошибки, обрабатываем их
    4. Останавливаем и закрываем воркеры
    :return:
    """
    workers = [asyncio.create_task(work(session, q, storage)) for _ in range(20)]
    producer_error = None
    try:
        await produce(request_samples, q) # продюсер можно внедрить через DI
    except Exception as e:
        producer_error = e
    finally:
        # Дожидаемся пока все сообщения обработаются воркером
        if not producer_error:
            # TODO или все равно обернуть в wait_for для надежности?
            await q.join()
        else:
            # Если была ошибка продюсера, то даем какое-то время, чтобы завершить задачи, которые попали в очередь
            try:
                await asyncio.wait_for(q.join(), timeout=3)
            except asyncio.TimeoutError:
                print('Graceful shutdown не удался')
        await stop_workers(workers, q)

        try:
            # ждём, что воркеры сами завершатся (graceful) за разумное время
            await asyncio.wait_for(asyncio.gather(*workers), timeout=2.0)
        except asyncio.TimeoutError:
            # не все успели завершиться → force shutdown
            await cancel_tasks(workers)
            res = await asyncio.gather(*workers, return_exceptions=True)

async def main():
    """
    Точка входа в приложение

    1. Формируем aiohttp.ClientSession
    2. Инициализируем хранилище для конечного результата
    3. запускаем оркестратор
    """
    # Здесь лежит миллион URLS
    requests_samples = []
    # Здесь лежит конечный результат
    storage = []
    q = asyncio.Queue(maxsize=1000)
    connector = aiohttp.TCPConnector(
        limit=600, # общий лимит одновременных TCP-соединений для коннектора.
        limit_per_host=200, # лимит соединений на один host:port.
    )
    async with aiohttp.ClientSession(
        connector=connector,
        timeout=HTTP_TIMEOUT
    ) as session:
        await process(
            session=session,
            q=q,
            storage=storage,
            request_samples=requests_samples
        )


asyncio.run(main())
