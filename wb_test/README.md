# Задание с собеседования в Wildberries
Дан список из 1 миллиона url, которые нужно опросить
из каждого url получить несколько item id и опросить 3 независимых сервиса c этими данными
для данных из всех 3х сервисов собрать итоговый ответ и сохранить результат в список.
После этого список нужно обработать, обработка элементов списка CPU-bound

```
requests_samples = [
  ('http://some-service/getItems/', {'user_id': 100}),  # вернет {item_ids: [1, 2, 3]}
  ('http://some-service/getItems/', {'user_id': 101}),
    ...
]

service_1_url = 'http://service1/fillItems/'
service_2_url = 'http://service2/scoreItems/'
service_3_url = 'http://service3/logItems/'
```