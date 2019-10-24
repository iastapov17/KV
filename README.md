# KV

Задание:
* скачать/собрать тарантул
* запустить тестовое приложение
* реализовать kv-хранилище доступное по http
* выложить на гитхаб
* задеплоить где-нибудь в публичном облаке

#

API:
* POST /kv body: {key: "test", "value": {SOME ARBITRARY JSON}}
* PUT kv/{id} body: {"value": {SOME ARBITRARY JSON}}
* GET kv/{id}
* DELETE kv/{id}
* POST  возвращает 409 если ключ уже существует
* POST, PUT возвращают 400 если боди некорректное
* PUT, GET, DELETE возвращает 404 если такого ключа нет
* все операции логируются

Config Tarantool
```bash
$ tarantool kv.lua
```

Start server
```bash
$ python kv.py
```
