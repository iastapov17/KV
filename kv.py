from flask import Flask, request
from tarantool import Connection
import json
import datetime

app = Flask(__name__)


def logger(*args):
    with open("log", "a+") as f:
        text = datetime.datetime.today().strftime("%Y-%m-%d  %H:%M:%S")
        for i in args:
            text += "  " + str(i)
        f.write(text + '\n')
    return True


def check_body(body):
    try:
        logger("check_body", body)
        data = json.loads(body)
        logger("Result: " + str(data))
        return data
    except TypeError:
        return False


def check_key(key):
    logger("check_key", key)
    return len([c.select("KV", key)][0].data)


def get_key(data):
    logger("get_key", data)
    try:
        return data["key"]
    except KeyError:
        return False


def get_value(data):
    logger("get_value", data)
    try:
        return str(data["value"])
    except KeyError:
        return False


def put(key, value):
    logger("Put", "key: " + key, "value:" + value)
    try:
        c.update("KV", key, [('=', 1, value)])
        return True
    except:
        return False


def get(key):
    logger("Get", "key: " + key)
    try:
        return [c.select("KV", key)][0].data[0][1]
    except:
        return False


def delete(key):
    logger("Detele", "key: " + key)
    try:
        c.delete("KV", key)
        return True
    except:
        return False


def post(key, value):
    logger("Post", "key: " + key, "value:" + value)
    try:
        c.insert("KV", (key, value))
        return True
    except:
        return False


@app.route('/kv/', methods=['POST'])
def get_post():
    if request.method == 'POST':
        logger('New ' + request.method + " request")
        data = check_body(request.data)

        if not data or not isinstance(data, dict):
            logger('Bad data in POST method')
            return app.make_response(('Bad data in POST method', 400))
        key = get_key(data)
        logger(str(key))
        value = get_value(data)
        logger(str(value))
        if not key or not value:
            logger('No valid data in POST method')
            return app.make_response(('No valid data in POST method', 400))
        if not check_key(key):
            if not post(key, value):
                logger('Post Error!')
                return app.make_response(('Error!', 404))
            logger('Added!')
            return app.make_response(('Added', 200))
        else:
            logger('Key exist!')
            return app.make_response(('Key exist', 409))


@app.route('/kv/<string>', methods=['GET', 'PUT', 'DELETE'])
def get_other_methods(string):
    logger('New ' + request.method + " request")
    if request.method == 'PUT':
        data = check_body(request.data)
        if not data or not isinstance(data, dict):
            logger('Bad data in POST method')
            return app.make_response(('Bad data in POST method', 400))
        key = string
        logger("Key: " + str(key))
        value = get_value(data)
        logger(str(value))
        if not key or not value:
            logger('No data in ' + request.method + ' method')
            return app.make_response(('No data in ' + request.method + ' method', 400))
        if check_key(key):
            if not put(key, value):
                logger('Put Error!')
                return app.make_response(('Error!', 404))
            logger('Put')
            return app.make_response(('Put', 200))
        else:
            logger('Key is not exist!')
            return app.make_response(('Key is not exist', 404))

    elif request.method == 'DELETE':
        key = string
        logger("Key: " + str(key))
        if check_key(key):
            if not delete(key):
                logger('Delete Error!')
                return app.make_response(('Error!', 404))
            logger('Deleted')
            return app.make_response(('Deleted', 200))
        else:
            logger('Key is not exist!')
            return app.make_response(('Key is not exist', 404))

    elif request.method == 'GET':
        key = string
        logger("Key: " + str(key))
        if check_key(key):
            result = get(key)
            if not result:
                logger('Get Error!')
                return app.make_response(('Error!', 404))
            logger('Result: ' + result)
            return app.make_response((result, 200))
        else:
            logger('Key is not exist!')
            return app.make_response(('Key is not exist', 404))


c = Connection("127.0.0.1", 3301)
app.run(host='0.0.0.0')
