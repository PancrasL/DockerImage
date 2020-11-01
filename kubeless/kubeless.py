#!/usr/bin/env python

import os
import imp
import datetime

from multiprocessing import Process, Queue
import queue
import bottle
import prometheus_client as prom

import requests
import json
import ahttp
import asyncio

mod = imp.load_source('function',
                      '/kubeless/%s.py' % os.getenv('MOD_NAME'))
func = getattr(mod, os.getenv('FUNC_HANDLER'))
func_port = os.getenv('FUNC_PORT', 8080)

timeout = float(os.getenv('FUNC_TIMEOUT', 180))

app = application = bottle.app()

func_hist = prom.Histogram('function_duration_seconds',
                           'Duration of user function in seconds',
                           ['method'])
func_calls = prom.Counter('function_calls_total',
                           'Number of calls to user function',
                          ['method'])
func_errors = prom.Counter('function_failures_total',
                           'Number of exceptions in user function',
                           ['method'])

function_context = {
    'function-name': func,
    'timeout': timeout,
    'runtime': os.getenv('FUNC_RUNTIME'),
    'memory-limit': os.getenv('FUNC_MEMORY_LIMIT'),
}

def funcWrap(q, event, c):
    try:
        q.put(func(event, c))
    except Exception as inst:
        q.put(inst)

@app.get('/healthz')
def healthz():
    return 'OK'

@app.get('/metrics')
def metrics():
    bottle.response.content_type = prom.CONTENT_TYPE_LATEST
    return prom.generate_latest(prom.REGISTRY)


@app.route('/<:re:.*>', method=['GET', 'POST', 'PATCH', 'DELETE'])
def handler():
    # 在当前线程创建协程的事件循环
    asyncio.set_event_loop(asyncio.new_event_loop())

    req = bottle.request

    ndp_type = req.forms.get('ndp_type')

    # 如果是GET请求，则通过proxy_link获取对象数据
    if ndp_type == 'GET':
        obj_urls = json.loads(req.forms.get('proxy_link'))

        # 发送head请求，获取对象的元数据
        head_requests = [ahttp.head(url) for url in obj_urls]
        head_responses = ahttp.run(head_requests)

        # 按照时间戳进行降序排序，以获取最新的对象
        head_responses = sorted(head_responses, key=lambda k: float(
            k.headers['X-Timestamp']), reverse=True)

        # 获取最新的对象数据
        get_response = requests.get(head_responses[0].url)
        data = get_response.content
    elif ndp_type == 'PUT':
        data = req.forms.get('obj_data')
    else:
        return bottle.HTTPError(408, "Unsupported method")
    event = {
        'data': data,
        'event-id': req.get_header('event-id'),
        'event-type': req.get_header('event-type'),
        'event-time': req.get_header('event-time'),
        'event-namespace': req.get_header('event-namespace'),
        'extensions': {
            'request': req
        }
    }
    method = req.method
    func_calls.labels(method).inc()
    with func_errors.labels(method).count_exceptions():
        with func_hist.labels(method).time():
            q = Queue()
            p = Process(target=funcWrap, args=(q, event, function_context))
            p.start()

            try:
                res = q.get(block=True, timeout=timeout)
            except queue.Empty:
                p.terminate()
                p.join()
                return bottle.HTTPError(408, "Timeout while processing the function")
            else:
                if isinstance(res, Exception):
                    raise res
                if ndp_type == 'PUT':
                    headers = json.loads(req.forms.get('headers'))
                    obj_urls = json.loads(req.forms.get('proxy_link'))
                    index = 0
                    put_requests = []
                    for url in obj_urls:
                        put_requests.append(
                            ahttp.put(url, data=res, headers=headers[index]))
                        #result = requests.put(url=url, data=data, headers=headers[index])
                        index += 1
                    put_responses = ahttp.run(put_requests)
                    #pprint(put_responses)
                    print(res)
                    return 'PUT OK\n'
                return res


if __name__ == '__main__':
    import logging
    import sys
    import requestlogger
    loggedapp = requestlogger.WSGILogger(
        app,
        [logging.StreamHandler(stream=sys.stdout)],
        requestlogger.ApacheFormatter())
    bottle.run(loggedapp, server='cherrypy', host='0.0.0.0', port=func_port)