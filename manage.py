import asyncio

import uvloop
from tornado.platform.asyncio import BaseAsyncIOLoop

from apps.eventService import Event
from apps.mosquittoClient import MosquittoClient
from setting.setting import MQTT_SERVICE_HOST, ENVIRONMENT

if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())  # 修改循环策略为uvloop
    aioloop = asyncio.get_event_loop()  # 获取aioloop循环事件
    ioloop = BaseAsyncIOLoop(aioloop)  # 使用aioloop创建ioloop


    # if ENVIRONMENT == 'build' or ENVIRONMENT == 'test':
    mosquittoClient = MosquittoClient(host=MQTT_SERVICE_HOST,port=1883)
    # else:
    #     mosquittoClient = MosquittoClient(host=MQTT_SERVICE_HOST, port=18883)


    # event_object = Event(mosquittoClient=mosquittoClient)
    # mosquittoClient.event_object = event_object
    ioloop.current().start()