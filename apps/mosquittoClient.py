import datetime

from tornado import gen

from apps.meetingRoom import MeetingRoom
from utils.bMosquittoClient import BMosquittoClient
from setting.setting import DEBUG, DATABASES, QOSLOCAL, QOS

from utils.logClient import logClient
from utils.my_json import json_dumps
from utils.MysqlClient import mysqlClient

class MosquittoClient(BMosquittoClient):
    def __init__(self,*args,**kwargs):
        super().__init__(*args,**kwargs)
        self.heartTopic = '/aaiot/eventService/send/controlbus/system/heartbeat'
        self.heartInterval = 15
        self.meetingRoomDict = {}
        self.ioloop.add_timeout(self.ioloop.time(),self.initMeetingRoom)


    @gen.coroutine
    def initMeetingRoom(self):
        for DATABASE in DATABASES:
            db = DATABASE['name']
            # 获取公司名称
            if db == 'aura':
                db_name = 'default'
            else:
                db_name = db
            data = {
                'database': db,
                'fields': ['name'],
                'eq': {
                    'database_name': db_name
                }
            }
            msg = yield mysqlClient.tornadoSelectOnly('d_company', data)
            if msg['ret'] == '0' and msg['lenght'] == 1:
                company_name = msg['msg'][0]['name']
            else:
                yield logClient.tornadoErrorLog('数据库:{},查询错误:({})'.format(db, 'd_company'))
                continue

            data = {
                'database': db,
                'fields': ['guid','room_name','sin_minute','status_group_guid_id as schedule_status',],
                'eq': {
                    'is_delete': False,
                    # 'guid':'ec5eadb8-9744-11e9-922b-5254002d0365',
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_meeting_room', data)
            if msg['ret'] == '0':
                room_list = msg['msg']
            else:
                yield logClient.tornadoErrorLog('获取数据库:排程信息失败')
                continue
            for room_info in room_list:
                room_info['company_db'] = db
                room_info['company_name'] = company_name
                room_info['publish_function'] = self.myPublish
                meeting_room_object = MeetingRoom(**room_info)
                self.meetingRoomDict[meeting_room_object.guid] = meeting_room_object
        else:
            yield logClient.tornadoDebugLog('会议室更新完成,开始分钟事件')
            self.ioloop.add_timeout(self.ioloop.time(), self.minuteTask)

    @gen.coroutine
    def minuteTask(self):
        # todo: time事件
        yield logClient.tornadoDebugLog('一分钟事件触发')
        self.ioloop.add_timeout(self.ioloop.time() // 60 * 60 + 60, self.minuteTask)

        now_ = datetime.datetime.now()
        compare_hour = now_.hour
        compare_minute = now_.minute
        # compare_second = now_.second
        # compare_year = now_.year
        # compare_month = now_.month
        # compare_day = now_.day
        topic = '/aaiot/0/send/controlbus/event/time/{}/{}'.format(compare_hour, compare_minute)
        if compare_hour == 0 and compare_minute == 0:
            update_schedule_flag = 1
        else:
            update_schedule_flag = 0
        yield self.myPublish(topic, '', qos=QOSLOCAL)

        for meeting_room_guid,meeting_room_object in self.meetingRoomDict.items():
            yield meeting_room_object.handleSelfSchedule(update_schedule=update_schedule_flag)


    @gen.coroutine
    def handleOnMessage(self,ReceiveMessageObject):
        topic_list = ReceiveMessageObject.topicList
        meeting_room_guid = topic_list[1]
        if topic_list[2] == 'receive':
            # 其他服务主动获取--会议室排程信息[当前会议,下次会议,会议室排程状态]
            if topic_list[6] == 'schedule_info':
                get_from = topic_list[7]
                topic = '/aaiot/{}/send/controlbus/event/schedule/schedule_info/{}'.format(meeting_room_guid,get_from)
                meeting_room_object = self.meetingRoomDict.get(meeting_room_guid)
                if meeting_room_object != None:
                    info = yield meeting_room_object.getSelfScheduleInfo()
                    yield logClient.tornadoInfoLog('会议室:{}.获取排程信息'.format(meeting_room_object.name))
                    if get_from == 0 or get_from == '0':
                        yield self.myPublish(topic,json_dumps(info),qos=QOSLOCAL)

                    elif get_from == 1 or get_from == '1':
                        yield self.myPublish(topic, json_dumps(info), qos=QOS)
                else:
                    return
            # 其他服务主动获取--会议室排程剩余时间
            elif topic_list[6] == 'remaining_time':
                topic = '/aaiot/{}/send/controlbus/event/schedule/remaining_time/0'.format(meeting_room_guid)
                meeting_room_object = self.meetingRoomDict.get(meeting_room_guid)
                if meeting_room_object != None:
                    yield logClient.tornadoInfoLog('会议室:{}.获取排程剩余时间信息'.format(meeting_room_object.name))
                    info = yield meeting_room_object.getSelfRemainingTime()
                    yield self.myPublish(topic,json_dumps(info),qos=QOSLOCAL)
            # 取消排程
            elif topic_list[6] == 'cancel':
                meeting_room_object = self.meetingRoomDict.get(meeting_room_guid)
                if meeting_room_object == None:
                    return
                yield meeting_room_object.overNowSchedule()
        # 监控到会议室排程信息发生改变
        elif topic_list[2] == 'send':
            print(ReceiveMessageObject.topic)
            if topic_list[6] == 'add' or topic_list[6] == 'change' or topic_list[6] == 'remove':
                meeting_room_object = self.meetingRoomDict.get(meeting_room_guid)
                if meeting_room_object != None:
                    yield gen.sleep(5)
                    yield meeting_room_object.handleSelfSchedule(update_schedule=1)

    @gen.coroutine
    def handle_on_connect(self):
        topic = '/aaiot/mqttService/receive/controlbus/system/heartbeat'
        qos_type = QOSLOCAL
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))

        topic = '/aaiot/+/receive/controlbus/event/schedule/schedule_info/0'
        qos_type = QOSLOCAL
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))

        topic = '/aaiot/+/receive/controlbus/event/schedule/remaining_time/0'
        qos_type = QOSLOCAL
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))

        topic = '/aaiot/+/receive/controlbus/event/schedule/cancel/0'
        qos_type = QOSLOCAL
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))

        topic = '/aaiot/+/send/controlbus/event/schedule/add'
        qos_type = QOSLOCAL
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))

        topic = '/aaiot/+/send/controlbus/event/schedule/remove'
        qos_type = QOSLOCAL
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))

        topic = '/aaiot/+/send/controlbus/event/schedule/change'
        qos_type = QOSLOCAL
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))

        topic = '/aaiot/+/receive/controlbus/event/schedule/schedule_info/1'
        qos_type = QOS
        self.connectObject.subscribe(topic, qos=qos_type)
        yield logClient.tornadoInfoLog('添加订阅主题为:{},订阅等级为:{}'.format(topic, qos_type))


