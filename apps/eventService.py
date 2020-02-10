import asyncio
import datetime
import uvloop
from tornado import gen
from tornado.platform.asyncio import BaseAsyncIOLoop
from setting.setting import DATABASES, QOSLOCAL, QOS

from utils.MysqlClient import mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps

from utils.baseAsync import BaseAsync


class Event(BaseAsync):
    def __init__(self,mosquittoClient=None):
        super().__init__()
        self.mosquittoClient = mosquittoClient
        if self.mosquittoClient == None:
            raise Exception('mosquitto is None')
        self.room_dict = {}
        self.ioloop.add_timeout(self.ioloop.time(), self.initAllMeetingRoom)

    @gen.coroutine
    def initAllMeetingRoom(self):
        '''获取所有的会议室,更新全局变量meeting_room_list'''
        # 获取所有的会议室
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
                    'guid':'ec5eadb8-9744-11e9-922b-5254002d0365',
                },
            }
            msg = yield mysqlClient.tornadoSelectAll('d_meeting_room', data)
            if msg['ret'] == '0':
                room_list = msg['msg']
            else:
                yield logClient.tornadoErrorLog('获取数据库:排程信息失败')
                continue
            now_time = datetime.datetime.now()
            compare_year = now_time.year
            compare_month = now_time.month
            compare_day = now_time.day
            compare_hour = now_time.hour
            compare_minute = now_time.minute

            for room_info in room_list:
                room_info['company_db'] = db
                room_info['company_name'] = company_name
                meeting_room_guid = room_info.get('guid')
                room_info['schedule_list'] = yield self.getMeetingRoomSchedule(compare_year,compare_month,compare_day,compare_hour,compare_minute,meeting_room_guid,db)
                if meeting_room_guid == None:
                    continue
                self.room_dict[meeting_room_guid] = room_info
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
        compare_year = now_.year
        compare_month = now_.month
        compare_day = now_.day
        topic = '/aaiot/0/send/controlbus/event/time/{}/{}'.format(compare_hour, compare_minute)
        if compare_hour == 0 and compare_minute == 0:
            update_schedule_flag = 1
        else:
            update_schedule_flag = 0
        self.mosquittoClient.myPublish(topic, '', qos=QOSLOCAL)

        for meeting_room_guid in self.room_dict.keys():
            yield self.checkMeetingRoomSchedule(compare_year,compare_month,compare_day,compare_hour,compare_minute,meeting_room_guid,update_schedule=update_schedule_flag)

    @gen.coroutine
    def getMeetingRoomSchedule(self,compare_year,compare_month,compare_day,compare_hour,compare_minute,meeting_room_guid,db):
        # 检验会议室会议状态
        data = {
            'database': db,
            'fields': ['guid', 'room_guid_id','title', 'schedule.start_time', 'schedule.stop_time','sender','member','event_id'],
            'eq': {
                'is_delete': False,
                'room_guid_id': meeting_room_guid
            },
            # 筛选出已结束会议排程,或刚刚结束的会议排程
            'gte': {
                'start_time':datetime.datetime(compare_year, compare_month, compare_day, 0, 0, 0)
            },
            # 筛选出当天会议排除
            'lt': {
                'start_time': datetime.datetime(compare_year, compare_month, compare_day, 23, 59, 59)
            },
            'sortInfo': [
                {'room_guid_id': 'ASC'},
                {'start_time': 'ASC'},
            ],
        }
        msg = yield mysqlClient.tornadoSelectAll('schedule', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('获取数据库:排程信息失败')
            return []
        return msg['msg']

    @gen.coroutine
    def checkMeetingRoomSchedule(self,compare_year,compare_month,compare_day,compare_hour,compare_minute,meeting_room_guid,update_schedule=None):

        meeting_room_info = self.room_dict.get(meeting_room_guid)
        if meeting_room_info == None:
            return
        db = meeting_room_info.get('company_db')
        room_name = meeting_room_info.get('room_name')
        company_name = meeting_room_info.get('company_name')
        meeting_room_guid = meeting_room_info.get('guid')
        sin_minute = meeting_room_info.get('sin_minute', 0)
        try:
            sin_minute = int(sin_minute)
        except:
            sin_minute = 0

        if update_schedule:
            schedule_list = yield self.getMeetingRoomSchedule(compare_year, compare_month, compare_day, compare_hour,compare_minute, meeting_room_guid, db)
            meeting_room_info['schedule_list'] = schedule_list
            return
        else:
            schedule_list = meeting_room_info['schedule_list']
        status_list = []
        now_time = datetime.datetime.now().replace(second=0, microsecond=0)
        # last_schedule_index = 0
        # schedule_over_count = 0
        for schedule_info in schedule_list:
            now_status = 0
            start_time = datetime.datetime.strptime(schedule_info['start_time'], "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
            stop_time = datetime.datetime.strptime(schedule_info['stop_time'], "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
            # 还没到开始时间   --  (大于等于才算开始)
            if now_time < start_time:                                       #当前时间小于开始时间
                if (start_time - now_time).seconds <= sin_minute * 60:      #当前时间小于开始时间,但到开始时间小于签到时间
                    now_status = 1  #准备中
            elif now_time >= start_time:                                    #当前时间大于开始时间
                if now_time == stop_time:                                   #当前时间等于结束时间
                    #会议结束,计算会议室工作时间
                    time = (stop_time - start_time).seconds//60
                    yield self.updateMeetingRoomWrokTime(db, meeting_room_guid, time)
                    schedule_info['timeout'] = 1
                    # last_schedule_index = schedule_over_count
                    # schedule_over_count += 1
                elif now_time > stop_time:                                  #当前时间大于开始时间
                    #标注改会议已经结束
                    schedule_info['timeout'] = 1
                    # last_schedule_index = schedule_over_count
                    # schedule_over_count += 1
                elif now_time < stop_time:                                  #当前时间小于结束时间,且大于开始时间
                    now_status = 2  # 进行中
                    #计算排程剩余时间
                    remaining_time = int((stop_time-now_time).seconds/60)
                    meeting_room_info['remaining_time'] = remaining_time
                    topic = '/aaiot/{}/send/controlbus/event/schedule/remaining_time/{}'.format(meeting_room_guid, remaining_time)
                    self.mosquittoClient.myPublish(topic, '', qos=QOSLOCAL)
                    yield logClient.tornadoInfoLog('会议室:({})进行中排除时间变更为:{}'.format(room_name,remaining_time),company=company_name)
            status_list.append(now_status)
        #筛选排除状态
        if not status_list:
            new_status = 0
        else:
            status_list = set(status_list)
            if 2  in status_list:   new_status = 2  #进行中
            elif 1 in status_list:  new_status = 1  #准备中
            else:                   new_status = 0  #空闲
        #判断会议室状态更改
        old_status = meeting_room_info.get('schedule_status')
        if old_status != new_status:
            yield self.storgeMeetingRoomScheduleStatus(meeting_room_guid,db,new_status)
            #如果会议室原来状态为进行中,修改会议室
            if old_status == 2 and new_status == 0:
                topic = '/aaiot/{}/send/controlbus/event/schedule/remaining_time/{}'.format(meeting_room_guid,0)
                meeting_room_info['remaining_time'] = 0
                self.mosquittoClient.myPublish(topic, '', qos=QOSLOCAL)
                yield logClient.tornadoInfoLog('会议室:({})进行中排除时间变更为:{}'.format(room_name, 0),company=company_name)
        #排程信息变更检测
        # valid_schedule_list = schedule_list[last_schedule_index:]
        last_over_schedule = {}
        now_schedule = {}
        next_schedule = {}
        other_schedule = []
        for schedule_info in schedule_list:
            if schedule_info.get('timeout'):
                last_over_schedule = schedule_info
                continue
            if not now_schedule:
                begin_time = datetime.datetime.strptime(schedule_info.get('start_time'), "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                if begin_time < now_time or (begin_time - now_time).seconds <= sin_minute * 60:
                    now_schedule = schedule_info
                    continue
            if not next_schedule:
                next_schedule = schedule_info
                continue
            # if not other_schedule:
            other_schedule.append(schedule_info)
        '''
            只计算排程开始前,开始后,结束后
        '''
        schedule_time_info = {}
        if last_over_schedule:
            stop_time = datetime.datetime.strptime(last_over_schedule.get('stop_time'), "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
            over_after = (now_time-stop_time).seconds//60   #排程结束了多少分钟
            schedule_time_info['over_after'] = {'time':over_after,'schedule_guid':last_over_schedule.get('guid')}
            # yield logClient.tornadoDebugLog('排程结束后{}分钟'.format(over_after))
        else:
            schedule_time_info['over_after'] = {'time': 480, 'schedule_guid': 'there is no schedule'}
            # yield logClient.tornadoDebugLog('排程结束后{}分钟'.format(480))

        if new_status == 0 or new_status == 1:    #空闲或准备时计算开始前
            if now_schedule:
                start_time = datetime.datetime.strptime(now_schedule.get('start_time'),"%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                if now_time > start_time:
                    start_before = (now_time - start_time).seconds // 60  # 排程结束了多少分钟
                else:
                    start_before = (start_time - now_time).seconds // 60  # 排程结束了多少分钟
                schedule_time_info['start_before'] = {'time':start_before,'schedule_guid':now_schedule.get('guid')}
                # yield logClient.tornadoDebugLog('排程开始前{}分钟'.format(start_before))
            elif next_schedule:
                start_time = datetime.datetime.strptime(next_schedule.get('start_time'),"%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                start_before = (start_time - now_time).seconds // 60
                schedule_time_info['start_before'] = {'time':start_before,'schedule_guid':next_schedule.get('guid')}
                # yield logClient.tornadoDebugLog('排程开始前{}分钟'.format(start_before))
            elif other_schedule:
                start_time = datetime.datetime.strptime(other_schedule[0].get('start_time'), "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                start_before = (start_time - now_time).seconds // 60
                schedule_time_info['start_before'] = {'time':start_before,'schedule_guid':other_schedule[0].get('guid')}
                # yield logClient.tornadoDebugLog('排程开始前{}分钟'.format(start_before))
        elif new_status == 2:   #进行时计算开始后
            start_time = datetime.datetime.strptime(now_schedule.get('start_time'), "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
            start_after = (now_time - start_time).seconds // 60  # 排程结束了多少分钟
            schedule_time_info['start_after'] = {'time':start_after,'schedule_guid':now_schedule.get('guid')}
            # yield logClient.tornadoDebugLog('排程开始后{}分钟'.format(start_after))

        old_now_schedule = meeting_room_info.get('now_schedule')
        old_next_schedule = meeting_room_info.get('next_schedule')
        old_other_schedule = meeting_room_info.get('other_schedule')
        old_status = meeting_room_info.get('schedule_status')
        if old_now_schedule != now_schedule or old_next_schedule != next_schedule or old_status != new_status or other_schedule != old_other_schedule:
            meeting_room_info['schedule_status'] = new_status
            data = {
                'schedule_status': new_status,
                'now_schedule':now_schedule,
                'other_schedule':other_schedule,
                'next_schedule':next_schedule,
            }
            meeting_room_info['next_schedule'] = next_schedule
            meeting_room_info['now_schedule'] = now_schedule
            meeting_room_info['other_schedule'] = other_schedule
            topic = '/aaiot/{}/send/controlbus/event/schedule/schedule_info/0'.format(meeting_room_guid)
            self.mosquittoClient.myPublish(topic, json_dumps(data), qos=QOSLOCAL)
            topic = '/aaiot/{}/send/controlbus/event/schedule/schedule_info/1'.format(meeting_room_guid)
            self.mosquittoClient.myPublish(topic, json_dumps(data), qos=QOS)
            yield logClient.tornadoInfoLog('会议室:({})排除信息变更为:{}'.format(room_name,json_dumps(data)),company=company_name)
        topic = '/aaiot/{}/send/controlbus/event/schedule/schedule_time/0'.format(meeting_room_guid)
        yield logClient.tornadoInfoLog('會議室:({}),排程時間變更爲:{}'.format(room_name,json_dumps(schedule_time_info)),company=company_name)
        self.mosquittoClient.myPublish(topic, json_dumps(schedule_time_info), qos=QOSLOCAL)
    @gen.coroutine
    def storgeMeetingRoomScheduleStatus(self,meeting_room_guid,db,new_status):
        # 更新数据库排程状态
        data = {
            'database': db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'status_group_guid_id': new_status,
                'is_delete': False,
            },
            'eq': {
                'guid': meeting_room_guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_meeting_room', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_meeting_room'))

    @gen.coroutine
    def updateMeetingRoomWrokTime(self,db,meeting_room_guid,time:int=None):
        data = {
            'database': db,
            'fields':['total_work_hour'],
            'eq': {
                'guid': meeting_room_guid,
            }
        }
        msg = yield mysqlClient.tornadoSelectOne('d_meeting_room', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_meeting_room'))
        meeting_room_info = msg['msg']
        if meeting_room_info == None:
            yield logClient.tornadoErrorLog('数据库查询失败:{}'.format('d_meeting_room'))
        last_total_work_hour = meeting_room_info.get('total_work_hour')
        try:
            time = int(time)
        except:
            return
        new_total_work_hour = last_total_work_hour + time
        data = {
            'database': db,
            'msg': {
                'update_time': datetime.datetime.now(),
                'total_work_hour': new_total_work_hour,
                'is_delete': False,
            },
            'eq': {
                'guid': meeting_room_guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_meeting_room', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_meeting_room'))