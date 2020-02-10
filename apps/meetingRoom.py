import datetime

from tornado import gen

from apps.schedule import Schedule
from setting.setting import QOSLOCAL, QOS
from utils.baseAsync import BaseAsync

from utils.MysqlClient import mysqlClient
from utils.logClient import logClient
from utils.my_json import json_dumps

class MeetingRoom(BaseAsync):
    def __init__(self,guid,room_name,sin_minute,schedule_status,company_db,company_name,publish_function):
        super().__init__()
        self.guid = guid
        self.name = room_name
        try:
            sin_minute = int(sin_minute)
            self.sinMinute = sin_minute
        except:
            self.sinMinute = 0

        self.scheduleStatus = schedule_status
        self.companyDb = company_db
        self.companyName = company_name

        self.lastLastOverScheduleInfo = {}
        self.lastNowScheduleInfo = {}
        self.lastNextScheduleInfo = {}
        self.lastOtherScheduleInfo = []

        self.mediumSchedule = None
        self.scheduleList = []
        self.remainingTime = 0
        self.myPublish = publish_function
        self.ioloop.add_timeout(self.ioloop.time(),self.updateSelfSchedule)

    @gen.coroutine
    def updateSelfSchedule(self):
        self.scheduleList = []
        now_time = datetime.datetime.now()
        compare_year = now_time.year
        compare_month = now_time.month
        compare_day = now_time.day
        data = {
            'database': self.companyDb,
            'fields': ['guid', 'room_guid_id', 'title', 'schedule.start_time', 'schedule.stop_time', 'sender', 'member',
                       'event_id'],
            'eq': {
                'is_delete': False,
                'room_guid_id': self.guid
            },
            # 筛选出已结束会议排程,或刚刚结束的会议排程
            'gte': {
                'start_time': datetime.datetime(compare_year, compare_month, compare_day, 0, 0, 0)
            },
            # 筛选出当天会议排除
            'lt': {
                'start_time': datetime.datetime(compare_year, compare_month, compare_day, 23, 59, 59)
            },
            'sortInfo': [
                # {'room_guid_id': 'ASC'},
                {'start_time': 'ASC'},
            ],
        }
        msg = yield mysqlClient.tornadoSelectAll('schedule', data)
        if msg['ret'] == '0':
            for schedule_info in msg.get('msg'):
                schedule_object = Schedule(**schedule_info)
                self.scheduleList.append(schedule_object)
        else:
            yield logClient.tornadoErrorLog('获取数据库:排程信息失败')

    # todo:处理本会议室排程
    @gen.coroutine
    def handleSelfSchedule(self,update_schedule=None):
        if update_schedule:
            yield self.updateSelfSchedule()
        now_time = datetime.datetime.now().replace(second=0, microsecond=0)
        yield self.checkSelfScheduleStatus(now_time)

    # todo:检查排程状态,统计排程信息
    @gen.coroutine
    def checkSelfScheduleStatus(self,now_time):
        status_list = []

        lastOverScheduleInfo = {}
        nowScheduleInfo = {}
        nextScheduleInfo = {}
        otherScheduleInfo = []

        for schedule_object in self.scheduleList:
            now_status = 0
            # 检查排程状态
            if schedule_object.timeout == 0:
                start_time = datetime.datetime.strptime(schedule_object.startTime, "%Y-%m-%d %H:%M:%S").replace(second=0,microsecond=0)
                stop_time = datetime.datetime.strptime(schedule_object.stopTime, "%Y-%m-%d %H:%M:%S").replace(second=0,microsecond=0)
                # 还没到开始时间   --  (大于等于才算开始)
                if now_time < start_time:  # 当前时间小于开始时间
                    if (start_time - now_time).seconds <= self.sinMinute * 60:  # 当前时间小于开始时间,但到开始时间小于签到时间
                        now_status = 1  # 准备中
                elif now_time >= start_time:  # 当前时间大于开始时间
                    if now_time == stop_time:  # 当前时间等于结束时间
                        # 会议结束,计算会议室工作时间
                        time = (stop_time - start_time).seconds // 60
                        yield self.storgeSelfScheduleWrokTime(time)
                        schedule_object.timeout = 1
                    elif now_time > stop_time:  # 当前时间大于开始时间
                        # 标注改会议已经结束
                        schedule_object.timeout = 1
                    elif now_time < stop_time:  # 当前时间小于结束时间,且大于开始时间
                        now_status = 2  # 进行中
                        self.mediumSchedule = schedule_object
                        # 计算排程剩余时间
                        remaining_time = int((stop_time - now_time).seconds / 60)
                        yield self.updateSelfScheduleRemainingTime(remaining_time,schedule_object.guid)
                status_list.append(now_status)

            # 取出排程状态
            if schedule_object.timeout:
                lastOverScheduleInfo = yield schedule_object.getSelfInfo()
                continue
            if not nowScheduleInfo:
                begin_time = datetime.datetime.strptime(schedule_object.startTime, "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                if begin_time < now_time or (begin_time - now_time).seconds <= self.sinMinute * 60:
                    nowScheduleInfo = yield schedule_object.getSelfInfo()
                    continue
            if not nextScheduleInfo:
                nextScheduleInfo = yield schedule_object.getSelfInfo()
                continue
            other_schedule_info = yield schedule_object.getSelfInfo()
            otherScheduleInfo.append(other_schedule_info)


        # 筛选排除状态
        if not status_list:
            new_schedule_status = 0
        else:
            status_list = set(status_list)
            if 2 in status_list:
                new_schedule_status = 2  # 进行中
            elif 1 in status_list:
                new_schedule_status = 1  # 准备中
            else:
                new_schedule_status = 0  # 空闲
        #
        if self.lastLastOverScheduleInfo != lastOverScheduleInfo \
                or self.lastNowScheduleInfo != nowScheduleInfo \
                or self.lastNextScheduleInfo != nextScheduleInfo \
                or self.lastOtherScheduleInfo != otherScheduleInfo \
                or self.scheduleStatus != new_schedule_status:

            yield self.updateSelfScheduleStatus(new_schedule_status)
            self.lastLastOverScheduleInfo = lastOverScheduleInfo
            self.lastNowScheduleInfo = nowScheduleInfo
            self.lastNextScheduleInfo = nextScheduleInfo
            self.lastOtherScheduleInfo = otherScheduleInfo
            info = yield self.getSelfScheduleInfo()

            topic = '/aaiot/{}/send/controlbus/event/schedule/schedule_info/0'.format(self.guid)
            yield self.myPublish(topic, json_dumps(info), qos=QOSLOCAL)
            topic = '/aaiot/{}/send/controlbus/event/schedule/schedule_info/1'.format(self.guid)
            yield self.myPublish(topic, json_dumps(info), qos=QOS)
            yield logClient.tornadoInfoLog('会议室:({})排除信息变更为:{}'.format(self.name, json_dumps(info)))




    # todo:获取排程进行时间
    @gen.coroutine
    def getSelfRemainingTime(self):
        info = {
            'remaining_time': self.remainingTime,
        }
        return info

    # todo:获取排程信息
    @gen.coroutine
    def getSelfScheduleInfo(self):

        info = {
            'schedule_status': self.scheduleStatus,
            'last_over_schedule':self.lastLastOverScheduleInfo,
            'now_schedule':  self.lastNowScheduleInfo,
            'next_schedule': self.lastNextScheduleInfo,
            'other_schedule': self.lastOtherScheduleInfo,
        }
        schedule_count = 0
        if self.lastNowScheduleInfo:
            schedule_count += 1
        if self.lastNextScheduleInfo:
            schedule_count += 1
        if self.lastOtherScheduleInfo:
            schedule_count += len(self.lastOtherScheduleInfo)
        info['schedule_count'] = schedule_count
        return info

    # todo:结束当前排程
    @gen.coroutine
    def overNowSchedule(self):
        if self.scheduleStatus == 2:
            if self.mediumSchedule == None:
                return
            schedule_guid = self.mediumSchedule.guid
            yield self.overSchedule(schedule_guid)
            yield self.handleSelfSchedule(update_schedule=1)

    # todo:结束某个排程
    @gen.coroutine
    def overSchedule(self, schedule_guid):
        data = {
            'database': self.companyDb,
            'msg': {
                'update_time': datetime.datetime.now(),
                'stop_time': datetime.datetime.now(),
            },
            'eq': {
                'guid': schedule_guid,
            }
        }
        msg = mysqlClient.updateMany('schedule', data)
        if msg['ret'] == '0':
            pass
        else:
            yield logClient.tornadoErrorLog('删除记录失败:{}'.format('schedule'))

    # todo：排除时间统计
    @gen.coroutine
    def scheduleTimeStatistics(self,now_time):
        '''
            只计算排程开始前,开始后,结束后
        '''
        schedule_time_info = {}
        if self.lastOverSchedule:
            stop_time = datetime.datetime.strptime(self.lastOverSchedule.stopTime, "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
            over_after = (now_time - stop_time).seconds // 60  # 排程结束了多少分钟
            schedule_time_info['over_after'] = {'time': over_after, 'schedule_guid': self.lastOverSchedule.guid}
        else:
            schedule_time_info['over_after'] = {'time': 480, 'schedule_guid': 'there is no schedule'}
        if self.scheduleStatus == 0 or self.scheduleStatus == 1:  # 空闲或准备时计算开始前
            if self.nowSchedule:
                start_time = datetime.datetime.strptime(self.nowSchedule.startTime, "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                if now_time > start_time:
                    start_before = (now_time - start_time).seconds // 60  # 排程结束了多少分钟
                else:
                    start_before = (start_time - now_time).seconds // 60  # 排程结束了多少分钟
                schedule_time_info['start_before'] = {'time': start_before, 'schedule_guid': self.nowSchedule.guid}
            elif self.nextSchedule:
                start_time = datetime.datetime.strptime(self.nextSchedule.startTime, "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                start_before = (start_time - now_time).seconds // 60
                schedule_time_info['start_before'] = {'time': start_before, 'schedule_guid': self.nextSchedule.guid}
            elif self.otherSchedule:
                start_time = datetime.datetime.strptime(self.otherSchedule[0].startTime,"%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
                start_before = (start_time - now_time).seconds // 60
                schedule_time_info['start_before'] = {'time': start_before,'schedule_guid': self.otherSchedule[0].guid}
        elif self.scheduleStatus == 2:  # 进行时计算开始后
            start_time = datetime.datetime.strptime(self.nowSchedule.startTime, "%Y-%m-%d %H:%M:%S").replace(second=0, microsecond=0)
            start_after = (now_time - start_time).seconds // 60  # 排程结束了多少分钟
            schedule_time_info['start_after'] = {'time': start_after, 'schedule_guid': self.nowSchedule.guid}

        topic = '/aaiot/{}/send/controlbus/event/schedule/schedule_time/0'.format(self.guid)
        yield logClient.tornadoInfoLog('會議室:({}),排程時間變更爲:{}'.format(self.name, json_dumps(schedule_time_info)))
        yield self.myPublish(topic, json_dumps(schedule_time_info), qos=QOSLOCAL)

    # todo:更新排程状态
    @gen.coroutine
    def updateSelfScheduleStatus(self,new_schedule_status):
        if self.scheduleStatus != new_schedule_status:
            last_status = self.scheduleStatus
            self.scheduleStatus = new_schedule_status
            yield self.storgeSelfScheduleStatus(new_schedule_status)
            if last_status == 2 and new_schedule_status == 0:
                topic = '/aaiot/{}/send/controlbus/event/schedule/remaining_time/{}'.format(self.guid, 0)
                yield self.updateSelfScheduleRemainingTime(0,None)
                yield self.myPublish(topic, '', qos=QOSLOCAL)
                yield logClient.tornadoInfoLog('会议室:({})进行中排除时间变更为:{}'.format(self.name, 0))

    # todo：更新排除剩余时间
    @gen.coroutine
    def updateSelfScheduleRemainingTime(self,remaining_time,schedule_guid):
        try:
            remaining_time = int(remaining_time)
        except:
            return
        if self.remainingTime != remaining_time:
            self.remainingTime = remaining_time
            msg = {
                'schedule_guid':schedule_guid
            }
            topic = '/aaiot/{}/send/controlbus/event/schedule/remaining_time/{}'.format(self.guid,remaining_time)
            yield self.myPublish(topic, msg, qos=QOSLOCAL)
            yield logClient.tornadoInfoLog('会议室:({})进行中排除时间变更为:{}'.format(self.name, remaining_time))

    # todo:存储排程状态
    @gen.coroutine
    def storgeSelfScheduleStatus(self, new_status):
        # 更新数据库排程状态
        data = {
            'database': self.companyDb,
            'msg': {
                'update_time': datetime.datetime.now(),
                'status_group_guid_id': new_status,
                'is_delete': False,
            },
            'eq': {
                'guid': self.guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_meeting_room', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_meeting_room'))

    # todo:存储排程工作时间
    @gen.coroutine
    def storgeSelfScheduleWrokTime(self,time: int = None):
        data = {
            'database': self.companyDb,
            'fields': ['total_work_hour'],
            'eq': {
                'guid': self.guid,
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
            'database': self.companyDb,
            'msg': {
                'update_time': datetime.datetime.now(),
                'total_work_hour': new_total_work_hour,
                'is_delete': False,
            },
            'eq': {
                'guid': self.guid,
            }
        }
        msg = yield mysqlClient.tornadoUpdateMany('d_meeting_room', data)
        if msg['ret'] != '0':
            yield logClient.tornadoErrorLog('数据库更新失败:{}'.format('d_meeting_room'))