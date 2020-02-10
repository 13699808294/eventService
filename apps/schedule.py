from tornado import gen

from utils.baseAsync import BaseAsync


class Schedule(BaseAsync):
    def __init__(self,guid, room_guid_id,title, start_time, stop_time,sender,member,event_id):
        super().__init__()
        self.guid = guid
        self.meetingRoomGuid =  room_guid_id
        self.title = title
        self.startTime = start_time
        self.stopTime = stop_time
        self.sender = sender
        self.member = member
        self.eventId = event_id
        self.timeout = 0


    @gen.coroutine
    def getSelfInfo(self):
        info = {
            'guid':self.guid,
            'room_guid_id':self.meetingRoomGuid,
            'title':self.title,
            'start_time':self.startTime,
            'stop_time':self.stopTime,
            'sender':self.sender,
            'member':self.member,
            'event_id':self.eventId,
        }
        return info