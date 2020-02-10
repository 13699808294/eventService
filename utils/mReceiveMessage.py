from tornado import gen


class MReceiveMessage():
    def __init__(self,client, userdata, message):
        self.mosquittoConnectObject = client
        self.mosquittoUserData = userdata
        self.topic = message.topic
        self.topicList = self.topicList()
        self.data = message.payload.decode()

    def topicList(self):
        topic_list = self.topic.split('/')
        while '' in topic_list:
            topic_list.remove('')
        return topic_list