import datetime


class SimEvent:
    def __init__(self,
                 start_date: datetime.datetime,
                 end_date: datetime.datetime,
                 object_name: str,
                 action_name: str):
        self.__start_date = start_date
        self.__end_date = end_date
        self.__object_name = object_name
        self.__action_name = action_name


class SimStep:
    def __init__(self):
        pass


class SimProcess:
    def __init__(self):
        pass
