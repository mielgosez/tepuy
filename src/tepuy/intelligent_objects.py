import datetime
from typing import Union


class IntelligentObject:
    def __init__(self, name: str):
        self.__name = name

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):
        self.__name = name


class Entity(IntelligentObject):
    def __init__(self,
                 name: str,
                 creation_date: datetime.datetime = datetime.datetime.now(),
                 sort_property_value: int = 1):
        super().__init__(name=name)
        self.__creation_date = creation_date
        self.__sort_property = sort_property_value

    @property
    def creation_date(self):
        return self.__creation_date

    @property
    def sort_property(self):
        return self.__sort_property

    @sort_property.setter
    def sort_property(self, value: int):
        self.__sort_property = value


class SimQueue(IntelligentObject):
    def __init__(self,
                 name: str,
                 sorting_feature: Union[str, None] = None,
                 sorting_policy: str = 'smallest'):
        super().__init__(name=name)
        self.__sorting_feature = sorting_feature
        self.__sorting_policy = sorting_policy
        self.__content = list()

    def add_entity(self, entity: Entity):
        if self.sorting_feature is None:
            entity.sort_property = len(self.content) + 1
        self.content.append(entity)
        self.sort_queue()

    def sort_queue(self):
        reverse_list = (self.sorting_policy == 'smallest')
        if self.sorting_feature is not None:
            self.content.sort(key=lambda x: getattr(x, self.sorting_feature),
                              reverse=reverse_list)

    def print_content_names(self):
        return [item.name for item in self.content]

    @property
    def content(self):
        return self.__content

    @property
    def sorting_feature(self):
        return self.__sorting_feature

    @sorting_feature.setter
    def sorting_feature(self, value: Union[str, None]):
        self.__sorting_feature = value

    @property
    def sorting_policy(self):
        return self.__sorting_policy

    @sorting_policy.setter
    def sorting_policy(self, value: Union[str, None]):
        self.__sorting_policy = value


class SimNode(IntelligentObject):
    def __init__(self,
                 name: str,
                 position: tuple):
        super().__init__(name=name)
        self.__position = position
        self.__queue = SimQueue(name='-'.join([name, 'queue']))

    @property
    def position(self):
        return self.__position

    @position.setter
    def position(self, pos: tuple):
        self.__position = pos


class MainSimModel:
    def __init__(self):
        pass


class Resource(IntelligentObject):
    def __init__(self,
                 name: str):
        super().__init__(name=name)


class Path(IntelligentObject):
    def __init__(self,
                 name: str):
        super().__init__(name=name)


class Creator(IntelligentObject):
    def __init__(self,
                 name: str):
        super().__init__(name=name)


class Destructor(IntelligentObject):
    def __init__(self, name: str):
        super().__init__(name=name)


class TaskStation(IntelligentObject):
    def __init__(self,
                 name: str,
                 position: tuple):
        super().__init__(name=name)
        self.__name = name
        self.__position = position
        self.__input_node = SimNode(name='-'.join([name, 'node']),
                                    position=position)
        self.__processing_queue = SimQueue(name='-'.join([name, 'processing_queue']))
        self.__output_node = SimNode(name='-'.join([name, 'node']),
                                     position=position)

    @property
    def position(self):
        return self.__position

    @position.setter
    def position(self, position: tuple):
        self.__position = position

    @property
    def input_node(self):
        return self.__input_node

    @property
    def output_node(self):
        return self.__output_node

    @property
    def processing_queue(self):
        return self.__processing_queue


if __name__ == '__main__':
    e1 = Entity(name='Entity1')
    e2 = Entity(name='Entity2')
    e3 = Entity(name='Entity3')
    sim_queue = SimQueue(name='NewQueue')
    sim_queue.add_entity(e2)
    sim_queue.add_entity(e3)
    sim_queue.add_entity(e2)
    print(sim_queue.print_content_names())
