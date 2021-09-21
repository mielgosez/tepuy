import datetime
from typing import Union
import pandas as pd
from tepuy.processes import SimEvent, SimProcess, EmptyProcess
import numpy as np


class IntelligentObject:
    def __init__(self, name: str):
        self.__name = name
        self.__available_date = None

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, name):
        self.__name = name

    @property
    def available_date(self):
        return self.__available_date

    @available_date.setter
    def available_date(self, available_date: datetime.datetime):
        self.__available_date = available_date


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
                 position: tuple,
                 capacity: int = 1,
                 next_node: Union[IntelligentObject, None] = None,
                 is_destructor: bool = False):
        super().__init__(name=name)
        self.__capacity = capacity
        self.__position = position
        self.__available = True
        self.__population = []
        self.__queue = SimQueue(name='-'.join([name, 'queue']))
        self.__next_node = next_node
        self.__is_destructor = is_destructor

    def on_entered(self,
                   entity: Entity,
                   events: dict,
                   actions: SimQueue,
                   enter_date: datetime.datetime,
                   process: Union[SimProcess, None] = None):
        if process is None:
            process = EmptyProcess(name='empty_process',
                                   associated_object=entity,
                                   context_object=self)
        if self.available:
            self.population.append(entity)
            if len(self.population) == self.capacity:
                self.available = False
            process.run_process(entity=entity,
                                events=events,
                                actions=actions)
            return SimEvent(start_date=enter_date,
                            end_date=enter_date,
                            event_name=f'on_entered_{self.name}')
        else:
            self.queue.add_entity(entity)

    def on_exited(self,
                  entity: Entity,
                  events: dict,
                  actions: SimQueue,
                  exit_date: datetime.datetime,
                  process: SimProcess = EmptyProcess
                  ):
        process.run_process(entity=entity,
                            events=events,
                            actions=actions)
        self.population.remove(entity)
        self.available = True
        return SimEvent(start_date=exit_date,
                        end_date=exit_date,
                        event_name=f'on_exited_{self.name}')

    # Getters and setters
    @property
    def capacity(self):
        return self.__capacity

    @capacity.setter
    def capacity(self, new_capacity: int):
        self.__capacity = new_capacity

    @property
    def available(self):
        return self.__available

    @available.setter
    def available(self, is_available: bool):
        self.__available = is_available

    @property
    def population(self):
        return self.__population

    @property
    def position(self):
        return self.__position

    @position.setter
    def position(self, pos: tuple):
        self.__position = pos

    @property
    def queue(self):
        return self.__queue

    @property
    def next_node(self):
        return self.__next_node

    @property
    def is_destructor(self):
        return self.__is_destructor


class MainSimModel:
    def __init__(self,
                 name: str,
                 model_network: dict,
                 start_date: datetime.datetime):
        self.__name = name
        self.__history = SimQueue(name=f'history_{name}',
                                  sorting_feature='end_date',
                                  sorting_policy='greatest')
        self.__network = model_network
        self.__alerts = dict()
        self.__start_date = start_date
        self.__actions = SimQueue(name=f'actions_{name}',
                                  sorting_feature='end_date',
                                  sorting_policy='smallest')

    def run(self):
        # Look at the start of the network:
        source = self.network['start']['next']
        source.create_entities_from_arrival_table(events_dict=self.alerts,
                                                  actions_queue=self.actions)
        SimEvent(start_date=self.start_date,
                 end_date=self.start_date,
                 event_name='starting model',
                 object_name=self.name,
                 action_name='instantiate_sources'
                 )

    # Setters and getters
    @property
    def start_date(self):
        return self.__start_date

    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, new_name):
        self.__name = new_name

    @property
    def history(self):
        return self.__name

    @property
    def alerts(self):
        return self.__alerts

    @property
    def actions(self):
        return self.__actions

    @property
    def network(self):
        return self.__network


class Resource(IntelligentObject):
    def __init__(self,
                 name: str,
                 owner: Union[IntelligentObject, None] = None,
                 sorting_feature: Union[str, None] = None,
                 sorting_policy: str = 'smallest'):
        super().__init__(name=name)
        self.__seized = False
        self.__owner = owner
        self.__ride_request_queue = SimQueue(name=f'{name}_ride_request_queue',
                                             sorting_feature=sorting_feature,
                                             sorting_policy=sorting_policy)

    @staticmethod
    def seize(self):
        pass

    # Getters and setters
    @property
    def owner(self):
        return self.__owner

    @owner.setter
    def owner(self, new_owner: IntelligentObject):
        self.__owner = new_owner

    @property
    def seized(self):
        return self.__seized

    @seized.setter
    def seized(self, seize_value: bool):
        self.__seized = seize_value

    @property
    def ride_request_queue(self):
        return self.__ride_request_queue


class Path(IntelligentObject):
    def __init__(self,
                 name: str,
                 path_type: str,
                 node_from: SimNode,
                 node_to: SimNode,
                 speed: Union[float, None] = None,
                 lead_time: Union[float, None] = None,
                 weight: float = 1.0,
                 available: bool = True):
        super().__init__(name=name)
        self.valid_options = ['path_time', 'standard']
        # Validation
        if path_type not in self.valid_options:
            raise NotImplementedError(f'{path_type} not a valid path_type. '
                                      f'Valid options are: {", ".join(self.valid_options)}')
        self.__path_type = path_type
        self.__speed = speed
        self.__lead_time = lead_time
        self.__node_from = node_from
        self.__node_to = node_to
        self.__weight = weight
        self.__available = available

    # Getters and setters
    @property
    def path_type(self):
        return self.__path_type

    @path_type.setter
    def path_type(self, path_type: str):
        if path_type not in self.valid_options:
            raise NotImplementedError(f'{path_type} not a valid path_type. '
                                      f'Valid options are: {", ".join(self.valid_options)}')
        self.__path_type = path_type

    @property
    def speed(self):
        return self.__speed

    @speed.setter
    def speed(self, new_speed: float):
        self.__speed = new_speed

    @property
    def lead_time(self):
        return self.__lead_time

    @lead_time.setter
    def lead_time(self, new_lead_time: float):
        self.__lead_time = new_lead_time

    @property
    def node_from(self):
        return self.__node_from

    @node_from.setter
    def node_from(self, new_node: SimNode):
        self.__node_from = new_node

    @property
    def node_to(self):
        return self.__node_to

    @node_to.setter
    def node_to(self, new_node: SimNode):
        self.__node_to = new_node

    @property
    def available(self):
        return self.__available

    @available.setter
    def available(self, new_value: bool):
        self.__available = new_value

    @property
    def weight(self):
        return self.__weight

    @weight.setter
    def weight(self, new_weight: bool):
        self.__weight = new_weight


class Creator(IntelligentObject):
    def __init__(self,
                 name: str,
                 position: tuple,
                 arrival_type: str,
                 arrival_rate: Union[str, None],
                 arrival_table: Union[pd.DataFrame, None],
                 datetime_column: str,
                 name_column: Union[str, None],
                 ):
        super().__init__(name=name)
        self.__position = position
        self.__arrival_type = arrival_type
        self.__arrival_rate = arrival_rate
        self.__arrival_table = arrival_table
        self.__datetime_column = datetime_column
        self.__name_column = name_column
        self.__output_node = SimNode(name=f'{name}_output_node',
                                     position=position)

    def create_entities_from_arrival_table(self,
                                           events_dict: dict,
                                           actions_queue: SimQueue):
        for idx, row in self.arrival_table.iterrows():
            datetime_loc = row[self.datetime_column]
            if self.name_column is None:
                new_entity = Entity(name=f'entity_{idx}',
                                    creation_date=datetime_loc)
            else:
                new_entity = Entity(name=row[self.name_column],
                                    creation_date=datetime_loc)
            # TODO: Redefine SimEvents such that it admits a list of objects
            # TODO: Pass the action as a string of the expression to be executed.
            # TODO: Use eval to define all the variables to be used in the string expression
            # TODO: Eval the final expression
            # TODO: Update list of events
            # TODO: Make the node unavailable until on entered process is finished.
            # TODO: Make it available once it has exited the node.
            new_event = SimEvent(start_date=datetime_loc,
                                 end_date=datetime_loc,
                                 event_name='created_entity',
                                 object_name=new_entity)
            self.output_node.on_entered(entity=new_entity,
                                        enter_date=row[self.datetime_column],
                                        events=events_dict,
                                        actions=actions_queue)

    # Getters and setters
    @property
    def position(self):
        return self.__position

    @position.setter
    def position(self, new_position: tuple):
        self.__position = new_position

    @property
    def arrival_type(self):
        return self.__arrival_type

    @property
    def arrival_rate(self):
        return self.__arrival_rate

    @property
    def arrival_table(self):
        return self.__arrival_table

    @property
    def datetime_column(self):
        return self.__datetime_column

    @property
    def name_column(self):
        return self.__name_column

    @property
    def output_node(self):
        return self.__output_node


class Destructor(IntelligentObject):
    def __init__(self,
                 name:
                 str, position: tuple):
        super().__init__(name=name)
        self.__input_node = SimNode(name=f'{name}_input_node',
                                    position=position)

    @property
    def input_node(self):
        return self.__input_node


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
