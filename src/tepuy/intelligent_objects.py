import datetime
from typing import Union
import pandas as pd
from tepuy.processes import SimEvent, SimProcess, EmptyProcess
import numpy as np


class IntelligentObject:
    def __init__(self, name: str):
        # TODO Create logger.
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
                 sort_property_value: int = 1,
                 network: dict = None,
                 destination: Union[IntelligentObject, None] = None):
        super().__init__(name=name)
        self.__creation_date = creation_date
        self.__sort_property = sort_property_value
        self.__destination = destination
        self.__network = network
        self.__current_node = None

    def set_destination(self):
        """
        Updates entity's destination and returns lead time to arrive there in hours.
        :return:lead time to arrive destination from current node in hours.
        """
        # TODO: consider new implementation with multiple paths possible.
        self.destination = self.network[self.current_node]['next']
        lead_time = self.network[self.current_node]['path'].lead_time
        return datetime.timedelta(hours=lead_time)

    @property
    def creation_date(self):
        return self.__creation_date

    @property
    def destination(self):
        return self.__destination

    @destination.setter
    def destination(self, new_destination: IntelligentObject):
        self.__destination = new_destination

    @property
    def sort_property(self):
        return self.__sort_property

    @sort_property.setter
    def sort_property(self, value: int):
        self.__sort_property = value

    @property
    def current_node(self):
        return self.__current_node

    @current_node.setter
    def current_node(self, new_node: IntelligentObject):
        self.__current_node = new_node

    @property
    def network(self):
        return self.__network

    @network.setter
    def network(self, new_network: dict):
        self.__network = new_network


class SimQueue(IntelligentObject):
    def __init__(self,
                 name: str,
                 sorting_feature: Union[str, None] = None,
                 sorting_policy: str = 'smallest'):
        super().__init__(name=name)
        self.__sorting_feature = sorting_feature
        self.__sorting_policy = sorting_policy
        self.__content = list()

    def add_entity(self, entity: Union[Entity, SimEvent]):
        if self.sorting_feature is None:
            entity.sort_property = len(self.content) + 1
        self.content.append(entity)
        self.sort_queue()

    def sort_queue(self):
        reverse_list = not (self.sorting_policy == 'smallest')
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

    @property
    def length(self):
        return len(self.content)


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
        enter_date = pd.to_datetime(enter_date)
        if process is None:
            process = EmptyProcess(name='empty_process',
                                   associated_object=entity,
                                   context_object=self)
        if self.available:
            self.population.append(entity)
            if len(self.population) == self.capacity:
                self.available = False
            entity.current_node = self
            process.run_process(entity=entity,
                                events=events,
                                actions=actions)
            new_event = SimEvent(start_date=enter_date,
                                 end_date=enter_date,
                                 event_name=f'on_entered_{self.name}',
                                 object_dictionary={'my_entity': entity,
                                                    'my_node': self,
                                                    'events': events,
                                                    'actions': actions,
                                                    'exit_date': enter_date},
                                 action_string='my_node.on_exited('
                                               'entity=my_entity,'
                                               'events=events,'
                                               'actions=actions,'
                                               'exit_date=exit_date)')
            actions.add_entity(entity=new_event)
        else:
            new_event = SimEvent(start_date=enter_date,
                                 end_date=enter_date,
                                 event_name='created_entity',
                                 object_dictionary={'new_entity': entity,
                                                    'output_node': self,
                                                    'events_dict': events,
                                                    'actions_queue': actions},
                                 action_string='output_node.on_entered(entity=new_entity, '
                                               'enter_date=creation_date, '
                                               'events=events,'
                                               'actions=actions)')
            try:
                events[f'on_exited_{self.name}'].append(new_event)
            except KeyError:
                events[f'on_exited_{self.name}'] = list()
                events[f'on_exited_{self.name}'].append(new_event)
            self.queue.add_entity(entity)

    def on_exited(self,
                  entity: Entity,
                  events: dict,
                  actions: SimQueue,
                  exit_date: datetime.datetime,
                  process: Union[SimProcess, None] = None
                  ):
        if process is None:
            process = EmptyProcess(name='empty_process',
                                   associated_object=entity,
                                   context_object=self)
        exit_date = pd.to_datetime(exit_date)
        self.population.remove(entity)
        self.available = True
        process.run_process(entity=entity,
                            events=events,
                            actions=actions)
        try:
            events = events[f'on_exited_{self.name}']
            for ev in events:
                ev.end_date = exit_date
                actions.add_entity(ev)
        except KeyError:
            pass
        lead_time = entity.set_destination()
        new_event = SimEvent(start_date=exit_date,
                             end_date=exit_date+lead_time,
                             event_name=f'on_entered_{entity.destination.name}',
                             object_dictionary={'new_entity': entity,
                                                'enter_node': entity.destination,
                                                'enter_date': exit_date+lead_time,
                                                'events_dict': events,
                                                'actions_queue': actions},
                             action_string='enter_node.on_entered(entity=new_entity, '
                                           'enter_date=enter_date, '
                                           'events=events,'
                                           'actions=actions)')
        actions.add_entity(entity=new_event)

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
                                                  network=self.network,
                                                  actions_queue=self.actions)
        first_action = self.actions.content.pop()
        def_string = first_action.create_definition_string(name='first_action')
        exec(def_string)
        exec(first_action.action_string)
        while self.actions.length > 0:
            # TODO encapsulate exec in a method to avoid overriding variables.
            # TODO implement scape option to avoid infinite loop.
            next_action = self.actions.content.pop()
            def_string = next_action.create_definition_string(name='next_action')
            exec(def_string)
            exec(next_action.action_string)
        print('hello')

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
                                           network: dict,
                                           events_dict: dict,
                                           actions_queue: SimQueue):
        for idx, row in self.arrival_table.iterrows():
            datetime_loc = row[self.datetime_column]
            if self.name_column is None:
                entity_name = f'entity_{idx}'
            else:
                entity_name = row[self.name_column]
            new_event = SimEvent(start_date=datetime_loc,
                                 end_date=datetime_loc,
                                 event_name='created_entity',
                                 object_dictionary={'entity_name': entity_name,
                                                    'my_network': network,
                                                    'creation_date': datetime_loc,
                                                    'creator_output_node': self.output_node,
                                                    'events_dict': events_dict,
                                                    'actions_queue': actions_queue},
                                 action_string='new_entity = Entity(name=entity_name, '
                                               'creation_date=creation_date, '
                                               'network=my_network);'
                                               'creator_output_node.on_entered(entity=new_entity, '
                                               'enter_date=creation_date, '
                                               'events=events_dict,'
                                               'actions=actions_queue)')
            actions_queue.add_entity(new_event)

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
