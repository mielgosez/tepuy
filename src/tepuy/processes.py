from typing import Union
from abc import ABC, abstractmethod
import datetime


class SimEvent:
    def __init__(self,
                 start_date: datetime.datetime,
                 end_date: datetime.datetime,
                 event_name: str,
                 object_name: Union[str, None] = None,
                 action_name: Union[str, None] = None):
        self.__start_date = start_date
        self.__end_date = end_date
        self.__object_name = object_name
        self.__action_name = action_name
        self.__event_name = event_name

    @property
    def start_date(self):
        return self.__start_date

    @property
    def end_date(self):
        return self.__end_date

    @property
    def object_name(self):
        return self.__object_name

    @property
    def action_name(self):
        return self.__action_name

    @property
    def event_name(self):
        return self.__event_name


class Material:
    def __init__(self,
                 name: str,
                 quantity: float,
                 unit: str,
                 bom: Union[dict, None]):
        self.__name = name
        self.__quantity = quantity
        self.__unit = unit
        self.__bom = bom

    # Getters and setters
    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, new_name: str):
        self.__name = new_name

    @property
    def quantity(self):
        return self.__name

    @quantity.setter
    def quantity(self, delta_quantity: float):
        if self.__quantity + delta_quantity < 0:
            raise ValueError(f'reducing {self.name} by {delta_quantity} results in negative material.')
        self.__quantity = self.__quantity + delta_quantity

    @property
    def unit(self):
        return self.__unit

    @unit.setter
    def unit(self, new_unit: str):
        self.__unit = new_unit

    @property
    def bom(self):
        return self.__bom

    @bom.setter
    def bom(self, new_bom: dict):
        self.__bom = new_bom


class SimProcess(ABC):
    def __init__(self,
                 name: str,
                 associated_object,
                 context_object):
        self.__name = name
        self.__associated_object: associated_object
        self.__context_object = context_object

    @abstractmethod
    def process(self, **kwargs):
        pass

    # Steps
    def delay_step(self,
                   duration: float,
                   unit: str,
                   start_date: datetime.datetime):
        duration_key = {
            'seconds': 1,
            'minutes': 60,
            'hours': 3600,
        }
        try:
            transformed_duration = duration*duration_key[unit]
        except KeyError:
            ValueError(f'{unit} is not a valid option. '
                       f'Valid options are: {", ".join([it for it in duration_key.keys()])}')
        available_date = start_date+datetime.timedelta(seconds=transformed_duration)
        self.associated_object.available_date = available_date
        way_event = SimEvent(start_date=start_date,
                             end_date=available_date,
                             event_name='Wait')
        return way_event

    def produce_step(self,
                     material: Material,
                     quantity: float):
        if quantity < 0:
            ValueError('Quantity must be positive.')
        material.quantity += quantity
        if material.bom is not None:
            for mat, qty in material.bom.items():
                self.consume_step(material=mat,
                                  quantity=qty)

    @staticmethod
    def consume_step(material,
                     quantity: float):
        if quantity < 0:
            ValueError('Quantity must be positive.')
        material.quantity -= quantity

    def seize_step(self, resource):
        if resource.seized:
            resource.ride_request_queue.add_entity(self.associated_object)
        else:
            resource.owner = resource
            resource.seized = True

    @staticmethod
    def release_step(resource):
        resource.owner = None
        resource.seized = False

    # Getters and Setters
    @property
    def name(self):
        return self.__name

    @name.setter
    def name(self, new_name: str):
        self.__name = new_name

    @property
    def associated_object(self):
        return self.__associated_object

    @property
    def context_object(self):
        return self.__context_object
