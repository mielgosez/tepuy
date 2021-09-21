import pandas as pd
from tepuy.intelligent_objects import Creator, MainSimModel, Destructor, Path


def create_mock_work_orders():
    wo = pd.DataFrame({
        'order_date': ['2021-09-30 15:00:00',
                       '2021-09-30 16:00:00',
                       '2021-09-30 17:00:00',
                       '2021-09-30 18:00:00'],
        'due_date': ['2021-09-30 15:00:00',
                     '2021-09-30 16:00:00',
                     '2021-09-30 17:00:00',
                     '2021-09-30 18:00:00'],
        'material': ['mat_1', 'mat_2', 'mat_1', 'mat_2']})
    return wo


def create_bom():
    bom = {'mat_1': {'mat_a': 2, 'mat_b': 5},
           'mat_2': {'mat_a': 4, 'mat_b': 3}}
    return bom


def create_lead_time():
    return {'mat_a': 3, 'mat_b': 4}


def create_initial_material_qty():
    return {'mat_a': 30, 'mat_b': 40}


def test_source():
    initial_material = create_initial_material_qty()
    lead_time = create_lead_time()
    bom = create_bom()
    wo_df = create_mock_work_orders()
    new_source = Creator(name='wo_creator',
                         position=(1, 1),
                         arrival_type='arrival_table',
                         arrival_rate=None,
                         arrival_table=wo_df,
                         datetime_column='order_date',
                         name_column=None)
    new_sink = Destructor(name='wo_destructor', position=(2, 1))
    new_path = Path(name='main_type',
                    path_type='path_time',
                    node_from=new_source.output_node,
                    node_to=new_sink.input_node,
                    lead_time=10)
    main_model = MainSimModel(name='new_model', start_date=pd.to_datetime('2021-09-30 15:00:00'),
                              model_network={'start': {'next': new_source},
                                             new_source: {'next': new_sink, 'path': new_path}})
    main_model.run()
    new_source.create_entities_from_arrival_table()
