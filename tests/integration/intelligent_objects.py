import pandas as pd
from tepuy.intelligent_objects import Creator


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


def test_source():
    wo_df = create_mock_work_orders()
    new_source = Creator(name='wo_creator',
                         position=(1, 1),
                         arrival_type='arrival_table',
                         arrival_table=wo_df,
                         datetime_column='order_date')
    new_source.create_entities_from_arrival_table()
