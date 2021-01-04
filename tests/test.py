from py_postgresql_wrapper.configuration import Configuration
from py_postgresql_wrapper.database import Database

Configuration.instance(configuration_file='configuration.json')


def test_create_table():
    with Database() as database:
        database.execute('''
            drop table if exists test
        ''')
        database.execute('''
            create table test (
                id int primary key,
                description varchar(255)
            )
        ''')


def test_insert():
    with Database() as database:
        database.insert('test').set('id', 1).set('description', 'Test').execute()


def test_truncate_table():
    with Database() as database:
        database.execute('''
            truncate table test
        ''')
