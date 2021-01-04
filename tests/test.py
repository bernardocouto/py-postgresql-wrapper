from py_postgresql_wrapper.configuration import Configuration
from py_postgresql_wrapper.database import Database, Page

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


def test_delete_by_id():
    with Database() as database:
        database.delete('test').where('id', 1).execute()
        assert len(database.execute('select id, description from test').fetch_all()) == 3


def test_delete_with_like():
    with Database() as database:
        database.delete('test').where('description', 'Test%', operator='like').execute()
        assert len(database.execute("select id, description from test").fetch_all()) == 0


def test_find_all():
    with Database() as database:
        data = database.execute('select id, description from test').fetch_all()
        assert len(data) == 4
        assert data[0].id == 1
        assert data[0].description == 'Test 1'


def test_find_by_file():
    with Database() as database:
        data = database.execute('find_test_by_id', {'id': 1}).fetch_one()
        assert data.id == 1
        assert data.description == 'Test 1'


def test_find_by_id():
    with Database() as database:
        data = database.execute('select id, description from test where id = %(id)s', {'id': 1}).fetch_one()
        assert data.id == 1


def test_find_by_select():
    with Database() as database:
        data = database.select('test').where('description', 'Test%', operator='like').execute().fetch_all()
        assert len(data) == 4


def test_find_by_select_without_where():
    with Database() as database:
        data = database.select('test').execute().fetch_all()
        print(data)
        assert len(data) == 4


def test_find_many():
    with Database() as database:
        data = database.execute('select id, description from test').fetch_many(2)
        assert len(data) == 2


def test_find_paging_by_select():
    with Database() as database:
        data = database.select('test').fields('id', 'description').where('id', 3, operator='<').order_by('id').paging(0, 2)
        assert type(data) == Page
        assert len(data.data) == 2
        assert data.data[0].id == 1
        assert data.data[0].description == 'Test 1'


def test_find_paging_by_select_without_where():
    with Database() as database:
        data = database.select('test').paging(0, 2)
        assert type(data) == Page
        assert len(data.data) == 2


def test_find_without_result():
    with Database() as database:
        data = database.select('test').where('1', '0', constant=True).execute().fetch_one()
        assert data is None


def test_insert():
    with Database() as database:
        database.insert('test').set('id', 1).set('description', 'Test 1').execute()
        database.insert('test').set('id', 2).set('description', 'Test 2').execute()
        database.insert('test').set('id', 3).set('description', 'Test 3').execute()
        database.insert('test').set('id', 4).set('description', 'Test 4').execute()


def test_rollback():
    try:
        with Database() as database:
            database.insert('test').set('id', 10).set('description', 'Test 10').execute()
            raise Exception()
    except():
        with Database() as database:
            assert database.select('test').where('id', 10).execute().fetch_one() is None


def test_truncate_table():
    with Database() as database:
        database.execute('''
            truncate table test
        ''')


def test_update():
    with Database() as database:
        data = database.update('test').set('description', 'New Test 1').where('id', 1).execute()
        assert data.cursor.rowcount == 1


def test_update_where_all():
    try:
        with Database() as database:
            database.update('test').set('description', 'New Test 1').where_all({'id': 1, 'description': 'Test 1'}).execute()
    except() as e:
        assert e is None
