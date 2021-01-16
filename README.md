# PyPostgreSQLWrapper
PyPostgreSQLWrapper is a simple adapter for PostgreSQL with connection pooling

## Configuration
The configuration can be done through **JSON** file or by **Dict** following the pattern described below:
```json
{
  "database": "postgres",
  "host": "localhost",
  "max_connection": 10,
  "password": "postgres",
  "port": 5432,
  "print_sql": true,
  "username": "postgres"
}
```

## Usage
PyPostgreSQLWrapper usage description:

### Delete

#### Delete with where
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.delete('test').where('id', 1).execute()
```

#### Delete with like condition
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.delete('test').where('description', 'Test%', operator='like').execute()
```

### Insert
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.insert('test').set('id', 1).set('description', 'Test').execute()
```

### Paging

#### Paging with where
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.select('test').fields('id', 'description').where('id', 3, operator='<').order_by('id').paging(0, 2)
```

#### Paging without where
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.select('test').paging(0, 2)
```

### Select

#### Fetch all
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.select('test').execute().fetch_all()
```

#### Fetch many
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.select('test').execute().fetch_many(1)
```

#### Fetch one
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.select('test').execute().fetch_one()
```

#### Select by file
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.execute('find_test_by_id', {'id': 1}).fetch_one()
```

#### Select by query
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.execute('select id, description from test where id = %(id)s', {'id': 1}).fetch_one()
```

### SQL
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.execute('''
        create table test (
            id int primary key,
            description varchar(255)
        )
    ''')
```

### Update

#### Update with where
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.update('test').set('description', 'New Test 1').where('id', 1).execute()
```

#### Update with where all
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.update('test').set('description', 'New Test 1').where_all({'id': 1, 'description': 'Test 1'}).execute()
```
