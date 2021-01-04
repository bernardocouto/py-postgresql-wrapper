# PyPostgreSQLWrapper
PyPostgreSQLWrapper is a simple adapter for PostgreSQL with connection pooling.

## Configuration
The configuration can be done through **JSON** file or by **Dict** following the pattern described below:
```json
{
  "dbname": "postgres",
  "host": "localhost",
  "maxconnections": 10,
  "password": "postgres",
  "port": 5432,
  "print_sql": true,
  "user": "postgres"
}
```

## Usage
PyPostgreSQLWrapper usage description:

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

### Insert
```python
from py_postgresql_wrapper.database import Database

with Database() as database:
    database.insert('test').set('id', 1).set('description', 'Test').execute()
```
