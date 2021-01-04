from .configuration import Configuration

import errno
import os
import psycopg2
import psycopg2.extras

QUERIES_DIRECTORY = os.path.realpath(os.path.curdir) + '/sql/'


class Database(object):

    """
    Facade to access database
    """

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        if exception_type is None and exception_value is None and exception_traceback is None:
            self.connection.commit()
        else:
            self.connection.rollback()
        self.disconnect()

    def __init__(self, configuration=None):
        self.configuration = Configuration.instance() if configuration is None else configuration
        self.connection = self.configuration.pool.connection()
        self.print_sql = self.configuration.print_sql

    def delete(self, table):
        """
        Delete string command
        :param table: Table name
        :return: Delete builder
        """
        return DeleteBuilder(self, table)

    def disconnect(self):
        """
        Disconnect from database
        :return: None
        """
        self.connection.close()

    def execute(self, sql, parameters=None, skip_load_query=False):
        """
        Execute query by name
        :param sql: String or name of file
        :param parameters: SQL parameters
        :param skip_load_query: Skip load file
        :return: Cursor
        """
        cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        if self.print_sql:
            print('SQL: {} - Parameters: {}'.format(sql, parameters))
        if skip_load_query:
            sql = sql
        else:
            sql = self.load_query(sql)
        cursor.execute(sql, parameters)
        return CursorWrapper(cursor)

    def insert(self, table):
        """
        Insert string command
        :param table: Table name
        :return: Insert builder
        """
        return InsertBuilder(self, table)

    def update(self, table):
        """
        Update string command
        :param table: Table name
        :return: Update builder
        """
        return UpdateBuilder(self, table)

    @staticmethod
    def load_query(name):
        """
        Load a query located in ./sql/<name.sql>
        :param name: File name
        :return: Query as a string
        """
        try:
            with open(QUERIES_DIRECTORY + name + '.sql') as file:
                query = file.read()
            return query
        except IOError as exception:
            if exception.errno == errno.ENOENT:
                return name
            else:
                raise exception

    def paging(self, sql, page=0, parameters=None, size=10, skip_load_query=True):
        """
        Paging string command
        :param sql: String or name of file
        :param page: Page number
        :param parameters: SQL parameters
        :param size: Page size
        :param skip_load_query: Skip load file
        :return:
        """
        if skip_load_query:
            sql = sql
        else:
            sql = self.load_query(sql)
        sql = '{} limit {} offset {}'.format(sql, size + 1, page * size)
        data = self.execute(sql, parameters, skip_load_query=True).fetch_all()
        last = len(data) <= size
        return Page(page, size, data[:-1] if not last else data, last)

    def select(self, table):
        """
        Select string command
        :param table: Table name
        :return: Select builder
        """
        return SelectBuilder(self, table)


class Page(dict):

    """
    Page object
    """

    def __init__(self, number, size, data, last):
        self['data'] = self.data = data
        self['last'] = self.last = last
        self['number'] = self.number = number
        self['size'] = self.size = size


# Builders
class SQLBuilder(object):

    """
    SQL constructor
    """

    def __init__(self, database, table):
        self.database = database
        self.parameters = {}
        self.table = table
        self.where_conditions = []

    def execute(self):
        """
        Execute SQL
        :return: Return of execution of SQL code in the database
        """
        return self.database.execute(self.sql(), self.parameters, True)

    def sql(self):
        """
        SQL
        :return: None
        """
        pass

    def where_all(self, data):
        """
        Construction of the command for where all
        :param data: Data for where
        :return: Self
        """
        for value in data.keys():
            self.where(value, data[value])
        return self

    def where_build(self):
        """
        Construction of the command for where
        :return: Where command
        """
        if len(self.where_conditions) > 0:
            conditions = ' and '.join(self.where_conditions)
            return 'where {}'.format(conditions)
        else:
            return ''

    def where(self, field, value, constant=False, operator='='):
        """
        Construction of the command for where conditions
        :param field: Where field
        :param value: Where value
        :param constant: Where constant
        :param operator: Where operator
        :return: Self
        """
        if constant:
            self.where_conditions.append('{} {} {}'.format(field, operator, value))
        else:
            self.parameters[field] = value
            self.where_conditions.append('{0} {1} %({0})s'.format(field, operator))
        return self


class DeleteBuilder(SQLBuilder):

    """
    Delete constructor
    """

    def sql(self):
        """
        Construction of the command for data delete
        :return: Delete SQL string
        """
        return 'delete from {} {}'.format(self.table, self.where_build())


class InsertBuilder(SQLBuilder):

    """
    Insert constructor
    """

    def __init__(self, database, table):
        super(InsertBuilder, self).__init__(database, table)
        self.constants = {}

    def set(self, field, value, constant=False):
        """
        Set constants or parameters
        :param field: SQL field
        :param value: SQL value
        :param constant: SQL constant
        :return: Self
        """
        if constant:
            self.constants[field] = value
        else:
            self.parameters[field] = value
        return self

    def set_all(self, data):
        """
        Set all properties
        :param data: Group of constants and parameters
        :return: Self
        """
        for value in data.keys():
            self.set(value, data[value])
        return self

    def sql(self):
        """
        Construction of the command for data entry
        :return: Insert SQL string
        """
        if len(set(list(self.parameters.keys()) + list(self.constants.keys()))) == len(self.parameters.keys()) + len(self.constants.keys()):
            columns = []
            values = []
            for field in self.constants:
                columns.append(field)
                values.append(self.constants[field])
            for field in self.parameters:
                columns.append(field)
                values.append('%({})s'.format(field))
            return 'insert into {} ({}) values ({})'.format(self.table, ', '.join(columns), ', '.join(values))
        else:
            raise ValueError('There are repeated keys in constants and values')


class SelectBuilder(SQLBuilder):

    """
    Select Constructor
    """

    def __init__(self, database, table):
        super(SelectBuilder, self).__init__(database, table)
        self.select_fields = ['*']
        self.select_group_by = []
        self.select_order_by = []
        self.select_page = ''

    def fields(self, *fields):
        """
        Set select fields
        :param fields: Select fields
        :return: Self
        """
        self.select_fields = fields
        return self

    def group_by(self, *fields):
        """
        Set group by fields
        :param fields: Group by fields
        :return: Self
        """
        self.select_group_by = fields
        return self

    def order_by(self, *fields):
        """
        Set order by fields
        :param fields: Order by fields
        :return: Self
        """
        self.select_order_by = fields
        return self

    def paging(self, page=0, size=10):
        """
        Pagination
        :param page: Page number
        :param size: Page size
        :return: Page
        """
        self.select_page = 'limit {} offset {}'.format(size + 1, page * size)
        data = self.execute().fetch_all()
        last = len(data) <= size
        return Page(page, size, data[:-1] if not last else data, last)

    def sql(self):
        """
        Construction of the command for data select
        :return: Select SQL string
        """
        group_by = ', '.join(self.select_group_by)
        if group_by != '':
            group_by = 'group by {}'.format(group_by)
        order_by = ', '.join(self.select_order_by)
        if order_by != '':
            order_by = 'order by {}'.format(order_by)
        return 'select {} from {} {} {} {} {}'.format(
            ', '.join(self.select_fields),
            self.table,
            self.where_build(),
            group_by,
            order_by,
            self.select_page
        )


class UpdateBuilder(SQLBuilder):

    """
    Update constructor
    """

    def __init__(self, database, table):
        super(UpdateBuilder, self).__init__(database, table)
        self.statements = []

    def set(self, field, value, constant=False):
        """
        Set constants or parameters
        :param field: SQL field
        :param value: SQL value
        :param constant: SQL constant
        :return: Self
        """
        if constant:
            self.statements.append('{} = {}'.format(field, value))
        else:
            self.statements.append('{0} = %({0})s'.format(field))
            self.parameters[field] = value
        return self

    def set_all(self, data):
        """
        Set all properties
        :param data: Group of constants and parameters
        :return: Self
        """
        for value in data.keys():
            self.set(value, data[value])
        return self

    def set_build(self):
        """
        Construction of the command for set
        :return: Set command
        """
        if len(self.statements) > 0:
            statements = ', '.join(self.statements)
            return 'set {}'.format(statements)
        else:
            return ''

    def sql(self):
        """
        Construction of the command for data update
        :return: Update SQL string
        """
        return 'update {} {} {}'.format(self.table, self.set_build(), self.where_build())


# Wrappers
class CursorWrapper(object):

    """
    Cursor wrapper to access cursor functions
    """

    def __init__(self, cursor):
        self.cursor = cursor

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def close(self):
        """
        Close a cursor structure
        :return: None
        """
        self.cursor.close()

    def fetch_all(self):
        """
        Fetch all record by the cursor
        :return: All data
        """
        return [DictWrapper(row) for row in self.cursor.fetchall()]

    def fetch_many(self, size):
        """
        Fetch many record by the cursor
        :param size: Size number
        :return: Many data
        """
        return [DictWrapper(row) for row in self.cursor.fetchmany(size)]

    def fetch_one(self):
        """
        Fetch one record by the cursor
        :return: Row data
        """
        row = self.cursor.fetchone()
        if row is not None:
            return DictWrapper(row)
        else:
            self.close()
        return row

    def next(self):
        """
        Return the next record by the cursor
        :return: Row data
        """
        row = self.fetch_one()
        if row is None:
            raise StopIteration()
        return row

    def row_count(self):
        """
        Return row numbers
        :return: Row numbers
        """
        return self.cursor.rowcount


class DictWrapper(dict):

    """
    Dict wrapper to access dict attribute with dot operator
    """

    def __getattr__(self, item):
        if item in self:
            if isinstance(self[item], dict) and not isinstance(self[item], DictWrapper):
                self[item] = DictWrapper(self[item])
            return self[item]
        raise AttributeError('{} is not a valid attribute'.format(item))

    def __init__(self, data):
        self.update(data)

    def __setattr__(self, key, value):
        self[key] = value

    def as_dict(self):
        """
        Return object as a dict
        :return: Self
        """
        return self
