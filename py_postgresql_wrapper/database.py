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

    def disconnect(self):
        """
        Disconnect from database
        :return: None
        """
        self.connection.close()

    def execute(self, sql, parameters=None, skip_load_query=False):
        """
        Execute query by name
        :param sql: String o name of file
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


# Builders
class SQLBuilder(object):

    """
    SQL constructor
    """

    def __init__(self, database, table):
        self.database = database
        self.parameters = {}
        self.table = table

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

    def fetch_one(self):
        """
        Fetch one record by the cursor
        :return: Data row
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
        :return: Data row
        """
        row = self.fetch_one()
        if row is None:
            raise StopIteration()
        return row


class DictWrapper(object):

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
