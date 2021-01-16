from dbutils.pooled_db import PooledDB

import json
import os
import psycopg2


class Configuration(object):

    """
    Configuration object
    """

    __instance__ = None

    def __init__(self, configuration_dict=None, configuration_file=None):
        if configuration_dict:
            self.data = configuration_dict
        elif configuration_file:
            if not os.path.exists(configuration_file):
                raise ConfigurationNotFoundException()
            with open(configuration_file, 'r') as file:
                try:
                    self.data = json.loads(file.read())
                except json.decoder.JSONDecodeError as exception:
                    raise ConfigurationInvalidException(exception)
        self.data = {
            "dbname": self.data['database'],
            "host": self.data['host'],
            "maxconnections": self.data['max_connection'],
            "password": self.data['password'],
            "port": self.data['port'],
            "print_sql": self.data['print_sql'],
            "user": self.data['username']
        }
        self.print_sql = self.data.pop('print_sql') if 'print_sql' in self.data else False
        self.pool = PooledDB(psycopg2, **self.data)

    @staticmethod
    def instance(configuration_dict=None, configuration_file='/etc/py_postgresql_wrapper/configuration.json'):
        """
        Get singleton instance of configuration
        :param configuration_dict: Configuration dict
        :param configuration_file: Configuration file
        :return: Configuration instance
        """
        if Configuration.__instance__ is None:
            Configuration.__instance__ = Configuration(configuration_dict, configuration_file)
        return Configuration.__instance__


class ConfigurationInvalidException(Exception):

    """
    Configuration is not a valid JSON file
    """


class ConfigurationNotFoundException(Exception):

    """
    Configuration file was not found
    """
