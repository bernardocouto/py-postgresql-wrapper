from setuptools import find_packages, setup

import os

with open(os.path.abspath('README.md')) as file:
    long_description = file.read()

setup(
    author='Bernardo Couto',
    author_email='bernardocouto.py@gmail.com',
    classifiers=[
        'License :: OSI Approved :: GNU General Public License v3 (GPLv3)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Database'
    ],
    description='PyPostgreSQLWrapper is a simple adapter for PostgreSQL with connection pooling',
    install_requires=[
        'dbutils',
        'psycopg2-binary'
    ],
    keywords='database postgresql psycopg2 sql',
    license='GPLv3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    name='py-postgresql-wrapper',
    packages=find_packages(),
    url='https://github.com/bernardocouto/py-postgresql-wrapper',
    version='1.0.1'
)
