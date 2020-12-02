#!/usr/bin/env python3

from setuptools import setup

# Get the long description from the README file
with open('README.rst', encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='influxdb-file-importer',
    version='0.2.0',
    description='Import data from files into InfluxDB',
    long_description=long_description,
    url='https://github.com/Nobatek/influxdb-file-importer',
    author='Jérôme Lafréchoux',
    author_email='jlafrechoux@nobatek.com',
    license='MIT',
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3 :: Only',
    ],
    keywords=[
        'InfluxDB',
        'timeseries',
    ],
    py_modules=['influxdb_file_importer'],
    python_requires='>=3.7',
    install_requires=[
        'influxdb_client>=1.12',
    ],
)
