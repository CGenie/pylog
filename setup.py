#!/usr/bin/env python

from setuptools import setup, find_packages

REQUIRED_PACKAGES = [
    'amqp==1.4.5',
    'pyes']

install_requires = []

setup(name='pylog',
      version='1.0',
      description='Asynchronous logging for Python with RabbitMQ and ElasticSearch',
      author='Przemyslaw Kaminski',
      packages=find_packages(),
      install_requires=REQUIRED_PACKAGES,
     )
