# -*- coding: utf-8 -*-

# Learn more: https://github.com/kennethreitz/setup.py

import pathlib
from setuptools import setup, find_packages


readme = pathlib.Path('README.rst').read_text()

setup(
    name='sample',
    version='0.1.0',
    description='Sample package for data_science exercise',
    long_description=readme,
    author='Kaveh Azizian',
    author_email='kaveh.azizian20@gmail.com',
    url='https://github.com/kavehazizian/2023-01-data-science-exercise',
    license='GPLV3',
    packages=find_packages(exclude=('tests', 'docs'))
)

