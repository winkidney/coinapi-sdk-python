import os

from setuptools import setup, find_packages

here = os.path.dirname(os.path.abspath(__file__))


setup(
    name='coin-api',
    version='0.0.1',
    author='winkidney@gmail.com',
    install_requires=['requests'],
    packages=find_packages(here)
)
