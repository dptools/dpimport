import os
from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))

requires = [
    'pyaml',
    'pymongo==3.11.4',
    'pandas'
]

about = dict()
with open(os.path.join(here, 'dpimport', '__version__.py'), 'r') as f:
    exec(f.read(), about)

setup(
    name=about['__title__'],
    version=about['__version__'],
    description=about['__description__'],
    author=about['__author__'],
    author_email=about['__author_email__'],
    url=about['__url__'],
    packages=find_packages(),
    scripts=[
        'scripts/import.py'
    ],
    install_requires=requires
)
