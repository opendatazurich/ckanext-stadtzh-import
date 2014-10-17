from setuptools import setup, find_packages
import sys, os

version = '0.0'

setup(
    name='ckanext-stadtzh-import',
    version=version,
    description="CKAN extension for the City of Zurich import the data of the existing website",
    long_description="""\
    """,
    classifiers=[], # Get strings from http://pypi.python.org/pypi?%3Aaction=list_classifiers
    keywords='',
    author='Liip AG',
    author_email='ogd@liip.ch',
    url='http://www.liip.ch',
    license='GPL',
    packages=find_packages(exclude=['ez_setup', 'examples', 'tests']),
    namespace_packages=['ckanext', 'ckanext.stadtzhimport'],
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        # -*- Extra requirements: -*-
        lxml==2.2.4,
        html2text==2014.9.25
    ],
    entry_points=\
    """
    [ckan.plugins]
    stadtzhimport=ckanext.stadtzhimport.plugins:StadtzhimportHarvest
    stadtzhimport_harvester=ckanext.stadtzhimport.harvesters:StadtzhimportHarvester
    stadtzhimport_test_harvester=ckanext.stadtzhimport.tests.test_queue:TestHarvester
    [paste.paster_command]
    harvester=ckanext.stadtzhimport.commands.harvester:Harvester
    """,
)
