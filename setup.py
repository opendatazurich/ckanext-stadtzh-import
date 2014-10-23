from setuptools import setup, find_packages

version = '0.0'

setup(
    name='ckanext-stadtzh-import',
    version=version,
    description="CKAN extension for the City of Zurich import the data of the existing website",
    long_description="""\
    """,
    classifiers=[],
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
        'ckanext-stadtzh-harvest',
        'html2text==2014.9.25'
    ],
    entry_points=
    """
    [ckan.plugins]
    stadtzhimport=ckanext.stadtzhimport.plugins:StadtzhimportHarvest
    stadtzhimport_harvester=ckanext.stadtzhimport.harvesters:StadtzhimportHarvester
    [paste.paster_command]
    harvester=ckanext.stadtzhimport.commands.harvester:HarvesterCommand
    """,
)
