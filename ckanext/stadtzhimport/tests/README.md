# Developing without running jobs manually

It's quite awkward and slow to run the harvesters on the command line when you are developing a harvester. To solve this issue I have setup the tests from the ckan harvester and tweaked them a bit so we can use them to get feedback quickly on what we are doing.

The important stuff is located in `ckanext-stadtzh-import/ckanext/stadtzhimport/tests/test_queue.py`

A `TestHarvester` inherits from `StadtzhimportHarvester`. Only the info is mocked, the other methods are kept. `TestHarvestqueue` then calls e.g. `gather_stage` from the original harvester. You can then either mock methods in the test or operate on the real harvester and run the tests to see what it does:

    sudo su - ckan
    . ~/default/bin/activate
    cd /vagrant/ckanext-stadtzh-import

    nosetests --logging-filter=ckanext.stadtzhimport.harvesters.stadtzhimportharvester --ckan --with-pylons=test.ini ckanext/stadtzhimport/tests

I this example the logging filter is used to only show messages of the `stadtzhimportharvester`.

    nosetests --logging-level=INFO --ckan --with-pylons=test.ini ckanext/stadtzhimport/tests

You can also limit the messages to a specific logging level.
