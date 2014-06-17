import os
from lxml import etree
from ckanext.stadtzhimport.helpers.xpath import XPathHelper
from ckanext.stadtzhimport.harvesters.stadtzhimportharvester import StadtzhimportHarvester
import ckanext.harvest.model as harvest_model
from ckanext.harvest.model import HarvestObject, HarvestObjectExtra
import ckanext.harvest.queue as queue
import json
import ckan.logic as logic
from ckan import model
from pprint import pprint


class TestHarvester(StadtzhimportHarvester):

    def info(self):
        return {'name': 'test', 'title': 'test', 'description': 'test'}

    def gather_stage(self, harvest_job):

        ids = []

        with open(os.path.join(self.IMPORT_PATH, 'cms_stzh_ch_content_portal_de_index_ogd_systemView.xml'), 'r') as cms_file:
            parser = etree.XMLParser(encoding='utf-8', ns_clean=True)
            datasets = XPathHelper(etree.fromstring(cms_file.read(), parser=parser)).multielement('.//sv:node[@sv:name="daten"]/sv:node[@sv:name="muetterberatungsstelle"]')
            for dataset in datasets:
                ids.append(self._save_dataset(dataset, harvest_job))

        return ids

    def import_stage(self, harvest_object):
        return False


class TestHarvestQueue(object):
    @classmethod
    def setup_class(cls):
        harvest_model.setup()

    @classmethod
    def teardown_class(cls):
        model.repo.rebuild_db()

    def test_01_basic_harvester(self):

        ### make sure queues/exchanges are created first and are empty
        consumer = queue.get_consumer('ckan.harvest.gather','harvest_job_id')
        consumer_fetch = queue.get_consumer('ckan.harvest.fetch','harvest_object_id')
        consumer.queue_purge(queue='ckan.harvest.gather')
        consumer_fetch.queue_purge(queue='ckan.harvest.fetch')


        user = logic.get_action('get_site_user')(
            {'model': model, 'ignore_auth': True}, {}
        )['name']

        context = {'model': model, 'session': model.Session,
                   'user': user, 'api_version': 3, 'ignore_auth': True}

        source_dict = {
            'title': 'Test Source',
            'name': 'test-source',
            'url': 'basic_test',
            'source_type': 'test',
        }

        harvest_source = logic.get_action('harvest_source_create')(
            context,
            source_dict
        )

        assert harvest_source['source_type'] == 'test', harvest_source
        assert harvest_source['url'] == 'basic_test', harvest_source


        harvest_job = logic.get_action('harvest_job_create')(
            context,
            {'source_id':harvest_source['id']}
        )

        job_id = harvest_job['id']

        assert harvest_job['source_id'] == harvest_source['id'], harvest_job

        assert harvest_job['status'] == u'New'

        logic.get_action('harvest_jobs_run')(
            context,
            {'source_id':harvest_source['id']}
        )

        assert logic.get_action('harvest_job_show')(
            context,
            {'id': job_id}
        )['status'] == u'Running'

        reply = consumer.basic_get(queue='ckan.harvest.gather')

        queue.gather_callback(consumer, *reply)

        reply = consumer_fetch.basic_get(queue='ckan.harvest.fetch')
        queue.fetch_callback(consumer_fetch, *reply)

        assert 1 == 2
