#coding: utf-8

import os
import base64
from lxml import etree

from ckanext.stadtzhimport.helpers.xpath import XPathHelper

from ofs import get_impl
from pylons import config
from ckan.lib.base import c
from ckan import model
from ckan.model import Session, Package
from ckan.logic import ValidationError, NotFound, get_action, action
from ckan.lib.helpers import json
from ckan.lib.munge import munge_title_to_name

from ckanext.harvest.model import HarvestJob, HarvestObject, HarvestGatherError, \
                                    HarvestObjectError
from ckanext.harvest.harvesters import HarvesterBase

from pylons import config

import logging
log = logging.getLogger(__name__)

class StadtzhimportHarvester(HarvesterBase):
    '''
    The harvester to harvest the existing data portal of the Stadt ZH
    '''

    ORGANIZATION = {
        'de': u'Stadt Zürich',
        'fr': u'fr_Stadt Zürich',
        'it': u'it_Stadt Zürich',
        'en': u'en_Stadt Zürich',
    }
    LANG_CODES = ['de', 'fr', 'it', 'en']
    BUCKET = config.get('ckan.storage.bucket', 'default')
    CKAN_SITE_URL = config.get('ckan.site_url', 'http://stadtzh.lo')

    config = {
        'user': u'harvest'
    }

    IMPORT_PATH = '/usr/lib/ckan/cmspilot_stzh_ch_content_portal_de_index_ogd'

    # ---
    # COPIED FROM THE CKAN STORAGE CONTROLLER
    # ---

    def create_pairtree_marker(self, folder):
        """ Creates the pairtree marker for tests if it doesn't exist """
        if not folder[:-1] == '/':
            folder = folder + '/'

        directory = os.path.dirname(folder)
        if not os.path.exists(directory):
            os.makedirs(directory)

        target = os.path.join(directory, 'pairtree_version0_1')
        if os.path.exists(target):
            return

        open(target, 'wb').close()


    def get_ofs(self):
        """Return a configured instance of the appropriate OFS driver.
        """
        storage_backend = config['ofs.impl']
        kw = {}
        for k, v in config.items():
            if not k.startswith('ofs.') or k == 'ofs.impl':
                continue
            kw[k[4:]] = v

        # Make sure we have created the marker file to avoid pairtree issues
        if storage_backend == 'pairtree' and 'storage_dir' in kw:
            self.create_pairtree_marker(kw['storage_dir'])

        ofs = get_impl(storage_backend)(**kw)
        return ofs

    # ---
    # END COPY
    # ---

    def info(self):
        '''
        Return some general info about this harvester
        '''
        return {
            'name': 'stadtzhimport',
            'title': 'Stadtzhimport',
            'description': 'Harvests the original city of Zurich data portal data',
            'form_config_interface': 'Text'
        }


    def gather_stage(self, harvest_job):
        log.debug('In StadtzhimportHarvester gather_stage')

        ids = []

        with open(os.path.join(self.IMPORT_PATH, 'cmspilot_stzh_ch_content_portal_de_index_ogd_systemView.xml'), 'r') as cms_file:
            parser = etree.XMLParser(encoding='utf-8', ns_clean=True)
            datasets = XPathHelper(etree.fromstring(cms_file.read(), parser=parser)).multielement('.//sv:node[@sv:name="daten"]/sv:node')

            for dataset in datasets:
                if XPathHelper(dataset).text('.//sv:property[@sv:name="jcr:primaryType"]/sv:value') == 'cq:Page':
                    xpath = XPathHelper(dataset)
                    metadata = {
                        'datasetID': xpath.text('./@sv:name'),
                        'title': xpath.text('.//sv:property[@sv:name="jcr:title"]/sv:value'),
                        'url': None,
                        'notes': None,
                        'author': xpath.text('.//sv:property[@sv:name="source"]/sv:value'),
                        'maintainer': 'Open Data Zürich',
                        'maintainer_email': 'opendata@zuerich.ch',
                        'license_id': 'to_be_filled',
                        'license_url': 'to_be_filled',
                        'tags': [],
                        'resources': [],
                        'notes': self._create_markdown({
                            'Details': xpath.text('.//sv:property[@sv:name="jcr:description"]/sv:value'),
                            u'Erstmalige Veröffentlichung': xpath.text('.//sv:property[@sv:name="creationDate"]/sv:value'),
                            'Zeitraum': xpath.text('.//sv:property[@sv:name="timeRange"]/sv:value'),
                            'Aktualisierungsintervall': xpath.text('.//sv:property[@sv:name="updateInterval"]/sv:value'),
                            'Aktuelle Version': xpath.text('.//sv:property[@sv:name="version"]/sv:value'),
                            'Aktualisierungsdatum': xpath.text('.//sv:property[@sv:name="modificationDate"]/sv:value'),
                            u'Räumliche Beziehung': xpath.text('.//sv:property[@sv:name="referencePlane"]/sv:value'),
                            'Datentyp': xpath.text('.//sv:property[@sv:name="datatype"]/sv:value'),
                            'Rechtsgrundlage': xpath.text('.//sv:property[@sv:name="datatype"]/sv:value'),
                            'Bemerkungen': base64.b64decode(xpath.text('.//sv:property[@sv:name="comments"]/sv:value')),
                            'Attribute': xpath.dict_from_nodes('.//sv:node[@sv:name="attributes"]/sv:node', 'fieldname_tech',  'field_description')
                        })
                    }

                    obj = HarvestObject(
                        guid = metadata['datasetID'],
                        job = harvest_job,
                        content = json.dumps(metadata)
                    )
                    obj.save()
                    log.debug('adding ' + metadata['datasetID'] + ' to the queue')
                    ids.append(obj.id)

        return ids


    def fetch_stage(self, harvest_object):
        log.debug('In StadtzhimportHarvester fetch_stage')

        # Get the URL
        datasetID = json.loads(harvest_object.content)['datasetID']
        log.debug(harvest_object.content)

        # Get contents
        try:
            harvest_object.save()
            log.debug('successfully processed ' + datasetID)
            return True
        except Exception, e:
            log.exception(e)



    def import_stage(self, harvest_object):
        log.debug('In StadtzhimportHarvester import_stage')

        if not harvest_object:
            log.error('No harvest object received')
            return False


        try:
            package_dict = json.loads(harvest_object.content)
            package_dict['id'] = harvest_object.guid
            package_dict['name'] = munge_title_to_name(package_dict[u'datasetID'])

            user = model.User.get(self.config['user'])
            context = {
                'model': model,
                'session': Session,
                'user': self.config['user']
            }

            # Find or create the organization the dataset should get assigned to.
            try:
                data_dict = {
                    'permission': 'edit_group',
                    'id': munge_title_to_name(self.ORGANIZATION['de']),
                    'name': munge_title_to_name(self.ORGANIZATION['de']),
                    'title': self.ORGANIZATION['de']
                }
                package_dict['owner_org'] = get_action('organization_show')(context, data_dict)['id']
            except:
                organization = get_action('organization_create')(context, data_dict)
                package_dict['owner_org'] = organization['id']

            # Insert or update the package
            package = model.Package.get(package_dict['id'])
            pkg_role = model.PackageRole(package=package, user=user, role=model.Role.ADMIN)

            # Move file around and make sure it's in the file-store
            for r in package_dict['resources']:
                if r['resource_type'] == 'file':
                    label = package_dict['datasetID'] + '/' + r['name']
                    file_contents = ''
                    with open(os.path.join(self.IMPORT_PATH, package_dict['datasetID'], 'DEFAULT', r['name'])) as contents:
                        file_contents = contents.read()
                    params = {
                        'filename-original': 'the original file name',
                        'uploaded-by': self.config['user']
                    }
                    r['url'] = self.CKAN_SITE_URL + '/storage/f/' + label
                    self.get_ofs().put_stream(self.BUCKET, label, file_contents, params)

            result = self._create_or_update_package(package_dict, harvest_object)
            Session.commit()

        except Exception, e:
            log.exception(e)

        return True

    def _get_metadata(self, file_name):
        return metadata

    def _create_markdown(self, properties):
        markdown = ''
        for key in properties:
            text = self._normalize(properties[key])
            key = self._normalize(key)
            markdown += '#' + key + '\n' + text + '\n'
        return markdown

    def _normalize(self, string):
        if type(string) == unicode:
            return string.encode('utf8', 'ignore')
        else:
            return str(string)
