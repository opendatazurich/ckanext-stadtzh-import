#coding: utf-8

import os
import traceback
import base64
import re
os.environ['http_proxy'] = ''
import httplib
import urllib2
import datetime
import html2text
import string
from lxml import etree
from pprint import pprint
from pylons import config
from ckan.model import Session
from ckan.lib.helpers import json
from ckanext.stadtzhimport.helpers.xpath import XPathHelper
from ckanext.stadtzhharvest.harvester import StadtzhHarvester

import logging
log = logging.getLogger(__name__)


class StadtzhimportHarvester(StadtzhHarvester):
    '''
    The harvester to harvest the existing data portal of the Stadt ZH
    '''

    DATA_PATH = '/usr/lib/ckan/cms_stzh_ch_content_portal_de_index_ogd'
    PERMALINK_FORMAT = 'http://data.stadt-zuerich.ch/ogd.%s.link'

    DATENLIEFERANTEN = {
        'ogdprovider':    u'Statistik Stadt Zürich',
        'ogdprovider_0':  u'Geomatik + Vermessung',
        'ogdprovider_1':  u'Entsorgung + Recycling Zürich',
        'ogdprovider_2':  u'Umwelt- und Gesundheitsschutz Zürich',
        'ogdprovider_3':  u'Stadt Zürich Finanzverwaltung',
        'ogdprovider_4':  u'Tiefbauamt, Abteilung Mobilität + Verkehr',
        'ogdprovider_5':  u'Abteilung Bewilligungen der Stadtpolizei Zürich',
        'ogdprovider_6':  u'Grün Stadt Zürich'
    }

    GROUP_NAMES = {
        u'basiskarten': u'Basiskarten',
        u'bauen-wohnen': u'Bauen und Wohnen',
        u'bevoelkerung': u'Bevölkerung',
        u'bildung': u'Bildung',
        u'freizeit': u'Freizeit',
        u'gesundheit': u'Gesundheit',
        u'kultur': u'Kultur',
        u'mobilitaet': u'Mobilität',
        u'politik': u'Politik',
        u'soziales': u'Soziales',
        u'umwelt': u'Umwelt',
        u'verwaltung': u'Verwaltung',
        u'wirtschaft': u'Wirtschaft'
    }

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

        try:
            with open(os.path.join(self.DATA_PATH, 'cms_stzh_ch_content_portal_de_index_ogd_systemView.xml'), 'r') as cms_file:
                parser = etree.XMLParser(encoding='utf-8', ns_clean=True)
                datasets = XPathHelper(etree.fromstring(cms_file.read(), parser=parser)).multielement('.//sv:node[@sv:name="daten"]/sv:node')

                for dataset in datasets:
                    if XPathHelper(dataset).text('.//sv:property[@sv:name="jcr:primaryType"]/sv:value') == 'cq:Page' and\
                       XPathHelper(dataset).text('.//sv:property[@sv:name="cq:lastReplicationAction"]/sv:value') != 'Deactivate':
                        xpath = XPathHelper(dataset)
                        datasetID = xpath.text('./@sv:name')
                        metadata = self._get_metadata(datasetID, xpath)
                        id = self._save_harvest_object(metadata, harvest_job)
                        ids.append(id)

            return ids
        except Exception, e:
            log.exception(e)
            self._save_gather_error(
                'Unable to get content from folder: %s: %s / %s'
                % (self.DATA_PATH, str(e), traceback.format_exc()),
                harvest_job
            )
	    return []

    def fetch_stage(self, harvest_object):
        log.debug('In StadtzhimportHarvester fetch_stage')
        return self._fetch_datasets(harvest_object)

    def import_stage(self, harvest_object):
        log.debug('In StadtzhimportHarvester import_stage')
        return self._import_datasets(harvest_object)

    def _get_metadata(self, datasetID, xpath):

        return {
            'datasetID': datasetID,
            'title': xpath.text('.//sv:property[@sv:name="jcr:title"]/sv:value'),
            'url': self._lookup_datenlieferant(xpath),
            'author': xpath.text('.//sv:property[@sv:name="source"]/sv:value'),
            'maintainer': 'Open Data Zürich',
            'maintainer_email': 'opendata@zuerich.ch',
            'license_id': 'cc-zero',
            'license_url': 'http://opendefinition.org/licenses/cc-zero/',
            'tags': self._generate_tags_array(xpath),
            'groups': self._get_groups(xpath),
            'resources': self._generate_resources_dict_array(xpath, datasetID),
            'notes': self._convert_base64(xpath.text('.//sv:property[@sv:name="jcr:description"]/sv:value')),
            'extras': [
                ('spatialRelationship', self._convert_base64(xpath.text('.//sv:property[@sv:name="referencePlane"]/sv:value'))),
                ('dateFirstPublished', self._convert_iso_date(
                    self._convert_base64(xpath.text('.//sv:property[@sv:name="creationDate"]/sv:value')))),
                ('dateLastUpdated', self._convert_iso_date(
                    self._convert_base64(xpath.text('.//sv:property[@sv:name="modificationDate"]/sv:value')))),
                ('version', self._convert_base64(xpath.text('.//sv:property[@sv:name="version"]/sv:value'))),
                ('updateInterval', self._decode(xpath.text('.//sv:property[@sv:name="updateInterval"]/sv:value')).replace('_', ' oder ')),
                ('timeRange', self._convert_base64(xpath.text('.//sv:property[@sv:name="timeRange"]/sv:value'))),
                ('dataType', string.capwords(self._decode(xpath.text('.//sv:property[@sv:name="datatype"]/sv:value')), '-')),
                ('legalInformation', self._convert_base64(xpath.text('.//sv:property[@sv:name="legalInformation"]/sv:value'))),
                ('sszBemerkungen', self._convert_markdown(self._convert_base64(xpath.text('.//sv:property[@sv:name="comments"]/sv:value')), datasetID)),
                ('sszFields', self._json_encode_attributes(self._get_attributes(xpath))),
                ('dataQuality', self._convert_base64(xpath.text('.//sv:property[@sv:name="quality"]/sv:value')))
            ],
            'related': self._get_related(xpath)
        }

    def _generate_resources_dict_array(self, xpath, datasetID):
        '''
        Given the xpath of a dataset return an array of resource metadata
        '''
        resources = []

        for file in xpath.multielement('.//sv:node[@sv:name="data"]/*[starts-with(@sv:name, "ogdfile")]'):
            element = XPathHelper(file)

            if element.text('./sv:property[@sv:name="fileName"]/sv:value'):
                datasetID = self._validate_package_id(datasetID)
                file_name = self._validate_filename(element.text('./sv:property[@sv:name="fileName"]/sv:value'))
                url = self._generate_permalink(element.text('./sv:property[@sv:name="permalinkid"]/sv:value'))

                if datasetID and file_name and url:
                    path = os.path.join(self.DATA_PATH, datasetID)

                    if self._download_file(url, path, file_name):
                        resources.append({
                            # 'url': '', # will be filled in the import stage
                            'name': file_name,
                            'format': file_name.split('.')[-1],
                            'resource_type': 'file'
                        })

        for link in xpath.multielement('.//sv:node[@sv:name="data"]/*[starts-with(@sv:name, "ogdlink")]'):
            element = XPathHelper(link)
            if element.text('./sv:property[@sv:name="text"]/sv:value'):
                resources.append({
                    'url': element.text('./sv:property[@sv:name="link"]/sv:value'),
                    'name': element.text('./sv:property[@sv:name="text"]/sv:value'),
                    'format': element.text('./sv:property[@sv:name="dataformat"]/sv:value').split('/')[-1],
                    'resource_type': 'api'
                })

        sorted_resources = sorted(resources, cmp=lambda x, y: self._sort_resource(x, y))
        return sorted_resources

    def _generate_tags_array(self, xpath):
        '''
        All tags for a dataset into an array
        '''
        tags = []
        values = self._convert_base64(xpath.text('.//sv:property[@sv:name="metaTagKeywords"]/sv:value'))
        try:
            if values:
                for tag in values.split(','):
                    tag_stripped = tag.rstrip()
                    if tag_stripped:
                        tags.append(tag_stripped)
        except AttributeError:
            pass

        return tags

    def _lookup_datenlieferant(self, xpath):
        lieferant = ''
        try:
            provider = re.match(".*\/(.*)$", xpath.text('.//sv:property[@sv:name="providerPath"]/sv:value')).group(1)
            lieferant = self.DATENLIEFERANTEN[provider]
        except:
            log.debug('datenlieferant not found')
        return lieferant

    def _get_attributes(self, xpath):
        result = []
        nodes = xpath.multielement('.//sv:node[@sv:name="attributes"]/sv:node')
        for node in nodes:
            tech = xpath.text('./sv:property[@sv:name="fieldname_tech"]/sv:value', node)
            clear = xpath.text('./sv:property[@sv:name="fieldname_clear"]/sv:value', node)
            value = xpath.text('./sv:property[@sv:name="field_description"]/sv:value', node)
            if clear:
                name = '%s (technisch: %s)' % (clear, tech)
            else:
                name = tech
            result.append((name, value))
        return result

    def _get_groups(self, xpath):
        '''
        Get the groups from the node, normalize them and get the ids.
        '''

        categories = xpath.multielement('.//sv:property[@sv:name="category"]/sv:value')
        groups = []

        for element in categories:
            category = element.text
            log.debug(category)
            match = re.search(r'^ogd_category:thema/(.*)$', category)
            if match:
                group_name = match.group(1)
                if group_name == 'bauen_und_wohnen':
                    group_name = 'bauen-wohnen'
                elif group_name == 'umwelt_und_verkehr':
                    group_name = 'umwelt'

            basiskarten_match = re.search(r'^ogd_category:inhaltstyp/(.*)$', category)
            if basiskarten_match:
                group_name = basiskarten_match.group(1)
                log.debug(group_name)

            groups.append((group_name, self.GROUP_NAMES[group_name]))

        return self._get_group_ids(groups)

    def _json_encode_attributes(self, properties):
        attributes = []
        for key, value in properties:
            if value:
                value = self._normalize(self._convert_base64(value))
                key = self._normalize(key)
                attributes.append((key, value))

        return json.dumps(attributes)

    def _get_related(self, xpath):
        related = []

        translations = {
            'applications': 'Applikation',
            'publications': 'Publikation'
        }

        for type in translations.keys():
            for value in xpath.multielement('.//sv:property[@sv:name="' + type + '"]/sv:value'):
                if value.text is not None:
                    # Set title
                    if re.match(".*/(.*)$", value.text):
                        title = re.match(".*/(.*)$", value.text).group(1)
                        if title == 'visualisierung-des-zuercher-budgets':
                            title = 'zuercher-budget'
                    else:
                        title = value.text
                        log.debug('Using url as related item title for value: %s' % title)
                    # Get rest of dictionary for related item
                    if re.match("/content/portal/de/index/ogd/anwendungen/.*$", value.text):
                        data_dict = self._get_related_onportal_dict(title)
                        data_dict['type'] = translations[type]
                    else:
                        data_dict = {
                            'title': title,
                            'type': translations[type],
                            'url': self._fix_related_url(value.text)
                        }
                    related.append(data_dict)

        return related

    def _fix_related_url(self, raw):
        url = ''
        try:
            m = re.match('/content/(.*)', raw)
            if m:
                url = 'https://www.stadt-zuerich.ch/' + m.group(1) + '.html'
            else:
                url = 'http://' + raw
        except:
            log.debug('Failed to fix url "%s"' % raw)

        return url

    def _get_related_onportal_dict(self, title):
        log.debug('Getting related item info from Anwendungen page.')
        log.debug(title)
        with open(os.path.join(self.DATA_PATH, 'cms_stzh_ch_content_portal_de_index_ogd_systemView.xml'), 'r') as cms_file:
            parser = etree.XMLParser(encoding='utf-8', ns_clean=True)
            applications  = XPathHelper(etree.fromstring(cms_file.read(), parser=parser)).multielement('.//sv:node[@sv:name="anwendungen"]/sv:node')
            for app_type in applications:
                if XPathHelper(app_type).text('.//sv:property[@sv:name="jcr:primaryType"]/sv:value') == 'cq:Page':
                    for app in app_type:
                        xpath = XPathHelper(app)
                        if xpath.text('./@sv:name') == title:
                            title = xpath.text('.//sv:property[@sv:name="jcr:title"]/sv:value')
                            description = xpath.text('.//sv:property[@sv:name="jcr:description"]/sv:value')
                            url = 'http://data.stadt-zuerich.ch/portal/de/index/ogd/anwendungen/' + XPathHelper(app_type).text('./@sv:name') + '/' + xpath.text('./@sv:name') + '.html'
                            data_dict = {
                                'title': title,
                                'description': description,
                                'url': url
                            }
                            log.debug(data_dict)
                            return data_dict

    def _generate_permalink(self, id):
        '''
        Validate the permalink id and return full permalink
        '''
        match = re.match('^[\w]+$', id)
        if not match:
            log.debug('Permalink id %s contains disallowed characters' % id)
            return False
        else:
            return self.PERMALINK_FORMAT % id

    def _download_file(self, url, path, file_name):
        '''
        Try to download the file and return True on success, False on failure
        '''

        if not os.path.exists(self.DATA_PATH):
            raise Exception('Importer path "%s" doesn\'t exist. Cannot proceed.' % self.DATA_PATH)

        # TODO remove the check again
        if not os.path.exists(os.path.join(path, file_name)):
            try:
                request = urllib2.Request(url, headers={"User-Agent": "curl"})
                contents = urllib2.urlopen(request)
            except (urllib2.HTTPError, httplib.BadStatusLine) as e:
                log.debug('Downloading "%s" failed with error code "%s".' % (url, e.code))
                return False

            if not os.path.exists(path):
                os.makedirs(path)

            with open(os.path.join(path, file_name), 'wb') as f:
                f.write(contents.read())

        return True

    def _normalize(self, string):
        # convert strings like 'ogd_datatype:datenaggregat' to 'Datenaggregat'
        match = re.search(r'^ogd_.*:(.*)$', string)
        if match:
            string = match.group(1).capitalize()
        if type(string) == unicode:
            return string.encode('utf8', 'ignore')
        else:
            return str(string)

    def _decode(self, string):
        result = self._convert_base64(string)
        try:
            return re.match("^ogd_.*:(.*)$", result).group(1)
        except:
            return result

    def _convert_iso_date(self, ts):
        try:
            date = datetime.datetime.strptime(ts[:-7], '%Y-%m-%dT%H:%M:%S.%f') + \
                datetime.timedelta(hours=int(ts[-5:-3]),
                                   minutes=int(ts[-2:]))*int(ts[-6:-5]+'1')
            return date.strftime("%d.%m.%Y, %H:%M")
        except:
            return ts

    def _convert_base64(self, string):
        '''
        If the given string is base64 encoded, decode it
        '''
        try:
            # try to decode base64, if it fails, carry on
            decoded = base64.b64decode(string)
            # base64 decoding worked, now try to decode the result as utf8
            # if this fails the original string was not really base64
            decoded.decode('utf8')
            return decoded
        except:
            return string

    def _convert_markdown(self, string, datasetID):
        try:
            # if the link text is the same as the href, strip off the http://, otherwise html2text returns a link like this: <foo.com>
            m = re.search('^(.*>)(http:\/\/)(.*)', string, re.DOTALL)
            if m:
                string = m.group(1) + m.group(3)

            # some of the comments have broken html in them: get rid of tags like <//a> or html2text throws an exception
            m = re.search('^(.*)(<\/\/\w*>)(.*)', string, re.DOTALL)
            if m:
                string = m.group(1) + m.group(3)

            h = html2text.HTML2Text(bodywidth=0)
            return h.handle(string)
        except Exception, e:
            log.debug('Error converting markdown for dataset %s' % datasetID)
            log.exception(e)
            return string
