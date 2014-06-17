from lxml import etree
import logging
log = logging.getLogger(__name__)

class XPathHelper(object):

    namespaces = {
        'sv': "http://www.jcp.org/jcr/sv/1.0",
        'jcr': "http://www.jcp.org/jcr/1.0"
    }

    def __init__(self, xml):
        self.xml = xml

    def element(self, xpath, xml=None):
        if xml is None:
            xml = self.xml
        try:
            value = xml.xpath(xpath, namespaces=self.namespaces)[0]
        except Exception as e:
            value = ''
        return value

    def multielement(self, xpath, xml=None):
        if xml is None:
            xml = self.xml
        try:
            value = xml.xpath(xpath, namespaces=self.namespaces)
        except Exception as e:
            value = ''
        return value

    def text(self, xpath, xml=None):
        value = self.element(xpath, xml)
        return value.text if hasattr(value, 'text') else value

    def tuple_from_nodes(self, xpath, key, value):
        result = []
        nodes = self.multielement(xpath);
        for node in nodes:
            result.append((self.text('./sv:property[@sv:name="%s"]/sv:value' % key, node), self.text('./sv:property[@sv:name="%s"]/sv:value' % value, node)))
        return result

