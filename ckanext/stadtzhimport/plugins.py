import ckan
import ckan.plugins as p
from pylons import config

class StadtzhimportHarvest(p.SingletonPlugin):
    """
    Plugin containing the harvester for StadtzhimportHarvester
    """
