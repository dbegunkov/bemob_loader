import json
import os
import time
import sys
import logging
from utils.Logger import Logger

from utils import lib

from utils.config import Config

config = Config.initialiseConfig()

Logger.initialiseLogging('campaigns.log', config)
logger = logging.getLogger('Campaigns')

Logger.addLoggers([logger])

campRequiredFields = ['camp name']

campFields = [
    lib.Field('geo', True, True),
    lib.Field('payout', True, True),
    lib.Field('camp name', True, True),
    lib.Field('camp url', True, True),
    lib.Field('trafficsorce', True, True),
    lib.Field('tracking domain', True, True),
    lib.Field('country', True, True),
    lib.Field('tags', True, True),
    lib.Field('cost model', True, True),
    lib.Field('campaigncpa', True, True),
    lib.Field('offer', True, True),
    lib.Field('percent', True, True),
    lib.Field('offer2', True, True),
    lib.Field('percent2', True, True),
    lib.Field('landings', True, True),
    lib.Field('redirect mode', True, True),
    lib.Field('rules', False, False)
]


def load_campaigns(filename, workspaceId, allOffers, allSources, allLandings, allDomains, allCountries):
    dd = lib.read_csv(filename, campRequiredFields, campFields)
    data = []
    for row in dd:
        payload = {
            "workspaceId": workspaceId,
            "trafficSourceId": allSources[row['trafficsorce']],
            "name": row['camp name'],
            "countryId": 0,
            "trackingDomainId": allDomains[lib.clean_domain(row['tracking domain'])],
            "redirectDomainId": None,
            "trafficLossPercent": 0,
            "postbackPercent": 100,
            "redirectMode": row.get('redirect mode', '302'),
            "costValue": float(row.get('campaigncpa', 0)),
            "costModel": row.get('cost model', 'auto').lower(),
            "currencyId": "USD",
            "destinationType": "flowInline",
            "flowId": None,
            "destinationUrl": None,
            "status": "active",
            "flowInline": {
                "rules": lib.set_rules(row, allLandings, allOffers)
            },
            "enableSixthSense": True,
            "setCustomUniqueness": True,
            "customUniquenessPeriod": 0,
            "uniquenessPeriod": 86400,
            "tags": row.get('tags', '').split(',')
        }
        logger.debug('DEBUG CAMPAIGNS BODY!!!')
        logger.debug(repr(payload))
        try:
            js = json.dumps(payload)
            resp = lib.post_request("campaigns", js)
            row['camp url'] = resp['campaignUrl']
            data.append(row)
        except Exception as e:
            logger.error('Got exception {}'.format(e))
        time.sleep(1)
    return data


def get_filename(suffix=''):
    if len(sys.argv) < 2:
        return lib.get_file_name('campaigns')
    else:
        dir = config.get('FILES', 'dir')
        fname = sys.argv[1]
        return os.path.join(dir, fname + suffix + '.csv')


class LoaderCampaigns:
    def __init__(self, filename, is_create_new=True):
        self.filename = filename
        self.is_create_new = is_create_new

    def load_file(self):
        logger.info("use filename: {}".format(self.filename))
        workspace = "Master"
        workspaceId = lib.get_workspace(workspace)
        if workspaceId is None:
            logger.error("Not fond workspace_id by name {}".format(workspace))
            exit(-1)

        logger.info("workspace_id {}".format(workspaceId))

        nameOffers = lib.load_all_offers()
        allLandings = lib.load_landings()
        allSources = lib.load_all_sources()
        allDomains = lib.load_domains()
        allCountries = lib.load_countries()
        result = load_campaigns(self.filename, workspaceId, nameOffers, allSources, allLandings, allDomains,
                                allCountries)

        file_name_out = get_filename('_out')

        logger.info("use filename out: {}".format(file_name_out))

        lib.write_csv(file_name_out, result, campFields)

        return result
