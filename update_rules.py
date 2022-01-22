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

campRequiredFields = ['offer']

campFields = [
    lib.Field('geo', True, True),
    lib.Field('payout', False, False),
    lib.Field('country', False, False),
    lib.Field('tags', False, False),
    lib.Field('tracking domain', False, False),
    lib.Field('cost model', False, False),
    lib.Field('offer', True, False),
    lib.Field('percent', True, False),
    lib.Field('offer2', True, False),
    lib.Field('percent2', True, False),
    lib.Field('landings', True, False),
    lib.Field('redirect mode', False, False),
    lib.Field('camp name', True, True)
]


def update_rules(filename, allCampaigns, allOffers, allLandings, allCountries):
    dd = lib.read_csv(filename, campRequiredFields, campFields)
    data = []
    row = dd[0]
    original_camp = allCampaigns.get(row['camp name'])
    if original_camp is None:
        logger.error('not found campaign {}'.format(row['offer']))
        return data
    rules = lib.set_rules_by_country(dd, allOffers, allLandings, allCountries)
    payload = {
        "workspaceId": original_camp['workspaceId'],
        "trafficSourceId": original_camp['trafficSourceId'],
        "name": row.get('camp name', original_camp['name']),
        "countryId": original_camp['countryId'],
        "trackingDomainId": original_camp['trackingDomainId'],
        "redirectDomainId": original_camp['redirectDomainId'],
        "trafficLossPercent": original_camp['trafficLossPercent'],
        "postbackPercent": original_camp['trafficLossPercent'],
        "redirectMode": row.get('redirect mode', original_camp['redirectMode']),
        "costValue": float(row.get('campaigncpa', original_camp['costValue'])),
        "costModel": row.get('cost model', original_camp['costModel']).lower(),
        "currencyId": original_camp['currencyId'],
        "destinationType": "flowInline",
        "flowId": original_camp['flowId'],
        "destinationUrl": original_camp['destinationUrl'],
        "status": "active",
        "flowInline": {
            "rules":
                rules
        },
        "enableSixthSense": original_camp['enableSixthSense'],
        "setCustomUniqueness": True,
        "customUniquenessPeriod": 0,
        "uniquenessPeriod": 86400,
        "tags": row.get('tags', '').split(',')
    }
    logger.error('DEBUG CAMPAIGNS BODY!!!')
    logger.error(repr(payload))
    try:
        js = json.dumps(payload)
        resp = lib.post_request("campaigns/{}".format(original_camp['id']), js, "PUT")
        row['camp url'] = resp['campaignUrl']
        data.append(row)
    except Exception as e:
        logger.error('Got exception {}'.format(e))
    return data


def get_filename(suffix=''):
    if len(sys.argv) < 2:
        return lib.get_file_name('rules')
    else:
        dir = config.get('FILES', 'dir')
        fname = sys.argv[1]
        return os.path.join(dir, fname + suffix + '.csv')


class LoaderRules:
    def __init__(self, filename, is_create_new=True):
        self.filename = filename
        self.is_create_new = is_create_new

    def load_file(self):
        logger.info("use filename: {}".format(self.filename))

        allOffers = lib.load_all_offers()
        allCampaigns = lib.load_all_campaigns()
        allLandings = lib.load_landings()
        allCountries = lib.load_countries()
        result = update_rules(self.filename, allCampaigns, allOffers, allLandings, allCountries)
        return result


# file_name_out = get_filename('_out')
#
# print("use filename out: {}".format(file_name_out))
#
# lib.write_csv(file_name_out, result, campFields)

