import os
import sys
import time

from utils import lib
import json
from utils.config import Config
import logging
from utils.Logger import Logger

config = Config.initialiseConfig()
Logger.initialiseLogging('campaigns.log', config)
logger = logging.getLogger('Campaigns')

Logger.addLoggers([logger])

offerRequiredFileds = ['offer name']

offersFields = [
    lib.Field('гео', True, True),
    lib.Field('payout manual', True, True),
    lib.Field('offer name', True, True),
    lib.Field('country', True, True),
    lib.Field('tags', True, True),
    lib.Field('url', True, True)
]


def load_offers(filename, workspace_id):
    dd = lib.read_csv(filename, offerRequiredFileds, offersFields)
    data = []
    for row in dd:
        payload = {
            "workspaceId": workspace_id,
            "name": row["offer name"],
            "url": row["url"],
            "payoutType": "manual",
            "payoutSum": float(row["payout manual"]),
            "clickIdParam": None,
            "countryId": 0,
            "currencyId": "USD",
            "affiliateNetworkId": None,  # network_id
            "enableDailyCap": False,
            "dailyCapLimit": 0,
            "dailyCapTimezone": "UTC",
            "dailyCapFailoverOfferId": None,
            "status": "active",
            "tags": row.get('tags', '').split(',')
        }
        # data.append(payload)
        js = json.dumps(payload)
        try:
            resp = lib.post_request("offers", js)
            data.append(resp)
        except Exception as e:
            logger.error('Got exception {}'.format(e))
        time.sleep(1)
    return data


def get_filename(suffix=''):
    if len(sys.argv) < 2:
        return lib.get_file_name('offers')
    else:
        dir_dest = config.get('FILES', 'dir')
        fname = sys.argv[1]
        return os.path.join(dir_dest, fname + suffix + '.csv')


class LoaderOffers:
    def __init__(self, filename):
        self.filename = filename

    def load_file(self):
        logger.info("use filename: {}".format(self.filename))

        workspace = "Master"
        workspaceId = lib.get_workspace(workspace)
        if workspaceId is None:
            logger.error("Not fond workspace_id by name {}".format(workspace))
            exit(-1)

        logger.info("workspace_id {}".format(workspaceId))

        return load_offers(self.filename, workspaceId)
