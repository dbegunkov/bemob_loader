import glob
import json
import ntpath
import os
import shutil
import tempfile
import time
import logging

from utils.Logger import Logger

from utils import lib

import update_rules
import load_campaigns
import load_offers

from utils.config import Config

config = Config.initialiseConfig()
Logger.initialiseLogging('campaigns.log', config)
logger = logging.getLogger('main_script')
Logger.addLoggers([logger])

root = config.get('FILES', 'root_folder')
tmp_folder = tempfile.TemporaryDirectory()
list_folders = json.loads(config.get('FILES', 'scan_folders'))

def get_handler(type_obj, filename):
    if list_folders[type_obj] == 'rules':
        return update_rules.LoaderRules(filename)
    if list_folders[type_obj] == 'campaigns':
        return load_campaigns.LoaderCampaigns(filename)
    if list_folders[type_obj] == 'offers':
        return load_offers.LoaderOffers(filename)
    logger.error('Bad folder name for loading {}'.format(type_obj))


def load_file(file_obj):
    file = file_obj['file']
    type_obj = file_obj['type']
    base_name = ntpath.basename(file)
    dest = os.path.join(tmp_folder.name, base_name)
    logger.info("move to {}".format(dest))
    shutil.move(file, dest)
    handler = get_handler(type_obj, dest)
    if handler:
        handler.load_file()


def scan_folders(folders):
    for folder in folders:
        path = os.path.join(root, folder, '*.csv')
        logger.info('SCANNED PATH {}'.format(path))
        founded_files = glob.glob(path)
        if founded_files:
            return {'type': folder, 'file': founded_files[0]}
    return None




while True:
    files = scan_folders(list_folders)
    if files:
        load_file(files)
    time.sleep(30)
