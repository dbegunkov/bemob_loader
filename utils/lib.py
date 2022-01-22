import http.client
import json
import csv
import os
import re
import logging
from utils.Logger import Logger

from utils.config import Config

config = Config.initialiseConfig()
Logger.initialiseLogging('campaigns.log', config)
logger = logging.getLogger('Campaigns')

Logger.addLoggers([logger])


class Field:
    def __init__(self, name, required, is_out):
        self.name = name
        self.required = required
        self.is_out = is_out


def read_csv(file_name, nonNullFields, allFields=[]):
    with open(file_name, newline='') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        num = 0
        headers = {}
        numers = {}
        outList = []
        for row in spamreader:
            num = num + 1
            if num == 1:
                headers = dict.fromkeys(to_low(row))
                c = 0
                for col in row:
                    col = col.lower().strip()
                    if col == '' or not field_exist(col, allFields):
                        c = c + 1
                        logger.debug("skip field {}".format(col))
                        continue
                    headers[col] = c
                    numers[c] = col
                    c = c + 1
                if not check_headers(headers, allFields):
                    logger.error('Check header! Must be {}'.format(to_names(allFields)))
                    logger.error('Got {}'.format(headers))
                    exit(-1)
                continue
            if not check_empty_fields(nonNullFields, row, headers):
                continue
            # print(', '.join(row))
            outDict = {}
            c = 0
            for val in row:
                name = get_name_column(c, numers)
                if name is None:
                    c = c + 1
                    continue
                outDict[name] = val
                c = c + 1
            outList.append(outDict)
    return outList

def to_names(allFields):
    out = []
    for row_str in allFields:
        out.append(row_str.name)
    return out

def field_exist(col, allFields):
    if not allFields:
        return True
    for field in allFields:
        if field.name == col:
            return True
    return False


def to_low(headers):
    out = []
    for str1 in headers:
        out.append(str1.lower().strip())
    return out


def check_headers(headers, allFields):
    for col in allFields:
        if (headers.get(col.name) is None) and col.required:
            logger.error("not found filed {}".format(col.name))
            return False
    return True


def write_csv(filename, data, headers):
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = get_out_fields(headers)
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            row1 = delete_extra_keys(row, fieldnames)
            writer.writerow(row1)


def delete_extra_keys(row, fieldnames):
    out = {}
    for key in row:
        if key in fieldnames:
            out[key] = row[key]
    return out


def get_out_fields(fields):
    out = []
    for field in fields:
        if field.is_out:
            out.append(field.name)
    return out


def check_empty_fields(nonNullFields, row, headers):
    for field in nonNullFields:
        if not row[get_num_column(field, headers)]:
            return False
    return True


def get_num_column(name, headers):
    return headers[name]


def get_name_column(num, numbers):
    return numbers.get(num)


def getRequest(url, debug=False):
    conn = http.client.HTTPSConnection('api.bemob.com')
    conn.request("GET", "/v1/" + url,
                 headers={'X-ACCESS-KEY': config.get('AUTH', 'access_key'),
                          'X-SECRET-KEY': config.get('AUTH', 'secret_key')})
    r1 = conn.getresponse()
    if r1.status != 200:
        logger.error("Bad response. code {}. Status {}".format(r1.status, r1.reason))
        exit(-1)
    body = r1.read()
    # print(repr(body))
    js = json.loads(body)
    # print(repr(js))
    if not js.get('success'):
        logger.error("Response isn't success. Code {}".format(js.get('success')))
        exit(-1)

    payload = js.get('payload')
    if debug:
        for cam in payload:
            for key in cam:
                value = cam.get(key)
                print("{}: {}".format(key, value))
    else:
        return payload


def get_workspace(name):
    payload = getRequest("/user/workspaces")
    for row in payload:
        if row.get("name") == name:
            return row.get("id")
    return None


def get_network(name):
    payload = getRequest("/affiliate-networks?status=active")
    for row in payload:
        if row.get("name") == name:
            return row.get("id")
    return None


def load_all_sources():
    payload = getRequest("/traffic-sources?status=active")
    out = {}
    for row in payload:
        out[row.get("name")] = row.get("id")
    return out


def load_all_offers():
    payload = getRequest("/offers?status=active")
    out = {}
    for row in payload:
        out[row.get("name")] = row.get("id")
    return out


def post_request(url, data, method="POST"):
    logger.debug("---POST REQUEST--- {} ".format(url))
    conn = http.client.HTTPSConnection('api.bemob.com')
    conn.set_debuglevel(10)
    conn.request(method, "/v1/" + url, body=data,
                 headers={'X-ACCESS-KEY': config.get('AUTH', 'access_key'),
                          'X-SECRET-KEY': config.get('AUTH', 'secret_key'),
                          'Content-Type': 'application/json',
                          'accept': 'application/json'})
    r1 = conn.getresponse()
    if r1.status != 200:
        logger.error("Bad response on post request. code {}. Status {}".format(r1.status, r1.reason))
        body = r1.read()
        logger.error(repr(body))
        raise ValueError('Bab response.')
    body = r1.read()
    # print(repr(body))
    js = json.loads(body)
    # print(repr(js))
    if not js.get('success'):
        logger.error("Response on post request isn't success. Code {}".format(js.get('success')))
        raise ValueError('Response failed.')

    logger.debug("---POST RESPONSE---")
    logger.debug(repr(js))
    logger.debug("------")
    return js.get("payload")


def get_file_name(section, suffix=''):
    fname = config.get('FILES', section)
    dir = config.get('FILES', 'dir')
    return os.path.join(dir, fname + suffix + '.csv')


def get_out_file_name(section, suffix=''):
    fname = config.get('FILES', section)
    dir = config.get('FILES', 'out_dir')
    return os.path.join(dir, fname + suffix + '.csv')


def compact_offers(offersRes):
    out_dict = {}
    for row in offersRes:
        out_dict[row.get("name")] = row.get("id")
    return out_dict


def make_offers(row, nameOffers):
    list = []
    if row.get('offer') != None and row.get('offer') != '':
        list.append({
            "id": nameOffers[row['offer']],
            "weight": int(row['percent'])
        })
    if row.get('offer2') != None and row.get('offer2') != '':
        list.append({
            "id": nameOffers[row['offer2']],
            "weight": calc_percent2(row)
        })
    return list


def make_offers_by_conutry(offer_name, nameOffers):
    return [
        {
            "id": nameOffers[offer_name],
            "weight": 100
        }
    ]


def calc_percent2(row):
    if row.get('percent2', '') == '':
        0
    else:
        int(row['percent2'])


def set_rules(row, allLandings, nameOffers):
    rules = [
        {
            "name": "Default",
            "default": True,
            "conditions": {},
            "status": "active",
            "paths": [
                make_rules(row, allLandings, nameOffers)
            ]
        }
    ]
    return rules


def make_rules(row, allLandings, nameOffers):
    landings = make_landings(row, allLandings)
    rules = {
        "name": "Path 1",
        "weight": 100,
        "status": "active",
        "redirectMode": "302",
        "redirectDomain": "",
        "directLinking": set_direct(landings),
        "landings": landings,
        "offers": make_offers(row, nameOffers)
    }
    return rules


def set_rules_by_country(rows, allOffers, allLandings, allCountries):
    landings = make_landings(rows[0], allLandings)
    offers = make_offers(rows[0], allOffers)
    rules = [
        {
            "name": "Default",
            "default": True,
            "status": "active",
            "conditions": {},
            "paths": [
                {
                    "name": "Path 1",
                    "directLinking": set_direct(landings),
                    "deleted": "0",
                    "redirectDomainId": "",
                    "redirectMode": "302",
                    "status": "active",
                    "weight": 100,
                    "landings": landings,
                    "offers": offers,
                }
            ]
        }
    ]
    for row in rows:
        geo = row['geo']
        if allCountries.get(geo) == None:
            logger.error('BAD COUNTRIES CODE {}'.format(geo))
            continue
        add_rule(geo, row, rules, allOffers, allLandings)
    return rules


def load_countries():
    payload = getRequest("/dictionaries/countries")
    out = {}
    for row in payload:
        out[row.get("code")] = row.get("id")
    return out


def extract_country(offer):
    return offer[-2:]


def add_rule(geo, row, rules, allOffers, allLandings):
    landings = make_landings(row, allLandings)
    offers = make_offers(row, allOffers)
    if geo != '':
        rules.append({
            "conditions": {
                "country": {
                    "predicate": "EQUALS",
                    "values": [
                        geo
                    ]
                }
            },
            "name": "",
            "status": "active",
            "default": False,
            "logicalRelation": "and",
            "paths": [
                {
                    "name": "Path 1",
                    "weight": 100,
                    "status": "active",
                    "redirectMode": "302",
                    "redirectDomain": "",
                    "directLinking": set_direct(landings),
                    "landings": landings,
                    "offers": offers
                }
            ]
        })


def set_direct(landings):
    if not landings:
        return True
    else:
        return False


def make_landings(row, allLandings):
    list = []
    if row.get('landings') != None and row.get('landings') != '':
        list.append({
            "id": allLandings[row['landings']],
            "weight": "1"
        })
    return list


def clean_domain(str):
    m = re.search('\/\/([^\/]*)', str)
    logger.debug('DOMAIN {}'.format(m.group(1)))
    return m.group(1)


def load_landings():
    payload = getRequest("/landings?status=active")
    out = {}
    for row in payload:
        out[row.get("name")] = row.get("id")
    return out


def load_all_campaigns():
    payload = getRequest("/campaigns?status=active")
    out = {}
    for row in payload:
        out[row.get("name")] = row
    return out


def load_domains():
    payload = getRequest("/user/tracking-domains")
    out = {}
    for row in payload:
        out[row.get("domain")] = row.get("id")
    return out
