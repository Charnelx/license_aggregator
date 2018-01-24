from typing import Iterable, NewType
import requests
import datetime
import asyncio
import json
import time
import logging
import re
from lxml import html

from .scraper_base import *
from .session import GSession

TSession = NewType('Session', object)


class Scraper(BaseScraper):

    ENTRY_URL = 'https://uakey.com.ua/ua/setificate-one-office/text=3&page=1'
    REQUEST_URL = 'https://uakey.com.ua/inc/sertificate_from_edrpo.php'

    HEADERS = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded',
            'Host': 'uakey.com.ua',
            'Origin': 'https://uakey.com.ua',
            'Referer': 'https://uakey.com.ua/ua/setificate-one-office/text=3&page=1?lang=ukr',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko)'
                          ' Chrome/63.0.3239.132 Safari/537.36 OPR/50.0.2762.67',
    }

    def __init__(self, coros_limit=100, r_timeout=5, raise_exceptions=True):
        self.r_timeout = r_timeout
        self.coros_limit = coros_limit
        self.raise_exceptions = raise_exceptions
        self.logger = logging.getLogger('key_scraper')

        self.id = 'keys_scraper'

    def find_one(self, org_code: str) -> dict:
        code_length = len(org_code)
        assert code_length == 8 or code_length == 10, \
            'Organization code should be 8 or 10 chars, instead got {}'.format(code_length)

        session = requests.Session()
        session.headers = self.HEADERS

        # Retrieve page with keys JSON data
        data = {
            'SUBJECTORGNAME': '',
            'SERIALNUMBER': '',
            'ORGEDRPOUNUMBER': org_code,
            'search': 'пошук',
        }
        resp = session.post(self.ENTRY_URL, params={'lang':'ukr'}, data=data)

        if resp.status_code == 200:
            data = resp.text
            result = self._process(data, org_code)
            result['org_code'] = org_code
            return result
        else:
            raise ResponseError(' -> Request on {} failed'.format(self.REQUEST_URL), org_code=org_code)

    def find_bulk(self, org_codes: Iterable) -> list:
        assert hasattr(org_codes, '__iter__'), 'Container for org_codes should be iterable'

        results = []

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        results_raw = loop.run_until_complete(self._run_bulk(org_codes))

        loop.close()

        for item in results_raw:
            # hack to obtain org_code from failed request
            if isinstance(item, Exception):
                firm_code_group = re.search(pattern_exc_firm_code, str(item))
                if firm_code_group:
                    data = {'org_code': firm_code_group.group(1), 'status': -1}
                    results.append(data)
                    self.logger.error('Error with code - {}', data['org_code'])
                else:
                    self.logger.error('Error with unknown org_code')
            else:
                for org_code, data_raw in item.items():
                    data = self._process(data_raw, org_code)
                    data['org_code'] = org_code
                    results.append(data)

        return results

    async def _run_bulk(self, org_codes: list) -> list:
        session = GSession(headers=self.HEADERS)
        semaphore = asyncio.Semaphore(self.coros_limit)

        tasks, result = [], []

        for code in org_codes:
            tasks.append(self._get_data(code, session, semaphore))

        result = await asyncio.gather(*tasks, return_exceptions=True)

        session.close()
        return result

    async def _get_data(self, org_code: str, session: TSession, semaphore):

        try:
            data = {
                'SUBJECTORGNAME': '',
                'SERIALNUMBER': '',
                'ORGEDRPOUNUMBER': org_code,
                'search': 'пошук',
            }

            response = await session.post(self.ENTRY_URL, params={'lang':'ukr'}, data=data)
        except Exception as err:
            raise ResponseError(' -> Request on {} failed. Error: {}'.format(self.REQUEST_URL, err), org_code=org_code)

        if response.content:
            data_raw = response.content
            data = {org_code: data_raw}
        return data

    def _process(self, data, org_code=None):

        result = {'status': -1}

        try:
            root = html.fromstring(data)
            script_node = root.xpath('//td[@class="str_4_3"]/script')[1]
            data = script_node.text.split('=', maxsplit=1)[1].lstrip().replace("'", "")[:-1]
        except:
            return result

        try:
            data = json.loads(data)
        except Exception as err:
            return result
        if not data or len(data[org_code]['id']) == 0:
            result['status'] = 0
            return result

        try:
            items_q = len(data[org_code]['id'])
            certs = []
            for idx in range(items_q):
                date_start = data[org_code]['start'][idx]
                date_end = data[org_code]['end'][idx]

                certs.append({
                    'owner': data[org_code]['text'][idx].replace('&quot;', '"'),
                    'id': data[org_code]['id'][idx],
                    'date_start': datetime.datetime.strptime(date_start, '%d.%m.%y'),
                    'date_end': datetime.datetime.strptime(date_end, '%d.%m.%y'),
                    'crypt_status': data[org_code]['forcript'][idx]
                })

            result['certs'] = certs

        except Exception as err:
            if self.raise_exceptions:
                raise ProcessingError(err, org_code=org_code)

        result['status'] = 1

        return result
