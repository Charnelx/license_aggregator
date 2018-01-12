from typing import Iterable, NewType
import requests
import datetime
import asyncio
import json
import time
import logging
import re

from scraper_base import *
from session import GSession

TSession = NewType('Session', object)


class Scraper(BaseScraper):

    ENTRY_URL = 'http://uakey.com.ua/ua/setificate-one-office/text=3&page=1?lang=ukr#blocy'
    REQUEST_URL = 'http://uakey.com.ua/inc/sertificate_from_edrpo.php'

    HEADERS = {
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate',
            'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.6,en;q=0.4',
            'Connection': 'keep-alive',
            'Content-Type': 'application/octet-stream',
            'Host': 'uakey.com.ua',
            'Origin': 'http://uakey.com.ua',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3013.3 Safari/537.36',
    }

    def __init__(self, coros_limit=100, r_timeout=5, raise_exceptions=True):
        self.r_timeout = r_timeout
        self.coros_limit = coros_limit
        self.raise_exceptions = raise_exceptions
        self.logger = logging.getLogger('key_scraper')

    def find_one(self, org_code: str) -> dict:
        code_length = len(org_code)
        assert code_length == 8 or code_length == 10, \
            'Organization code should be 8 or 10 chars, instead got {}'.format(code_length)

        session = requests.Session()
        session.headers = self.HEADERS

        # Preparations to retrieve SSID
        session.post(self.ENTRY_URL, data={'ORGEDRPOUNUMBER': org_code})
        ssid = session.cookies['PHPSESSID']
        epoch_time = int(time.time())

        # Retrieve keys info
        req_params = {'PHPSESSID': ssid, 'JsHttpRequest': '{}{}'.format(epoch_time, '0-xml')}
        req_data = {'SertEdrpo': org_code}

        resp = session.post(self.REQUEST_URL, params=req_params, data=req_data)

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
                self.logger.error(item)
                firm_code_group = re.search(pattern_exc_firm_code, str(item))
                if firm_code_group.group():
                    data = {'org_code': firm_code_group.group(1), 'status': -1}
                    results.append(data)
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

        ssid = await self._get_ssid(session, '123456789199')

        for code in org_codes:
            tasks.append(self._get_data(code, ssid, session, semaphore))

        result = await asyncio.gather(*tasks, return_exceptions=True)

        session.close()
        return result

    async def _get_ssid(self, session: TSession, org_code: str) -> str:
        response = await session.post(self.ENTRY_URL, data={'ORGEDRPOUNUMBER': org_code})
        ssid = response.cookies.get('PHPSESSID')
        return ssid.value

    async def _get_data(self, org_code: str, ssid: str, session: TSession, semaphore):
        epoch_time = int(time.time())
        req_params = {'PHPSESSID': ssid, 'JsHttpRequest': '{}{}'.format(epoch_time, '0-xml')}
        req_data = {'SertEdrpo': org_code}

        try:
            response = await session.post(self.REQUEST_URL, params=req_params, data=req_data, semaphore=semaphore,
                                          timeout=self.r_timeout)
        except Exception as err:
            raise ResponseError(' -> Request on {} failed. Error: {}'.format(self.REQUEST_URL, err), org_code=org_code)

        if response.content:
            data_raw = response.content
            data = {org_code: data_raw}
        return data

    def _process(self, data, org_code=None):

        result = {'status': -1}

        try:
            data = json.loads(data)
        except:
            return result

        if not data or data['js']['id'][0] == '':
            result['status'] = 0
            return result

        try:
            items_q = len(data['js']['id'])
            certs = []
            for idx in range(items_q):
                date_start = data['js']['start_date'][idx]
                date_end = data['js']['end_date'][idx]

                certs.append({
                    'owner': data['js']['text'][idx].replace('&quot;', '"'),
                    'id': data['js']['id'][idx],
                    'date_start': datetime.datetime.strptime(date_start, '%d.%m.%y'),
                    'date_end': datetime.datetime.strptime(date_end, '%d.%m.%y'),
                    'crypt_status': data['js']['FORCRYPT'][idx]
                })

            result['certs'] = certs

        except Exception as err:
            if self.raise_exceptions:
                raise ProcessingError(err, org_code=org_code)

        result['status'] = 1

        return result




