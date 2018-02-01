from typing import Iterable
import requests
import asyncio
import json
import re
import logging

from .scraper_base import *
from .session import GSession


class Scraper(BaseScraper):

    BASE_URL = 'https://opendatabot.com/iframe/search'
    HEADERS = {'User-Agent': 'OD-Bot', 'Host': 'opendatabot.com'}

    def __init__(self, coros_limit=100, r_timeout=5, raise_exceptions=True):
        self.r_timeout = r_timeout
        self.coros_limit = coros_limit
        self.raise_exceptions = raise_exceptions
        self.logger = logging.getLogger('od_scraper')

        self.id = 'od_scraper'

    def find_one(self, org_code: str):
        code_length = len(org_code)
        assert code_length == 8 or code_length == 10, \
            'Organization code should be 8 or 10 chars, instead got {}'.format(code_length)

        req_params = {'text': org_code, 'type': 'code_search', 'start': 0}

        resp = requests.get(self.BASE_URL, headers=self.HEADERS, params=req_params)

        if resp.status_code == 200:
            data = resp.json()
            result = self._process(data, org_code)
            result['org_code'] = org_code
            return result
        else:
            raise ResponseError(' -> Request on {} failed'.format(self.BASE_URL), org_code=org_code)

    def find_bulk(self, org_codes: Iterable):
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

    async def _run_bulk(self, org_codes):
        session = GSession(headers=self.HEADERS)
        semaphore = asyncio.Semaphore(self.coros_limit)

        tasks, result = [], []

        for code in org_codes:
            tasks.append(self._get_data(code, session, semaphore))

        result = await asyncio.gather(*tasks, return_exceptions=True)

        session.close()
        return result

    async def _get_data(self, org_code, session, semaphore):
        data = {org_code: []}
        req_params = {'text': org_code, 'type': 'code_search', 'start': 0}

        try:
            response = await session.get(self.BASE_URL, params=req_params, semaphore=semaphore, timeout=self.r_timeout)
        except Exception as err:
            raise ResponseError(' -> Request on {} failed. Error: {}'.format(self.BASE_URL, err), org_code=org_code)

        if response.content:
            data_raw = json.loads(response.content)
            data = {org_code: data_raw}
        return data

    def _process(self, data, org_code=None):

        result = {'status': -1}

        if not data or data['overall'] == 0:
            result['status'] = 0
            return result

        try:
            firm_data = data['companies'][0]
            result['firm_reg_date'] = ''
            if firm_data.get('edr'):
                result['firm_reg_date'] = firm_data['edr']['registration']['date']
            result['firm_name'] = firm_data['full_name'] \
                if firm_data.get('full_name') else 'Название не определено'
            if not firm_data.get('short_name') and not firm_data.get('full_name'):
                result['firm_name_short'] = 'Название не определено'
            else:
                result['firm_name_short'] = firm_data['short_name'] \
                if firm_data.get('short_name') else firm_data.get('full_name')
            result['firm_ceo'] = firm_data['ceo_name']
            result['firm_location'] = firm_data['location']
            result['firm_status'] = firm_data['status'] \
                if firm_data.get('firm_status') else 'Не определено'
            result['firm_activities'] = firm_data['activities'] \
                if firm_data.get('firm_status') else ''
            result['firm_beneficiaries'] = firm_data['beneficiaries'] \
                if firm_data.get('beneficiaries') else ''

            result['firm_vat'] = 0
            result['firm_vat_code'] = ''
            for item in firm_data['warnings']:
                if item['type'] == 'pdv':
                    if not item.get('date_cancellation'):
                        result['firm_vat'] = 1
                        result['firm_vat_code'] = item['number']

        except Exception as err:
            if self.raise_exceptions:
                raise ProcessingError(err, org_code=org_code)

        result['status'] = 1

        return result

