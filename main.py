from typing import Iterable
import requests
from collections import defaultdict
import datetime
import asyncio
import json

from session import GSession


class ResponseError(Exception):
    pass


class ProcessingError(Exception):
    pass


class Scraper(object):

    BASE_URL = 'https://api.medoc.ua/lic/key_medoc_test.php'
    LIC_TYPES = defaultdict(lambda : 'Unknown')
    LIC_TYPES.update({'12': 'Local', '13': 'Network'})

    def __init__(self, coros_limit=100, r_timeout=5, raise_exceptions=True):
        self.r_timeout = r_timeout
        self.coros_limit = coros_limit
        self.raise_exceptions = raise_exceptions

    def find_one(self, org_code: str):
        # TODO: add headers
        code_length = len(org_code)
        assert code_length == 8 or code_length == 10, \
            'Organization code should be 8 or 10 chars, instead got {}'.format(code_length)

        req_params = {'edrpo': org_code, 'type': 'json'}

        resp = requests.get(self.BASE_URL, params=req_params)

        if resp.status_code == 200:
            data = resp.json()
            result = self._process(data)
            result['org_code'] = org_code
            return result
        else:
            raise ResponseError

    def find_bulk(self, org_codes: Iterable):
        assert hasattr(org_codes, '__iter__'), 'Container for org_codes should be iterable'

        results = []

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        results_raw = loop.run_until_complete(self._run_bulk(org_codes))

        loop.close()

        for item in results_raw:
            for org_code, data_raw in item.items():
                data = self._process(data_raw)
                data['org_code'] = org_code
                results.append(data)

        return results

    async def _run_bulk(self, org_codes):
        # TODO: add headers
        session = GSession()
        semaphore = asyncio.Semaphore(self.coros_limit)

        tasks, result = [], []

        for code in org_codes:
            tasks.append(self._get_data(code, session, semaphore))

        result = await asyncio.gather(*tasks, return_exceptions=True)

        session.close()
        return result

    async def _get_data(self, org_code, session, semaphore):
        data = {org_code: []}
        req_params = {'edrpo': org_code, 'type': 'json'}

        response = await session.get(self.BASE_URL, params=req_params, semaphore=semaphore, timeout=self.r_timeout)
        if response.content:
            data_raw = json.loads(response.content)
            data = {org_code: data_raw}
        return data

    def _process(self, data):

        result = {'status': -1, 'lics': {}}

        if not data:
            result['status'] = 0
            return result

        try:
            for item in data:
                lic_rtype = item['LIC_Type']
                lic_type = self.LIC_TYPES[lic_rtype]

                if not result['lics'].get(lic_type):
                    result['lics'][lic_type] = {}

                for lic in item['Lic_TypeR']:
                    lic_name = lic['name_module']
                    lic_end_date_raw = lic['end_date']
                    lic_end_date = datetime.datetime.strptime(lic_end_date_raw, '%d/%m/%Y')

                    if not result['lics'][lic_type].get(lic_name):
                        result['lics'][lic_type][lic_name] = lic_end_date
                    else:
                        if result['lics'][lic_type][lic_name] < lic_end_date:
                            result['lics'][lic_type][lic_name] = lic_end_date
        except Exception as err:
            if self.raise_exceptions:
                raise ProcessingError(err)
            return result

        result['status'] = 1
        return result



a = Scraper()
# a.find_one('38345394')
r = a.find_bulk(['38345394', '12345678', 'x2343'])
print(r)