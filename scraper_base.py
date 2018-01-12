from abc import ABC, abstractmethod
import re
from typing import Iterable

pattern_exc_firm_code = re.compile(r'\[(\d+)\]')


class ResponseError(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.org_code = kwargs.get('org_code')

    def __str__(self):
        s = super().__str__()
        return '[{}]{}'.format(self.org_code, s)


class ProcessingError(Exception):

    def __init__(self, *args, **kwargs):
        super().__init__(*args)
        self.org_code = kwargs.get('org_code')

    def __str__(self):
        s = super().__str__()
        return '[{}]{}'.format(self.org_code, s)


class BaseScraper(ABC):

    @abstractmethod
    def find_one(self, org_code: str) -> dict:
        pass

    @abstractmethod
    def find_bulk(self, org_codes: Iterable) -> list:
        pass
