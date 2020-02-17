"""
Provide astroquery API access to OIR Lab Astro Data Archive (natica).

This does DB access through web-services. It allows query against ALL
fields in FITS file headers.

Also possible but not provided here: query through Elasticsearch DSL.  ES will
be much faster but much more limitted in what can be used in query.  """

# Python library
# External packages
import requests
import astropy.table
# Local packages

# 3. local imports - use relative imports
# commonly required local imports shown below as example
# all Query classes should inherit from BaseQuery.
from ..query import BaseQuery
# has common functions required by most modules
# #!from ..utils import commons
# prepend_docstr is a way to copy docstrings between methods
# #!from ..utils import prepend_docstr_nosections
# async_to_sync generates the relevant query tools from _async methods
from ..utils import async_to_sync
from ..utils.class_or_instance import class_or_instance
# import configurable items declared in __init__.py
from . import conf


__all__ = ['Noao', 'NoaoClass']


@async_to_sync
class NoaoClass(BaseQuery):

    NAT_URL = conf.server
    SIA_URL = f'{NAT_URL}/api/sia'
    ADS_URL = f'{NAT_URL}/api/adv_search'

    def __init__(self, which='voimg'):
        """ set some parameters """
        self._api_version = None
        self.which = which

        if which == 'vohdu':
            self.url = f'{self.SIA_URL}/vohdu'
        elif which == 'voimg':
            self.url = f'{self.SIA_URL}/voimg'
        elif which == 'fasearch':
            self.url = f'{self.ADS_URL}/fasearch'
        elif which == 'hasearch':
            self.url = f'{self.ADS_URL}/hasearch'
        else:
            self.which = 'voimg'
            self.url = f'{self.SIA_URL}/voimg'

    @property
    def api_version(self):
        if self._api_version is None:
            response = requests.get(f'{self.NAT_URL}/api/version')
            # Following gets error:
            #   AttributeError: 'NoaoClass' object has
            #      no attribute 'cache_location'
            # Don't see documenation saying what that should be.
            # #!response = self._request('GET', f'{self.NAT_URL}/api/version',
            # #!                         cache=False)
            self._api_version = float(response.content)
        return self._api_version

    def _validate_version(self):
        KNOWN_GOOD_API_VERSION = 2.0
        if int(self.api_version) > int(KNOWN_GOOD_API_VERSION):
            msg = (f'The astroquery.noao module is expecting an older version '
                   f'of the {self.NAT_URL} API services.  '
                   f'Please upgrade to latest astroquery.  '
                   f'Expected version {KNOWN_GOOD_API_VERSION} but got '
                   f'{self.api_version} from the API.')
            raise Exception(msg)

    @class_or_instance
    def _query_sia(self, coordinate, radius=1.0, limit=1000, sort=False):
        ra, dec = coordinate.to_string('decimal').split()
        size = radius
        url = (f'{self.url}?POS={ra},{dec}&SIZE={size}'
               f'&format=json&limit={limit}')
        print(f'DBG: SIA url={url}')
        # #!response = self._request('GET', url)
        response = requests.get(url)
        # Return value from service is not sorted (it takes longer to sort).
        # But we need to test against known PARTIAL results.
        # To allow PARTIAL compare, sort here.
        if sort:
            header = response.json()[0]
            body = response.json()[1:]
            ordered = sorted(body, key=lambda d: d['archive_filename'])
            return astropy.table.Table(data=[header]+ordered)
        else:
            return astropy.table.Table(data=response.json())

    def service_metadata(self):
        """Denotes a Metadata Query: no images are requested; only metadata
    should be returned. This feature is described in more detail in:
    http://www.ivoa.net/documents/PR/DAL/PR-SIA-1.0-20090521.html#mdquery
        """
        url = f'{self.url}?FORMAT=METADATA&format=json'
        response = requests.get(url)
        return response.json()[0]

    @class_or_instance
    def query_region(self, coordinate, radius=1.0, limit=1000, sort=False):
        self._validate_version()

        if self.which == 'vohdu':
            return self._query_sia(coordinate,
                                   radius=radius, limit=limit, sort=sort)
        elif self.which == 'voimg':
            return self._query_sia(coordinate,
                                   radius=radius, limit=limit, sort=sort)

    def _query_ads(self, jdata, limit=1000):
        print(f'DBG-0: ADS jdata={jdata}')
        # #!url = f'{self.url}/'
        # #!params = dict(limit=limit)
        # #!print(f'DBG-0: ADS params={params}')
        # #!response = requests.post(self.url, json=jdata, params=params)
        adsurl = f'{self.url}/?limit={limit}'
        print(f'DBG-0: adsurl = {adsurl}')
        response = requests.post(adsurl, json=jdata)
        print(f'DBG-0: ADS response={response}')
        print(f'DBG-0: ADS response.content={response.content}')
        print(f'DBG-0: ADS response.json()={response.json()}')
        return astropy.table.Table(data=response.json())

    def core_fields(self):  # @@@ make this property and cache it
        """List the available CORE fields. CORE fields are faster to search
than AUX fields.."""
        if self.which == 'fasearch':
            url = f'{self.ADS_URL}/core_file_fields/'
        elif self.which == 'hasearch':
            url = f'{self.ADS_URL}/core_hdu_fields/'
        else:
            return None
        response = requests.get(url)
        return response.json()

    def aux_fields(self, instrument, proctype):
        """List the available AUX fields. AUX fields are ANY fields in the
Archive FITS files that are not core DB fields.  These are generally
common to a single Instrument, Proctype combination. AUX fields are
slower to search than CORE fields. """
        if self.which == 'fasearch':
            url = f'{self.ADS_URL}/aux_file_fields/{instrument}/{proctype}/'
        elif self.which == 'hasearch':
            url = f'{self.ADS_URL}/aux_hdu_fields/{instrument}/{proctype}/'
        else:
            return None
        response = requests.get(url)
        return response.json()

    def categoricals(self):
        """List the currently acceptable values for each 'categorical field'
        associated with Archive files.  A 'categorical field' is one in
        which the values are restricted to a specific set.  The specific
        set may grow over time, but not often. The categorical fields are:
        collection, instrument, obs_mode, proc_type, prod_type, site, survey,
        telescope.
        """
        url = f'{self.ADS_URL}/cat_lists/?format=json'
        response = requests.get(url)
        return response.json()

    @class_or_instance
    def query_metadata(self, qspec, limit=1000):
        self._validate_version()

        if qspec is None:
            jdata = {"outfields": ["md5sum", ], "search": []}
        else:
            jdata = qspec

        if self.which == 'hasearch':
            return self._query_ads(jdata, limit=limit)
        elif self.which == 'fasearch':
            return self._query_ads(jdata, limit=limit)

    def _args_to_payload(self, *args):
        # convert arguments to a valid requests payload
        return dict


Noao = NoaoClass()
