# Licensed under a 3-clause BSD style license - see LICENSE.rst
# Intended for use by: pytest
# Python library
from __future__ import print_function
from pprint import pformat
# External packages
from astropy import units as u
from astropy.coordinates import SkyCoord
from astropy.tests.helper import remote_data
# Local packages
from .. import NoaoClass
from . import expected_resp as exp

# performs similar tests as test_module.py, but performs
# the actual HTTP request rather than monkeypatching them.
# should be disabled or enabled at will - use the
# remote_data decorator from astropy:


@remote_data
class TestNoaoClass(object):

    def test_query_region_1(self):
        """Ensure query gets at least the set of files we expect.
        Its ok if more files have been added to the remote Archive
        since expected.query_region_1 was saved."""
        print(f'DBG: query_region_1')
        arch = NoaoClass(which='voimg')
        c = SkyCoord(ra=10.625*u.degree, dec=41.2*u.degree, frame='icrs')
        # Queury cannot use LIMIT; unsorted results unpredictable for tests
        r = arch.query_region(c, radius=0.1, sort=True) 
        actual = set(list(r['md5sum'])[1:6])
        print(f'DBG: query_region_1; actual({len(r)})={actual}')
        expected = exp.query_region_1
        print(f'DBG: query_region_1; expected={expected}')
        assert expected.issubset(actual)

    def test_service_metadata(self):
        """Metadata query per:
        http://www.ivoa.net/documents/PR/DAL/PR-SIA-1.0-20090521.html#mdquery
        """
        arch = NoaoClass(which='voimg')
        actual = arch.service_metadata()
        print(f'DBG-test_service_metadata: actual={pformat(actual)}')
        expected = exp.service_metadata
        assert actual == expected
        
        
    def test_query_metadata_1(self):
        """Ensure query gets at least the set of files we expect.
        Its ok if more files have been added to the remote Archive
        since expsia.query_metadata_1 was saved."""

        arch = NoaoClass(which='fasearch')
        jj = {
            "outfields" : [
                "md5sum",
                "ra_min", 
                "archive_filename",
                "instrument",
                "proc_type",
                "obs_type",
                "release_date",
                "proposal",
                "caldat",
                "EXPNUM",    # AUX field. Slows search
                "AIRMASS",
            ],
            "search" : [
                ["instrument", "decam"],
                ["proc_type", "raw"],
                ["ra_min",322,324],
                ["EXPNUM", 667000, 669000],
            ]
        }
        
        r = arch.query_metadata(jj, limit=5)
        print(f'DBG: response({len(r)})={r}')
        actual = set(list(r['md5sum'])[1:])

        print(f'DBG: query_metadata_1; actual={actual}')
        expected = exp.query_metadata_1
        print(f'DBG: query_metadata_1; expected={expected}')
        assert expected.issubset(actual)
        
    def test_core_fields(self):
        """core_file_fields ok"""
        arch = NoaoClass(which='fasearch')
        actual = arch.core_fields()
        print(f'DBG-test_core_fields: actual={pformat(actual)}')
        expected = exp.core_fields
        assert actual == expected

    def test_aux_fields(self):
        """aux_file_fields ok"""
        arch = NoaoClass(which='fasearch')
        actual = arch.aux_fields('decam','raw')
        print(f'DBG-test_aux_fields: actual={pformat(actual)}')
        expected = exp.aux_fields
        assert actual == expected
