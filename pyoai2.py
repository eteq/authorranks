#!/usr/bin/env python
from __future__ import division

"""
A module to harvest data from sites that support the
`OAI PMH v2 protocol <http://www.openarchives.org/OAI/2.0/openarchivesprotocol.htm>`_.

It has thus far only been tested on `the arXiv <http://arxiv.org/>`_,
but should work with other OAI PMH v2 Repositories.
"""

__all__ = ['OAI2Harvester', 'run_session']


class OAI2Harvester(object):
    """
    A harvester for OAI2 repositories.

    To subclass this, ...
    """
    def __init__(self, baseurl, recordset, startdate=None, format='dc',
                 verbose=True, basewritename='arXiv_oai/reclist',
                 recnumpadding=4):

        self.startdate = startdate
        self.format = format
        self.recordset = recordset
        self.baseurl = baseurl

        self.basewritename = basewritename
        self.recnumpadding = recnumpadding

        self.verbose = verbose

        self.reset_session()  # initializes session-related vars

    def reset_session(self):
        """
        Sets the state for a new session
        """
        self.sessionnum = None
        self.i = None
        self.currentreq = None

    def _get_last_session_info(self):
        from glob import glob

        fns = [fn[len(self.basewritename):] for fn in glob(self.basewritename + '*')]

        sessionnums = [int(fn.split('_')[0]) for fn in fns]
        lastsessionnum = 0 if len(sessionnums) == 0 else max(sessionnums)

        if lastsessionnum == 0:
            firstfn = None
        else:
            lastsessionfns = [fn for fn in fns if int(fn.split('_')[0]) == lastsessionnum]
            inums = [int(fn.split('_')[1]) for fn in lastsessionfns]
            mininum = min(inums)
            minfns = [fn for inum, fn in zip(inums, fns) if mininum == inum]
            assert len(minfns) == 1
            firstfn = self.basewritename + minfns[0]

        return lastsessionnum, firstfn

    def clear_session_files(self, sessionnum):
        """
        Deletes the files associated with the given session number

        Returns a list of the deleted files' names
        """
        from os import unlink
        from glob import glob

        fns = glob(self.basewritename + str(sessionnum) + '_*')
        for fn in fns:
            unlink(fn)

        return fns

    def construct_start_url(self):
        """
        Returns the URL for the first request of a session
        """
        from urllib import urlencode

        params = [('verb', 'ListRecords'),
                  ('set', self.recordset),
                  ('metadataPrefix', self.format)]
        if self.startdate is not None:
            params.insert(1, ('from', self.startdate))

        return self.baseurl + '?' + urlencode(params)

    def construct_resume_url(self, token):
        """
        Returns the URL for a continuing request of a session
        """
        from urllib import urlencode

        params = [('verb', 'ListRecords'),
                  ('resumptionToken', token)]

        return self.baseurl + '?' + urlencode(params)

    def extract_resume_info(self, reqtext):
        """
        returns False if info missing, else (token, listsize, cursor)
        """
        from xml.etree import ElementTree

        reqtext = reqtext[-1000:]  # this should always be enough to find the token?

        for l in reqtext.split('\n'):
            if l.startswith('<resumptionToken'):
                e = ElementTree.fromstring(l)
                cursor = int(e.attrib['cursor'])
                listsize = int(e.attrib['completeListSize'])
                token = e.text
                break
        else:
            return False

        return token, listsize, cursor

    @property
    def writefn(self):
        templ = '{0}{1}_{2}'
        if self.recnumpadding:
            templ = templ[:-1] + ':0' + str(int(self.recnumpadding)) + '}'
        return templ.format(self.basewritename, self.sessionnum, self.i + 1)

    def do_request(self, url):
        import requests
        from time import sleep
        from xml.etree import ElementTree

        req = self.currentreq = requests.get(url)

        while (not req.ok):
            if req.status_code == 503:
                waittime = float(req.headers['retry-after'])
                if self.verbose:
                    print '503: asked to wait', waittime, 'sec'
                sleep(waittime)
                req = requests.get(url)
            else:
                msg = 'Request failed w/status code {code}. Contents:\n{text}'
                raise ValueError(msg.format(code=req.status_code, text=req.text), req)

        #now check for OAI errors
        for i, e in enumerate(ElementTree.fromstring(req.text)):
            if i > 5:
                break  # error should be near the start
            if e.tag.endswith('error'):
                raise ValueError('Request responded with an error', e.get('code'), e.text)

        return req

    def setup_incremental_session(self, prevsessionnum=None):
        """
        Sets up for a session that's an incremental update of the `precsessionnum`.
        If `prevsiessionnum` is None, uses the latest
        """
        from xml.etree import ElementTree

        if self.sessionnum is not None:
            raise ValueError('Already in a session.  Call reset_session() before doing this again.')

        if prevsessionnum is None:
            prevsessionnum, firstfn = self._get_last_session_info()
        else:
            firstistr = (('0' * (self.recnumpadding - 1)) if self.recnumpadding else '') + '1'
            firstfn = self.basewritename + str(prevsessionnum) + '_' + firstistr

        if prevsessionnum < 1:
            raise ValueError("No previous session to update from!")

        with open(firstfn) as f:
            gotdate = gotreq = False

            for event, elem in ElementTree.iterparse(f):
                if elem.tag == '{http://www.openarchives.org/OAI/2.0/}responseDate':
                    datestr = elem.text
                    gotdate = True
                elif elem.tag == '{http://www.openarchives.org/OAI/2.0/}request':
                    if elem.attrib['verb'] != 'ListRecords':
                        raise ValueError('Verb for most recent session is {0}, but should be ListRecords!'.format(elem.attrib['verb']))
                    format = elem.attrib['metadataPrefix']
                    recset = elem.attrib['set']
                    gotreq = True

                if gotdate and gotreq:
                    break
            else:
                if not gotdate and not gotreq:
                    raise ValueError('Could not find responseDate or request!')
                elif not gotdate:
                    raise ValueError('Could not find responseDate!')
                elif not gotreq:
                    raise ValueError('Could not find request!')
                else:
                    # should be unreachable
                    raise RuntimeError('unreachable')

            self.startdate = datestr
            self.format = format
            self.recordset = recset

    def start_session(self):
        """
        Do the initial request for a session

        Returns the continuation token or False if the session is finished.
        """
        if self.sessionnum is None:
            self.sessionnum = self._get_last_session_info()[0] + 1
            self.i = 0
        else:
            raise ValueError('Already in a session.  Call reset_session() before doing this again.')

        if self.verbose:
            print 'Starting initial request to', self.baseurl
        req = self.do_request(self.construct_start_url())

        self._process_record(req)

        res = self.extract_resume_info(req.text)
        if res is False:
            if self.verbose:
                print 'Completed request in one go, no resumption info'
            self.reset_session()
            return False
        else:
            token, listsize, cursor = res
            if self.verbose:
                print 'First request completed, nrecords:', listsize
            self.i += 1

        return token

    def continue_session(self, token):
        """
        Do the next request for a session

        Returns the continuation token, or False if the session completed
        """

        if self.sessionnum is None:
            raise ValueError("Can't continue a session that's not started!")

        req = self.do_request(self.construct_resume_url(token))

        self._process_record(req)

        res = self.extract_resume_info(req.text)
        if res is False:
            if self.verbose:
                print "Couldn't find resumptionToken - possibly the request failed?"
                print "(Leaving session state alone - need to reset_session() to do anything more)"
            return False
        else:
            token, listsize, cursor = res
            if token == '':
                #blank token means this was the last request of the session
                if self.verbose:
                    print 'Completed request for session', self.sessionnum
                self.reset_session()
                return False
            else:
                if self.verbose:
                    print 'Request completed. cursor at ', cursor, 'of', listsize
                self.i += 1

        return token

    def _process_record(self, request):
        if self.verbose:
            print 'Writing request to', self.writefn
        with open(self.writefn, 'w') as f:
            f.write(request.text)

    def get_sets(self):
        """
        Returns a list of (name, spec) pairs, where `spec` is a specifier that
        can be used as `self.recordset`.
        """
        from urllib import urlencode
        from urllib2 import urlopen
        from xml.dom import minidom

        params = [('verb', 'ListSets')]

        url = self.baseurl + '?' + urlencode(params)
        try:
            u = urlopen(url)
            dom = minidom.parseString(u.read())
            specs = [e.firstChild.data for e in dom.getElementsByTagName('setSpec')]
            names = [e.firstChild.data for e in dom.getElementsByTagName('setName')]

            return zip(names, specs)
        finally:
            u.close()

    def get_formats(self, identifier=None):
        """
        Returns a list of (name, schema, namespace) where `name` can be used as
        `self.format`.
        """
        from urllib import urlencode
        from urllib2 import urlopen
        from xml.dom import minidom

        params = [('verb', 'ListMetadataFormats')]
        if identifier is not None:
            params.append(('identifier', identifier))

        url = self.baseurl + '?' + urlencode(params)
        try:
            u = urlopen(url)
            dom = minidom.parseString(u.read())
            names = [e.firstChild.data for e in dom.getElementsByTagName('metadataPrefix')]
            schema = [e.firstChild.data for e in dom.getElementsByTagName('schema')]
            namespaces = [e.firstChild.data for e in dom.getElementsByTagName('metadataNamespace')]

            return zip(names, schema, namespaces)
        finally:
            u.close()


def run_session(incremental=False, **kwargs):
    """
    Runs a complete session, downloading the full list of records with
    repeated queries.

    Parameters
    ----------
    incremental : bool or int
        If False, will start a brand new session.  If True, will do an
        incremental query (i.e. only new items since the previous session).
        If an integer, it will be passed into `setup_incremental_session`
        as the previous session number.

    kwargs are passed into the `OAI2Harvester` initializer.

    Returns
    -------
    harvster : OAI2Harvester
        The harvester object used to run the session.

    """
    from time import time

    o = OAI2Harvester(**kwargs)
    if incremental:
        o.setup_incremental_session(None if incremental is True else incremental)

    sttime = time()
    try:
        res = o.start_session()
    except ValueError as e:
        if e.args[0] == 'Request responded with an error' and len(e.args) > 2 and 'Bad date' in e.args[2]:
            # this means the dat is a full datetime and we only want date
            newstartdate = o.startdate.split('T')[0]
            if o.verbose:
                print 'Initial request errored for date', o.startdate, 'trying', newstartdate
            o.startdate = newstartdate
            o.reset_session()
            res = o.start_session()
        else:
            raise

    while res is not False:
        if o.verbose:
            print 'Token: "{0}"'.format(res)
            print 'Time from start:', (time() - sttime) / 60., 'min'
        res = o.continue_session(res)

    return o
