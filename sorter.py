"""
Variations on the arxivsorter algorithm using the ADS API
http://www.cita.utoronto.ca/~menard/Arxivsorter_Documentation/
"""

import requests
import numpy as np

def _debug(s):
    print s

def get_ads_api_key():
    return 'FBdkL7FdzQCloyg4'


def ads_api_query_docs(query, upperlimitconns=None):
    apikey = get_ads_api_key()
    docs = []

    i = 0
    nhits = 1
    while i < nhits:
        res = requests.get('http://adslabs.org/adsabs/api/search/',
                            params={'q': query,
                                     'dev_key': apikey,
                                     'db_f': 'astronomy',
                                     'rows': 200,
                                     'fields': 'astronomy',
                                     'start': i})
        j = res.json()
        nhits = j['meta']['hits']
        if upperlimitconns is not None and upperlimitconns < nhits:
            raise ValueError('Found too many in author query! ({0}, {1})'.format(query, nhits))
        _debug('query_docs: On {i} of {nhits}'.format(i=i, nhits=nhits))
        docs.extend(j['results']['docs'])
        i += j['meta']['count']

    return docs


def encode_author(author):
    from unidecode import unidecode
    simplified = unidecode(unicode(author)).replace('~', '').replace('.', '').replace('-', '').replace(' ','').lower()
    if ',' in simplified:
        spl = simplified.split(',')
        if len(spl) > 1 and len(spl[1]) > 0:
            return spl[0] + ',' + spl[1][0]
        else:
            return spl[0]
    else:
        return simplified

def author_connectivity(author, connmatrix=None, upperlimitconns=None):
    """
    normalized by total ranking sum
    """
    if connmatrix is None or author not in connmatrix:
        from collections import defaultdict

        docs = ads_api_query_docs('author:' + author, upperlimitconns=upperlimitconns)

        ranks = defaultdict(lambda:0.0)
        #lauth = unidecode(unicode(author)).lower().split(',')[0].replace('-', ' ')
        author = encode_author(author)
        for d in docs:
            for i, auth in enumerate(d['author']):
                #if unidecode(auth).replace('~', '').replace('-',' ').lower().startswith(lauth):
                if author.startswith(encode_author(auth)):
                    break
            else:
                encalist = [encode_author(a) for a in d['author']]
                raise ValueError(u'Did not find author {a} in author list {alist}, processed to {alist2}!'.format(a=author, alist=d['author'], alist2=encalist))

            for j, auth in enumerate(d['author']):
                if j > 0:
                    ranks[encode_author(auth)] += 1./(i + j)

        #remove self-cites
        ranks.pop(author, None)

        if connmatrix is not None:
            connmatrix[author] = dict(ranks)
    else:
        ranks = connmatrix[author]

    return ranks


def connmatrix_to_trans_prob(connmatrix):
    pmatrix = {}
    if len(connmatrix) == 0:
        return pmatrix

    if isinstance(connmatrix.itervalues().next(), dict):
        for k in connmatrix:
            #recursive
            pmatrix[k] = connmatrix_to_trans_prob(connmatrix[k])
    else:
        ranksum = sum(connmatrix.values())
        for k in connmatrix:
            pmatrix[k] = connmatrix[k] / ranksum
    return pmatrix


def expand_connmatrix(connmatrix, nperstep=None):
    allau = {}
    for au in connmatrix.values():
        for au2, r in au.iteritems():
            allau[au2] = r

    if nperstep:
        notdoneau = sorted([(r, au) for au in allau if au not in connmatrix])
        notdoneau = [t[1] for t in notdoneau[:nperstep]]

    for j, au in enumerate(notdoneau):
        _debug(u'Processing author {au}, #{on} of {nau}'.format(au=au, on=j+1, nau=len(notdoneau)))
        author_connectivity(au, connmatrix)


def step_connmatrix(authori, connmatrix, connstep=True, upperlimitconns=None):
    """
    do a step through the connections matrix probabilistically, starting at
    `authori`.  Also do a connectivity search on the new author if `connstep`
    is True.
    """
    from random import random

    pmat = connmatrix_to_trans_prob(connmatrix[authori])

    if len(pmat)==0:
        #single-author paper
        return authori, 1

    nms = np.array(pmat.keys())
    ps = np.cumsum(pmat.values())
    i = np.searchsorted(ps, random())

    newauthor = nms[i]
    newp = pmat.values()[i]

    if connstep:
        _debug('stepping to author ' + newauthor)
        author_connectivity(newauthor, connmatrix, upperlimitconns=upperlimitconns)

    return newauthor, newp


def search_for_connected_author(authori, goalauthor, connmatrix, nsteps=5, nwalkers=5, upperlimitconns=1601):
    authori = encode_author(authori)
    goalauthor = encode_author(goalauthor)

    if goalauthor.startswith(authori):
        return 1

    if authori not in connmatrix:
        author_connectivity(authori, connmatrix)

    for au in connmatrix[authori]:
        if au.startswith(goalauthor):
            return connmatrix_to_trans_prob(connmatrix)[authori][au]

    allranks = []
    allsteps = []
    for i in range(nwalkers):
        _debug('running walker {i} of {n}'.format(i=i+1, n=nwalkers))

        try:
            currp = 1
            steps = [authori]
            currauth = authori
            found = False
            for j in range(nsteps):
                _debug('running step {i} of {n}'.format(i=j+1, n=nsteps))
                currauth, latestp = step_connmatrix(currauth, connmatrix,upperlimitconns=upperlimitconns)
                steps.append(currauth)
                currp *= latestp
                if goalauthor.startswith(currauth):
                    found = True
                else:
                    for a in connmatrix[currauth]:
                        if goalauthor.startswith(a):
                            found = True
                if found:
                    allranks.append(currp)
                    allsteps.append(steps)
                    break
            else:
                allranks.append(0)
                allsteps.append(steps)

        except ValueError as e:
            if 'Found too many in author query!' in e.args[0]:
                print 'Skipping walker due to too many copies of an author: ', e.args[0].split('!')[1]
            else:
                print 'Failed for some other reason, printing here and skipping walker'
                import traceback
                traceback.print_last()

    return np.mean(allranks), allranks, allsteps


