"""
Variations on the arxivsorter algorithm using the ADS API
http://www.cita.utoronto.ca/~menard/Arxivsorter_Documentation/
"""

import requests

def get_ads_api_key():
	return 'FBdkL7FdzQCloyg4'

def ads_api_query_docs(query):
	apikey = get_ads_api_key()
	docs = []

	i = 0
	nhits = 1
	while i > nhits:
		res = requests.post('http://adslabs.org/adsabs/api/search/',
                         params={'q':query,
                                 'dev_key': apikey,
                                 'db_f':'astronomy',
                                 'rows':200,
                                 'fields':'astronomy',
                                 'start':i})
		j = res.json()
		nhits = j['meta']['hits']
		docs.extend(j['results']['docs'])
		i += 200

	return docs


def author_connectivity_matrix(author, store=None):


