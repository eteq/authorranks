import pyoai2


kw = dict(incremental=False, basewritename='arXiv_oai/reclist',
          startdate=None, format='arXivRaw', recordset='physics:astro-ph',
          baseurl='http://export.arxiv.org/oai2', recnumpadding=4)

s = pyoai2.run_session(**kw)
