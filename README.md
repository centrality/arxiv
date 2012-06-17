arxiv
=====

arxiv crawler and some helpers written in python

example
-------

    ids = arxiv.crawl.fetch_ids('hep-th', range(1991, 2013))
    arxiv.crawl.fetch_raw_abs(ids, outdir='raw_abs')
    arxiv.crawl.fetch_raw_refs(ids, outdir='raw_ref',
                               unfetched='unfetched.pickle')
    refs = arxiv.parse.parse_refs(ids, 'raw_refs')
    abs = arxiv.parse.parse_abs(ids, 'raw_abs')
