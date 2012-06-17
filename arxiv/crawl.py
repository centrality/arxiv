""":mod:`arxiv.crawl` -- Fetch documents, abstracts, and references

Has 3 main methods:

- fetch_arxiv_ids(field, years), which returns arxiv_ids for corresponding years
- fetch_raw_refs(arxiv_ids, save_to, save_uncrawled_to), which stores references for each arxiv_ids
- fetch_raw_abstracts(arxiv_ids, save_to)

Use :mod:`arxiv.parse` to parse raw documents.

"""
import os
import re
import datetime
import pickle
import shutil
import sys
import urllib
import urllib2

import eventlet
import lxml, lxml.html, lxml.objectify

eventlet.monkey_patch()

# arxiv capped the list size at 2000
MAX_SHOW = 2000
ARXIV_LIST_BASE_URL = "http://arxiv.org/list/%(field)s/%(year)s?" \
                      "skip=%(skip)d&show=" + str(MAX_SHOW)
ARXIV_API_BASE_URL = "http://export.arxiv.org/api/query"
ARXIV_REF_BASE_URL = "http://arxiv.org/refs/"
SPIRES_REF_BASE_URL = "http://www-spires.slac.stanford.edu/spires/find/hep/"\
                      "wwwrefs?rawcmd=eprint+"


def fetch_arxiv_ids(field, years):
    idset = set()
    if type(years) is int:
        years = [years]
    for year in years:
        year %= 100
        if year < 10:
            year = "0" + str(year)
        else:
            year = str(year)
        skip = 0
        total_entries = sys.maxint
        while skip < total_entries:
            # retrieve pages
            page = fetch_arxiv_list(field, year, skip)
            total_entries = int(
                re.search("(?<=total of )\d+(?= entries)", page.text_content()).group()
            )
            print total_entries
            skip += MAX_SHOW

            # parse ids
            links = page.xpath("//a[@title='Abstract']")
            idlist = [a.get('href')[5:] for a in links]
            for id in idlist:
                idset.add(id)
    return idset

def fetch_arxiv_list(field, year, skip):
    args = {'field':field, 'year':year, 'skip': skip}
    url = ARXIV_LIST_BASE_URL % args
    html = urllib2.urlopen(urllib2.Request(url)).read()
    return lxml.html.fromstring(html)


def chunks(lst, n):
    return [lst[i:i+n] for i in range(0, len(lst), n)]


def save_raw_abstracts(idset, save_dir):
    ids = list(idset)
    i = 0
    for chunk in chunks(ids, 1000):
        values = {'max_results': 1000,
                  'id_list': ','.join(chunk)}
        data = urllib.urlencode(values)
        print 'retrieving %d' % i
        req = urllib2.Request(ARXIV_API_BASE_URL, data)
        res = urllib2.urlopen(req)
        with open(os.path.join(save_dir, '%d.abs' % i), 'w') as f:
            shutil.copyfileobj(res, f)
        i += 1


def save_raw_refs(arxiv_ids, save_dir):
    """
    for arxiv_id like "hep-th/9410226", stores raw ref to 
    "{save_dir}/hep-th_9410226"
    """
    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    for id in arxiv_ids:
        with open(os.path.join(save_dir, id.replace("/","_")), 'w') as f:
            req = urllib2.Request(ARXIV_REF_BASE_URL+id)
            res = urllib2.urlopen(req)
            shutil.copyfileobj(res, f)


def fetch_n_save(args):
    """Async fetcher"""
    url, filename, key = args
    error = None
    try:
        with open(filename, 'w') as f:
            req = urllib2.Request(url)
            res = urllib2.urlopen(req)
            shutil.copyfileobj(res, f)
    except urllib2.URLError as e:
        error = e
    return key, error


def fetch_raw_refs(arxiv_ids, save_to, save_uncrawled_to):
    pool = eventlet.GreenPool(10)
    if not os.path.isdir(save_to):
        os.makedirs(save_to)
    fetchlist = [] 
    errorlist = [] 
    uncrawled = {}
    for id in arxiv_ids:
        url = SPIRES_REF_BASE_URL + id
        filename = os.path.join(save_to, id.replace("/","_"))
        fetchlist.append((url, filename, id))
        uncrawled[id] = True
    cnt = 0
    try:
        for key, error in pool.imap(fetch_n_save, fetchlist):
            cnt += 1
            if cnt % 100 is 0:
                with open(save_uncrawled_to,'w') as f:
                    pickle.dump(uncrawled, f)
                    f.flush()
                today = datetime.datetime.now()
                print "%s: crawled %d refs" % (str(today), cnt)
            if error:
                errorlist.append(error)
                print key
                print error
            else:
                del uncrawled[key]
    except Exception as e:
        raise e
    return errorlist

def load_ids(filename):
    with open(filename) as f:
        return pickle.load(f)
