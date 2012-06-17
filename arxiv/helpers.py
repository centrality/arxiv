import datetime
import sqlite3

ABSTRACT_DB = 'abstract.db'
CITATIONS = 'hep-th_refs_1991-2012_5_2.txt'

def print_citations(output_name, start, end):
    """
    year format = 2001-01-01
    """
    citations = load_citations()
    conn = sqlite3.connect(ABSTRACT_DB)
    c = conn.cursor()
    stmt = "select id from abstract where published_at between '%s' and '%s'" % (start, end)
    c.execute(stmt)
    with open(output_name, 'w') as f:
        index = 0
        for row in c:
            if index % 100 is 0:
                print "parsed %d " % index
            id = row[0]
            if id in citations:
                for cited in citations[id]:
                    f.write('%s\t%s\n' % (id, cited))
            index += 1
        f.flush()


def load_citations(filename=CITATIONS):
    import fileinput
    index = 0
    h = {}
    for line in fileinput.input(filename):
        p1, p2 = line.strip().split("\t")
        if p1 not in h:
            h[p1] = set()
        h[p1].add(p2)
    return h


def find_papers(author_keys):
    conn = sqlite3.connect(ABSTRACT_DB)
    papers = {}
    for author_key in author_keys:
        papers[author_key] = set()
        for arxiv_id in conn.execute('select arxiv_id from aidkey2 where author_key = "' + author_key.replace("'",'') + '"'):
            papers[author_key].add(arxiv_id[0])
    for key, arxiv_ids in papers.items():
        yield key, arxiv_ids
