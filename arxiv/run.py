import arxiv.helpers


def gen_cite_files():
    start = 1991
    for end in xrange(1991, 2013):
        filename = "hep-th_cites_%d_to_%d" % (1991, end-1)
        dateformat = "%d-01-01"
        arxiv.helpers.print_citations(filename, dateformat % start, dateformat % end)

def author_to_paper(filename, out):
    keys = set([key.strip() for key in open(filename).readlines()])
    with open(out, 'w') as f:
        for key, arxiv_ids in arxiv.helpers.find_papers(keys):
            f.write(key)
            f.write("\t")
            f.write("|".join(arxiv_ids))
            f.write("\n")
        f.flush()
