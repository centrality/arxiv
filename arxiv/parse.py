""":mod:`arxiv.parse` -- Parse abstracts and references

"""
import os
import lxml.html
import feedparser


def parse_ref_folder(folder, refs={}, idset=None):
    index = 0
    out = open('hep-th_refs.txt', 'a')
    for filename in os.listdir(folder):
        if index % 100 is 0:
            print "parsed %d " % index
        key = filename.replace("_", "/")
        with open(os.path.join(folder, filename)) as f:
            try:
                cites = parse_ref_file(f)
                if idset:
                    cites = cites.intersection(idset)
                for cite in cites:
                    out.write("%s\t%s\n" % (key, cite))
            except Exception as e:
                print key
                print e
        index += 1
    out.flush()
    return refs

def parse_ref_file(f):
    tree = lxml.html.parse(f)
    return set([node.text for node in tree.xpath("//dt/a[1]")])

def map_to_arxiv_refs(arxiv_ids, refs):
    arxiv_refs = {}
    if type(arxiv_ids) is not set:
        arxiv_ids = set(arxiv_ids)
    index = 0
    for id, refset in refs.items():
        if index % 100 is 0:
            print "parsed %d " % index
        if type(refset) is not set:
            refset = set(refset)
        arxiv_refs[id] = refset.union(arxiv_ids)
        index += 1
    return arxiv_refs

def parse_abs_file(f, absfolder, save_to):
    from arxiv.abstract import RawAbstract, Abstract, session, create_tables
    create_tables()
    for filename in os.listdir(absfolder):
        atom = feedparser.parse(os.path.join(absfolder, filename))
        for entry in atom.entries:
            rawabs = RawAbstract()
            abs = Abstract()
            rawabs.from_atom_entry(entry)
            abs.from_atom_entry(entry)
            session.add(abs)
            session.add(rawabs)
        session.commit()
        print 'finished ', filename


