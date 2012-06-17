

def save_refs(refs, filename):
    with open(filename, 'w') as f:
        for id, refset in refs.items():
            for ref in refset:
                f.write("%s\t%s\n" % (id, ref))
        f.flush()
        
