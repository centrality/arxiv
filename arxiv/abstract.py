from datetime import datetime
import json
import sqlalchemy
import sqlalchemy.types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, Text, \
                       create_engine
from sqlalchemy.orm import sessionmaker
import re
import pytz
import time
from time import mktime

STRCHAR = re.compile("[^a-zA-Z\s]")

engine = create_engine('sqlite:///abstract.db')
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()


class RawAbstract(Base):
    __tablename__ = 'raw_abstract'
    id = Column(String(15), primary_key=True)
    journal_raw = Column(String(255))
    published_raw = Column(String(255)) 
    updated_raw = Column(String(255))
    published_at = Column(DateTime)
    updated_at = Column(DateTime)
    title = Column(String(255))
    summary = Column(Text)
    author_names = Column(Text)
    prim_category = Column(String(50))
    categories = Column(Text)
    IDREGEX = re.compile(r'v[0-9]+$')

    def from_atom_entry(self, entry):
        self.id = self.IDREGEX.sub('', entry.id[21:])
        if 'arxiv_journal_ref' in entry:
            self.journal_raw = entry['arxiv_journal_ref']
        self.published_raw = entry['published']
        self.published_at = datetime.fromtimestamp(mktime(entry['published_parsed']))
        self.updated_raw = entry['updated']
        self.updated_at = datetime.fromtimestamp(mktime(entry['updated_parsed']))
        self.title = entry['title']
        self.title = entry['summary']
        names = [a['name'] for a in entry['authors']]
        self.author_names = '|'.join(names)
        self.prim_category = entry['arxiv_primary_category']['term']
        self.categories = '|'.join([t['term'] for t in entry['tags']])


FIRST_INT = re.compile(r'[^a-zA-Z\s\.]')
STRCHAR2 = re.compile("[^a-zA-Z]")

class Abstract(Base):
    __tablename__ = 'abstract'
    id = Column(String(15), primary_key=True)
    journal = Column(String(255))
    published_at = Column(DateTime)
    updated_at = Column(DateTime)
    author_names = Column(Text)
    author_ids = Column(String(255))
    prim_category = Column(String(50))
    categories = Column(Text)
    IDREGEX = re.compile(r'v[0-9]+$')

    def from_atom_entry(self, entry):
        self.id = self.IDREGEX.sub('', entry.id[21:])
        if 'arxiv_journal_ref' in entry:
            journal_raw = entry['arxiv_journal_ref']
            first_integer = FIRST_INT.search(journal_raw)
            if first_integer:
                self.journal = journal_raw[0:first_integer.start()].strip()
            else:
                self.journal = journal_raw
            self.journal = STRCHAR2.sub('', journal_raw)
        self.published_at = datetime.fromtimestamp(mktime(entry['published_parsed']))
        self.updated_at = datetime.fromtimestamp(mktime(entry['updated_parsed']))
        names = [a['name'] for a in entry['authors']]
        ids = []
        for name in names:
            parts = STRCHAR.sub('', name).strip().split(' ')
            if len(parts) > 1:
                ids.append('%s_%s' % (parts[0].upper()[0], parts[-1].upper()))
            else:
                ids.append('_' + parts[0])
        self.author_ids = '|'.join(ids)
        self.author_names = '|'.join(names)
        self.prim_category = entry['arxiv_primary_category']['term']
        self.categories = '|'.join([t['term'] for t in entry['tags']])

    def __repr__(self):
        return '%s %s %s %s %s %s %s %s' % (self.id,
        self.journal ,
        str(self.published_at),
        str(self.updated_at) ,
        self.author_names ,
        self.author_ids ,
        self.prim_category,
        self.categories)


ff = None

def test(filename):
    import feedparser
    ff = feedparser.parse(filename)
    abs = RawAbstract()
    abs.from_atom_entry(ff.entries[0])
    create_tables()
    session.add(abs)
    session.commit()
    return abs


def create_tables():
    Base.metadata.create_all(engine)
