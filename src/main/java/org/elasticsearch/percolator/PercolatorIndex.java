package org.elasticsearch.percolator;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.elasticsearch.index.engine.Engine;


interface PercolatorIndex {

    Engine.Searcher getSearcher();

    IndexReader getIndexReader();

    AtomicReaderContext getAtomicReaderContext();

}
