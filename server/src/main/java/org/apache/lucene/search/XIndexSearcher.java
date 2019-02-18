package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;

import java.io.IOException;
import java.util.List;

/**
 * A wrapper for {@link IndexSearcher} that makes {@link IndexSearcher#search(List, Weight, Collector)}
 * visible by sub-classes.
 */
public class XIndexSearcher extends IndexSearcher {
    private final IndexSearcher in;

    public XIndexSearcher(IndexSearcher in, QueryCache queryCache, QueryCachingPolicy queryCachingPolicy) {
        super(in.getIndexReader());
        this.in = in;
        setSimilarity(in.getSimilarity());
        setQueryCache(queryCache);
        setQueryCachingPolicy(queryCachingPolicy);
    }

    @Override
    public void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
        in.search(leaves, weight, collector);
    }
}
