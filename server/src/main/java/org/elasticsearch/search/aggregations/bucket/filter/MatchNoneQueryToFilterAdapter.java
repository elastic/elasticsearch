/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.filter;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.IntPredicate;

/**
 * Special case when the filter can't match anything.
 */
class MatchNoneQueryToFilterAdapter extends QueryToFilterAdapter<MatchNoDocsQuery> {
    MatchNoneQueryToFilterAdapter(IndexSearcher searcher, String key, MatchNoDocsQuery query) {
        super(searcher, key, query);
    }

    @Override
    QueryToFilterAdapter<?> union(Query extraQuery) throws IOException {
        return this;
    }

    @Override
    IntPredicate matchingDocIds(LeafReaderContext ctx) throws IOException {
        return l -> false;
    }

    @Override
    long count(LeafReaderContext ctx, FiltersAggregator.Counter counter, Bits live) throws IOException {
        return 0;
    }

    @Override
    void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("specialized_for", "match_none");
    }
}
