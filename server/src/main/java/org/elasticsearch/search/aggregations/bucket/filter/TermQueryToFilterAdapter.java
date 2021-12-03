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
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.Bits;

import java.io.IOException;
import java.util.function.BiConsumer;

/**
 * Specialized {@link QueryToFilterAdapter} for {@link TermQuery} that reads counts from metadata.
 */
class TermQueryToFilterAdapter extends QueryToFilterAdapter<TermQuery> {
    private int resultsFromMetadata;

    TermQueryToFilterAdapter(IndexSearcher searcher, String key, TermQuery query) {
        super(searcher, key, query);
    }

    @Override
    long count(LeafReaderContext ctx, FiltersAggregator.Counter counter, Bits live) throws IOException {
        if (countCanUseMetadata(counter, live)) {
            resultsFromMetadata++;
            return ctx.reader().docFreq(query().getTerm());
        }
        return super.count(ctx, counter, live);
    }

    @Override
    void collectDebugInfo(BiConsumer<String, Object> add) {
        super.collectDebugInfo(add);
        add.accept("specialized_for", "term");
        add.accept("results_from_metadata", resultsFromMetadata);
    }
}
