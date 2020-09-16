/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;


import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.HitExtractor;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Supplier;


public class SearchHitCursor extends ResultCursor<HitExtractor> {

    public static final String NAME = "s";

    public SearchHitCursor(byte[] next, List<HitExtractor> exts, BitSet mask, int remainingLimit, boolean includeFrozen,
                           String... indices) {
        super(next, exts, mask, remainingLimit, includeFrozen, indices);
    }

    public SearchHitCursor(List<HitExtractor> exts, BitSet mask, int remainingLimit, boolean includeFrozen,
                           String... indices) {
        super(exts, mask, remainingLimit, includeFrozen, indices);
    }

    public SearchHitCursor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected Supplier<ResultRowSet<HitExtractor>> makeRowSet(SearchResponse response) {
        return () -> new SearchHitRowSet(extractors(), mask(), limit(), response);
    }

    @Override
    protected BiFunction<byte[], ResultRowSet<HitExtractor>, ResultCursor<HitExtractor>> makeCursor() {
        return (q, r) -> new SearchHitCursor(q, r.extractors(), r.mask(), r.remainingLimit(), includeFrozen(), indices());
    }

    @Override
    protected boolean hasResults(SearchResponse response) {
        return response.getHits().getHits().length > 0;
    }

    @Override
    protected void updateSourceAfterKey(ResultRowSet<HitExtractor> rowSet, SearchSourceBuilder source) {
        assert rowSet instanceof SearchHitRowSet;
        Object[] sortValues = ((SearchHitRowSet) rowSet).lastSortValues();
        if (sortValues != null) {
            source.searchAfter(sortValues);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String toString() {
        return "cursor for search hits on index [" + Arrays.toString(indices()) + "]";
    }
}
