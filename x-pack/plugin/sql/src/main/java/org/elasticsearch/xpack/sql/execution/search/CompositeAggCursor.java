/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.querydsl.agg.Aggs;

import java.io.IOException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Cursor for composite aggregation (GROUP BY).
 * Stores the query that gets updated/slides across requests.
 */
public class CompositeAggCursor extends ResultCursor<BucketExtractor> {

    public static final String NAME = "c";

    CompositeAggCursor(byte[] next, List<BucketExtractor> exts, BitSet mask, int remainingLimit, boolean includeFrozen, String... indices) {
        super(next, exts, mask, remainingLimit, includeFrozen, indices);
    }

    CompositeAggCursor(List<BucketExtractor> exts, BitSet mask, int remainingLimit, boolean includeFrozen, String... indices) {
        super(exts, mask, remainingLimit, includeFrozen, indices);
    }

    public CompositeAggCursor(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Supplier<ResultRowSet<BucketExtractor>> makeRowSet(SearchResponse response) {
        return () -> new CompositeAggRowSet(extractors(), mask(), response, limit());
    }

    @Override
    protected BiFunction<byte[], ResultRowSet<BucketExtractor>, ResultCursor<BucketExtractor>> makeCursor() {
        return (q, r) -> new CompositeAggCursor(q, r.extractors(), r.mask(), r.remainingLimit(), includeFrozen(), indices());
    }

    @Override
    protected boolean hasResults(SearchResponse response) {
        return response.getAggregations().asList().isEmpty() == false;
    }

    @Override
    protected boolean shouldRetryUpdatedRequest(SearchResponse response, SearchSourceBuilder search) {
        CompositeAggregation composite = getComposite(response);
        // if there are no buckets but a next page, go fetch it instead of sending an empty response to the client
        if (composite != null && composite.getBuckets().isEmpty() && composite.afterKey() != null && !composite.afterKey().isEmpty()) {
            updateSourceAfterKey(composite.afterKey(), search);
            return true;
        } else {
            return false;
        }
    }

    static CompositeAggregation getComposite(SearchResponse response) {
        Aggregation agg = response.getAggregations().get(Aggs.ROOT_GROUP_NAME);
        if (agg == null) {
            return null;
        }

        if (agg instanceof CompositeAggregation) {
            return (CompositeAggregation) agg;
        }

        throw new SqlIllegalArgumentException("Unrecognized root group found; {}", agg.getClass());
    }

    @Override
    protected void updateSourceAfterKey(ResultRowSet<BucketExtractor> rowSet, SearchSourceBuilder source) {
        assert rowSet instanceof CompositeAggRowSet;
        Map<String, Object> afterKey = ((CompositeAggRowSet) rowSet).afterKey();
        if (afterKey != null) {
            updateSourceAfterKey(afterKey, source);
        }
    }

    private static void updateSourceAfterKey(Map<String, Object> afterKey, SearchSourceBuilder search) {
        AggregationBuilder aggBuilder = search.aggregations().getAggregatorFactories().iterator().next();
        // update after-key with the new value
        if (aggBuilder instanceof CompositeAggregationBuilder) {
            CompositeAggregationBuilder comp = (CompositeAggregationBuilder) aggBuilder;
            comp.aggregateAfter(afterKey);
        } else {
            throw new SqlIllegalArgumentException("Invalid client request; expected a group-by but instead got {}", aggBuilder);
        }
    }

    @Override
    public String toString() {
        return "cursor for composite on index [" + Arrays.toString(indices()) + "]";
    }
}
