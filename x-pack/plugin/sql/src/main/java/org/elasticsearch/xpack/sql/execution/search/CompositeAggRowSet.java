/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.xpack.ql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;

/**
 * {@link RowSet} specific to (GROUP BY) aggregation.
 */
class CompositeAggRowSet extends ResultRowSet<BucketExtractor> {

    final List<? extends CompositeAggregation.Bucket> buckets;

    Map<String, Object> afterKey;
    int remainingData;
    int size;
    int row = 0;

    CompositeAggRowSet(List<BucketExtractor> exts, BitSet mask, SearchResponse response, int limit) {
        super(exts, mask);

        CompositeAggregation composite = CompositeAggCursor.getComposite(response);
        if (composite != null) {
            buckets = composite.getBuckets();
            afterKey = composite.afterKey();
        } else {
            buckets = emptyList();
            afterKey = null;
        }

        // page size
        size = limit == -1 ? buckets.size() : Math.min(buckets.size(), limit);
        remainingData = remainingData(afterKey != null, size, limit);
    }

    static int remainingData(boolean hasNextPage, int size, int limit) {
        if (hasNextPage == false) {
            return 0;
        } else {
            int remainingLimit = (limit == -1) ? limit : ((limit - size) >= 0 ? (limit - size) : 0);

            // if the computed limit is zero, or the size is zero it means either there's nothing left or the limit has been reached
            // note that a composite agg might be valid but return zero groups (since these can be filtered with HAVING/bucket selector)
            // however the Querier takes care of that and keeps making requests until either the query is invalid or at least one response
            // is returned.
            return size == 0 ? size : remainingLimit;
        }
    }

    @Override
    protected Object extractValue(BucketExtractor e) {
        return e.extract(buckets.get(row));
    }

    @Override
    protected boolean doHasCurrent() {
        return row < size;
    }

    @Override
    protected boolean doNext() {
        if (row < size - 1) {
            row++;
            return true;
        }
        return false;
    }

    @Override
    protected void doReset() {
        row = 0;
    }

    @Override
    public int size() {
        return size;
    }

    int remainingData() {
        return remainingData;
    }

    Map<String, Object> afterKey() {
        return afterKey;
    }
}
