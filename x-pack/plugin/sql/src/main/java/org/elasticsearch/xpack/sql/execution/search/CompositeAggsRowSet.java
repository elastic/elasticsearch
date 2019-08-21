/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.xpack.sql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.util.BitSet;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * {@link RowSet} specific to (GROUP BY) aggregation.
 */
class CompositeAggsRowSet extends ResultRowSet<BucketExtractor> {

    private final List<? extends CompositeAggregation.Bucket> buckets;
    private final int remainingData;
    private final int size;
    private int row = 0;

    CompositeAggsRowSet(List<BucketExtractor> exts, BitSet mask, SearchResponse response, int limit, byte[] next) {
        super(exts, mask);

        CompositeAggregation composite = CompositeAggregationCursor.getComposite(response);
        if (composite != null) {
            buckets = composite.getBuckets();
        } else {
            buckets = emptyList();
        }

        // page size
        size = limit == -1 ? buckets.size() : Math.min(buckets.size(), limit);

        if (next == null) {
            remainingData = 0;
        } else {
            // Compute remaining limit

            // If the limit is -1 then we have a local sorting (sort on aggregate function) that requires all the buckets
            // to be processed so we stop only when all data is exhausted.
            int remainingLimit = (limit == -1) ? limit : ((limit - size) >= 0 ? (limit - size) : 0);

            // if the computed limit is zero, or the size is zero it means either there's nothing left or the limit has been reached
            // note that a composite agg might be valid but return zero groups (since these can be filtered with HAVING/bucket selector)
            // however the Querier takes care of that and keeps making requests until either the query is invalid or at least one response
            // is returned.
            if (size == 0 || remainingLimit == 0) {
                remainingData = 0;
            } else {
                remainingData = remainingLimit;
            }
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
}