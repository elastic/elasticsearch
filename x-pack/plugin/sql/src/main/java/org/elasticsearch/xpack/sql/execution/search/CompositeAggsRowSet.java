/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.xpack.sql.execution.search.extractor.BucketExtractor;
import org.elasticsearch.xpack.sql.session.Cursor;
import org.elasticsearch.xpack.sql.session.RowSet;

import java.util.BitSet;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * {@link RowSet} specific to (GROUP BY) aggregation.
 */
class CompositeAggsRowSet extends ResultRowSet<BucketExtractor> {

    private final List<? extends CompositeAggregation.Bucket> buckets;

    private final Cursor cursor;

    private final int size;
    private int row = 0;

    CompositeAggsRowSet(List<BucketExtractor> exts, BitSet mask, SearchResponse response, int limit, byte[] next, String... indices) {
        super(exts, mask);

        CompositeAggregation composite = CompositeAggregationCursor.getComposite(response);
        if (composite != null) {
            buckets = composite.getBuckets();
        } else {
            buckets = emptyList();
        }

        // page size
        size = limit < 0 ? buckets.size() : Math.min(buckets.size(), limit);

        if (next == null) {
            cursor = Cursor.EMPTY;
        } else {
            // compute remaining limit
            int remainingLimit = limit - size;
            // if the computed limit is zero, or the size is zero it means either there's nothing left or the limit has been reached
            // note that a composite agg might be valid but return zero groups (since these can be filtered with HAVING/bucket selector)
            // however the Querier takes care of that and keeps making requests until either the query is invalid or at least one response
            // is returned
            if (next == null || size == 0 || remainingLimit == 0) {
                cursor = Cursor.EMPTY;
            } else {
                cursor = new CompositeAggregationCursor(next, exts, mask, remainingLimit, indices);
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

    @Override
    public Cursor nextPageCursor() {
        return cursor;
    }
}