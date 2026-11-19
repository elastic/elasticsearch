/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.ScorerSupplier;
import org.elasticsearch.lucene.search.cost.PointRangeQueryCostEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * The per-leaf execution RAM of a {@link PointRangeQuery}, sized via {@link PointRangeQueryCostEstimator#executionBytesForLeaf}:
 * nothing when the leaf has no points for the field or the range covers all of its values, otherwise the doc id set the points
 * branch materialises.
 */
final class PointRangeExecutionEstimator implements LeafExecutionEstimator {

    static final String BREAKER_LABEL = "pointrange-execution";

    private final PointRangeQuery pointRangeQuery;

    PointRangeExecutionEstimator(PointRangeQuery pointRangeQuery) {
        this.pointRangeQuery = pointRangeQuery;
    }

    @Override
    public long bytesForLeaf(LeafReaderContext ctx, ScorerSupplier inner, long leadCost) throws IOException {
        final PointValues values = ctx.reader().getPointValues(pointRangeQuery.getField());
        if (values == null) {
            return 0L;
        }
        return PointRangeQueryCostEstimator.executionBytesForLeaf(
            inner.cost(),
            ctx.reader().maxDoc(),
            values.size() == values.getDocCount(),
            coversAllValues(values),
            pointRangeQuery.getNumDims(),
            pointRangeQuery.getBytesPerDim()
        );
    }

    /** Whether the query range covers the leaf's entire indexed value range (Lucene's no-alloc match-all path). */
    private boolean coversAllValues(PointValues values) throws IOException {
        final byte[] minPackedValue = values.getMinPackedValue();
        final byte[] maxPackedValue = values.getMaxPackedValue();
        final byte[] lower = pointRangeQuery.getLowerPoint();
        final byte[] upper = pointRangeQuery.getUpperPoint();
        final int numDims = pointRangeQuery.getNumDims();
        final int bytesPerDim = pointRangeQuery.getBytesPerDim();
        for (int dim = 0; dim < numDims; dim++) {
            final int offset = dim * bytesPerDim;
            final int to = offset + bytesPerDim;
            // lower must be <= the leaf minimum and upper must be >= the leaf maximum for this dimension.
            if (Arrays.compareUnsigned(lower, offset, to, minPackedValue, offset, to) > 0
                || Arrays.compareUnsigned(upper, offset, to, maxPackedValue, offset, to) < 0) {
                return false;
            }
        }
        return true;
    }
}
