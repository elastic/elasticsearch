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
import org.apache.lucene.search.ScorerSupplier;
import org.elasticsearch.lucene.search.cost.DocIdSetBuilderCostEstimator;

import java.io.IOException;

/**
 * The per-leaf execution RAM of a points query that intersects the field's BKD tree into a {@code DocIdSetBuilder} and
 * whose {@link ScorerSupplier#cost()} is {@link PointValues#estimateDocCount} of that intersection, such as
 * {@link org.apache.lucene.search.PointInSetQuery} and {@link org.apache.lucene.document.RangeFieldQuery}. Sized via
 * {@link DocIdSetBuilderCostEstimator}; nothing when the leaf has no points for the field or the query matches nothing.
 */
final class PointsExecutionEstimator implements LeafExecutionEstimator {

    static final String BREAKER_LABEL = "points-execution";

    private final String field;

    PointsExecutionEstimator(String field) {
        this.field = field;
    }

    @Override
    public long bytesForLeaf(LeafReaderContext ctx, ScorerSupplier inner, long leadCost) throws IOException {
        final PointValues values = ctx.reader().getPointValues(field);
        if (values == null) {
            return 0L;
        }
        final long cost = inner.cost();
        if (cost <= 0) {
            return 0L;
        }
        return new DocIdSetBuilderCostEstimator(grownValues(cost, values), ctx.reader().maxDoc()).estimate();
    }

    /**
     * Bound on the count an intersection whose estimated doc count is {@code cost} passes to {@code DocIdSetBuilder#grow}.
     * The BKD reader grows by a whole leaf block where the block crosses the query, and {@link PointValues#estimatePointCount}
     * counts such a block as half matching, so the intersection grows by at most twice the estimated point count. On a
     * multi-valued field the estimated doc count is that point count scaled down by at most {@code size / docCount}, and
     * then truncated.
     */
    static long grownValues(long cost, PointValues values) {
        final int docCount = values.getDocCount();
        final long size = values.size();
        final double valuesPerDoc = docCount > 0 && size > docCount ? (double) size / docCount : 1d;
        // the cast saturates at Long.MAX_VALUE
        return (long) Math.ceil(2d * (cost + 1) * valuesPerDoc);
    }
}
