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
 * whose {@link ScorerSupplier#cost()} is {@link PointValues#estimateDocCount} of that intersection.
 * Sized via {@link DocIdSetBuilderCostEstimator}. Nothing when the leaf has no points for the field or the query matches nothing.
 */
class PointsExecutionEstimator implements LeafExecutionEstimator {

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
        return bytesForMatches(values, ctx.reader().maxDoc(), cost);
    }

    /** Sizes the set the query collects its matches in, on a leaf with points for the field and an estimated doc count above 0. */
    long bytesForMatches(PointValues values, int maxDoc, long cost) {
        return DocIdSetBuilderCostEstimator.forPoints(cost, values.size(), values.getDocCount(), maxDoc).estimate();
    }
}
