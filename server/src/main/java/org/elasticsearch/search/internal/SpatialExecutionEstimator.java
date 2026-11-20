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
import org.apache.lucene.search.ScorerSupplier;
import org.elasticsearch.lucene.search.cost.DocIdSetBuilderCostEstimator;

import java.io.IOException;

/**
 * The per-leaf execution RAM of a Lucene {@code SpatialQuery} over points. What it builds depends on the query relation,
 * which is exposed only through the package-private {@code SpatialQuery}, so this reserves the most that any relation holds at once:
 * two {@code FixedBitSet(maxDoc)} for {@code CONTAINS}, and for {@code WITHIN} and {@code DISJOINT} on multivalued leaves.
 * That covers the single bit set of the other paths. Their {@code DocIdSetBuilder} peaks at one bit set plus buffers of at most
 * {@code maxDoc / 32} bytes, under the second bit set except for the builder's fixed overhead on leaves of under a thousand
 * documents, so that overhead is added on top. It does not ask for {@link ScorerSupplier#cost()}, a BKD walk that Lucene skips on
 * some of these paths.
 */
final class SpatialExecutionEstimator implements LeafExecutionEstimator {

    private final String field;

    SpatialExecutionEstimator(String field) {
        this.field = field;
    }

    @Override
    public long bytesForLeaf(LeafReaderContext ctx, ScorerSupplier inner, long leadCost) throws IOException {
        if (ctx.reader().getPointValues(field) == null) {
            return 0L;
        }
        return peakBytes(ctx.reader().maxDoc());
    }

    static long peakBytes(int maxDoc) {
        return 2 * PointsExecutionEstimator.bitSetBytes(maxDoc) + DocIdSetBuilderCostEstimator.BUILDER_BASE_BYTES;
    }
}
