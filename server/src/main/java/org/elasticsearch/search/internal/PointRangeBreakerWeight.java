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
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.lucene.search.cost.PointRangeQueryCostEstimator;

import java.io.IOException;
import java.util.Arrays;

/**
 * Transparent {@link Weight} wrapper that charges the request circuit breaker for the per-leaf
 * execution RAM a {@link PointRangeQuery} allocates, sized via
 * {@link PointRangeQueryCostEstimator#executionBytesForLeaf} and released by
 * {@link ContextIndexSearcher#searchLeaf}. The charge is applied only when the bitset-allocating
 * points branch runs: always for a bare {@link PointRangeQuery}, and for an
 * {@link org.apache.lucene.search.IndexOrDocValuesQuery} only on the path Lucene routes to points
 * (otherwise doc values run and allocate no bitset).
 */
final class PointRangeBreakerWeight extends Weight {

    private final ContextIndexSearcher searcher;
    private final Weight in;
    private final PointRangeQuery pointRangeQuery;
    private final boolean indexOrDocValues;

    PointRangeBreakerWeight(ContextIndexSearcher searcher, Weight in, PointRangeQuery pointRangeQuery, boolean indexOrDocValues) {
        super(in.getQuery());
        this.searcher = searcher;
        this.in = in;
        this.pointRangeQuery = pointRangeQuery;
        this.indexOrDocValues = indexOrDocValues;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final ScorerSupplier inner = in.scorerSupplier(context);
        if (inner == null) {
            return null;
        }
        final PointValues values = context.reader().getPointValues(pointRangeQuery.getField());
        if (values == null) {
            return inner;
        }
        final int leafMaxDoc = context.reader().maxDoc();
        final boolean singleValued = values.size() == values.getDocCount();
        final boolean matchAll = coversAllValues(values);
        final long cost = inner.cost();
        final long charge = PointRangeQueryCostEstimator.executionBytesForLeaf(
            cost,
            leafMaxDoc,
            singleValued,
            matchAll,
            pointRangeQuery.getNumDims(),
            pointRangeQuery.getBytesPerDim()
        );

        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                if (indexOrDocValues == false || (cost >>> 3) <= leadCost) {
                    searcher.chargeLeafExecutionBytes(charge);
                }
                return inner.get(leadCost);
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                searcher.chargeLeafExecutionBytes(charge);
                return inner.bulkScorer();
            }

            @Override
            public long cost() {
                return inner.cost();
            }

            @Override
            public void setTopLevelScoringClause() throws IOException {
                inner.setTopLevelScoringClause();
            }
        };
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

    @Override
    public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        return in.explain(context, doc);
    }

    @Override
    public int count(LeafReaderContext context) throws IOException {
        return in.count(context);
    }

    @Override
    public Matches matches(LeafReaderContext context, int doc) throws IOException {
        return in.matches(context, doc);
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
        return in.isCacheable(ctx);
    }
}
