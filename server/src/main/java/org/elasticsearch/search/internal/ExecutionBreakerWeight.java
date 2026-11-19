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
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.FilterWeight;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.internal.ExecutionEstimators.ExecutionCharge;

import java.io.IOException;

/**
 * Transparent {@link Weight} wrapper that charges the request circuit breaker for the per-leaf execution RAM a query
 * allocates, sized by the {@link ExecutionCharge}'s estimator when a scorer is built and released by the
 * {@link LeafExecutionScope} that built it, or by {@link ContextIndexSearcher#searchLeaf} when there is none. A leaf the
 * estimator sizes at {@code 0} is not charged. Charges go through
 * {@link ContextIndexSearcher#chargeLeaf} rather than a cached accounting reference, so this survives a
 * {@link ContextIndexSearcher#setCircuitBreaker} swap. See {@link ExecutionEstimators} for the queries that are charged.
 */
final class ExecutionBreakerWeight extends FilterWeight {

    private final ContextIndexSearcher searcher;
    private final ExecutionCharge charge;

    ExecutionBreakerWeight(ContextIndexSearcher searcher, Weight in, ExecutionCharge charge) {
        super(in);
        this.searcher = searcher;
        this.charge = charge;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final ScorerSupplier inner = in.scorerSupplier(context);
        if (inner == null) {
            return null;
        }

        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                chargeLeaf(context, inner, leadCost);
                return inner.get(leadCost);
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                chargeLeaf(context, inner, Long.MAX_VALUE);
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

    private void chargeLeaf(LeafReaderContext context, ScorerSupplier inner, long leadCost) throws IOException {
        final long bytes = charge.estimator().bytesForLeaf(context, inner, leadCost);
        if (bytes > 0) {
            searcher.chargeLeaf(context, bytes, charge.label());
        }
    }
}
