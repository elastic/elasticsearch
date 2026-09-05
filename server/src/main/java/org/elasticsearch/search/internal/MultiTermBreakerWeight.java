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
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.lucene.search.cost.TermsQueryCostEstimator;

import java.io.IOException;

/**
 * Transparent {@link Weight} wrapper that charges the request circuit breaker for the per-leaf
 * execution RAM Lucene's shared multi-term constant-score rewrite (a {@code MultiTermQuery}, or the
 * package-private {@code MultiTermQueryConstantScoreWrapper}/{@code MultiTermQueryConstantScoreBlendedWrapper}
 * it rewrites into) allocates, sized via {@link TermsQueryCostEstimator#executionBytesForLeaf} and
 * released by {@link ContextIndexSearcher#searchLeaf}. Unlike {@link PointRangeBreakerWeight}, there is
 * no points/doc-values branch or match-all short-circuit here: every leaf that produces a scorer
 * materialises a result set, so every leaf is charged. Charges go through
 * {@link ContextIndexSearcher#chargeLeaf} rather than a cached accounting reference, so this survives a
 * {@link ContextIndexSearcher#setCircuitBreaker} swap.
 */
final class MultiTermBreakerWeight extends Weight {

    private final ContextIndexSearcher searcher;
    private final Weight in;

    MultiTermBreakerWeight(ContextIndexSearcher searcher, Weight in) {
        super(in.getQuery());
        this.searcher = searcher;
        this.in = in;
    }

    @Override
    public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
        final ScorerSupplier inner = in.scorerSupplier(context);
        if (inner == null) {
            return null;
        }
        final long charge = TermsQueryCostEstimator.executionBytesForLeaf(inner.cost(), context.reader().maxDoc());

        return new ScorerSupplier() {
            @Override
            public Scorer get(long leadCost) throws IOException {
                searcher.chargeLeaf(context, charge, "multiterm-execution");
                return inner.get(leadCost);
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                searcher.chargeLeaf(context, charge, "multiterm-execution");
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
