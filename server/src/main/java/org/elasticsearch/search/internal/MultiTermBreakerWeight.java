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
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.BulkScorer;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.search.cost.TermsQueryCostEstimator;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * Transparent {@link Weight} wrapper that charges the request circuit breaker for the per-leaf
 * execution RAM Lucene's shared multi-term constant-score rewrite (a {@code MultiTermQuery}, or the
 * package-private {@code MultiTermQueryConstantScoreWrapper}/{@code MultiTermQueryConstantScoreBlendedWrapper}
 * it rewrites into) allocates, sized via {@link TermsQueryCostEstimator#executionBytesForLeaf} and
 * released by {@link ContextIndexSearcher#searchLeaf}. Leaves that Lucene runs as a plain disjunction
 * instead of materialising a result set are not charged; see {@link #skipsDocIdSet}. Charges go through
 * {@link ContextIndexSearcher#chargeLeaf} rather than a cached accounting reference, so this survives a
 * {@link ContextIndexSearcher#setCircuitBreaker} swap.
 */
final class MultiTermBreakerWeight extends Weight {

    /** Mirrors Lucene's package-private {@code AbstractMultiTermQueryConstantScoreWrapper#BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD}. */
    private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

    private final ContextIndexSearcher searcher;
    private final Weight in;
    @Nullable
    private final MultiTermQuery multiTermQuery;

    MultiTermBreakerWeight(ContextIndexSearcher searcher, Weight in) {
        super(in.getQuery());
        this.searcher = searcher;
        this.in = in;
        this.multiTermQuery = unwrapMultiTermQuery(in.getQuery());
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
                chargeLeaf(context, inner.cost());
                return inner.get(leadCost);
            }

            @Override
            public BulkScorer bulkScorer() throws IOException {
                chargeLeaf(context, inner.cost());
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

    private void chargeLeaf(LeafReaderContext context, long cost) throws IOException {
        final long charge = skipsDocIdSet(context) ? 0L : TermsQueryCostEstimator.executionBytesForLeaf(cost, context.reader().maxDoc());
        searcher.chargeLeaf(context, charge, "multiterm-execution");
    }

    private boolean skipsDocIdSet(LeafReaderContext context) throws IOException {
        if (multiTermQuery == null || multiTermQuery.getTermsCount() < 0) {
            return false;
        }
        final int threshold = Math.min(BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD, IndexSearcher.getMaxClauseCount());
        if (multiTermQuery.getTermsCount() <= threshold) {
            return true;
        }
        final Terms terms = context.reader().terms(multiTermQuery.getField());
        if (terms == null) {
            return true;
        }
        final int fieldDocCount = terms.getDocCount();
        final TermsEnum termsEnum = multiTermQuery.getTermsEnum(terms);
        for (int i = 0; i < threshold; i++) {
            if (termsEnum.next() == null || termsEnum.docFreq() == fieldDocCount) {
                return true;
            }
        }
        return termsEnum.next() == null;
    }

    @Nullable
    private static MultiTermQuery unwrapMultiTermQuery(Query query) {
        final MultiTermQuery[] found = new MultiTermQuery[1];
        query.visit(new QueryVisitor() {
            @Override
            public void consumeTerms(Query leafQuery, Term... terms) {
                capture(leafQuery);
            }

            @Override
            public void consumeTermsMatching(Query leafQuery, String field, Supplier<ByteRunAutomaton> automaton) {
                capture(leafQuery);
            }

            private void capture(Query leafQuery) {
                if (leafQuery instanceof MultiTermQuery multiTerm) {
                    found[0] = multiTerm;
                }
            }
        });
        return found[0];
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
