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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.util.automaton.ByteRunAutomaton;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.lucene.search.cost.TermsQueryCostEstimator;

import java.io.IOException;
import java.util.function.Supplier;

/**
 * The per-leaf execution RAM of Lucene's shared multi-term constant-score rewrite (a {@code MultiTermQuery}, or the
 * package-private {@code MultiTermQueryConstantScoreWrapper}/{@code MultiTermQueryConstantScoreBlendedWrapper} it rewrites
 * into), sized via {@link TermsQueryCostEstimator#executionBytesForLeaf}. Leaves that Lucene runs as a plain disjunction
 * instead of materialising a result set cost nothing; see {@link #skipsDocIdSet}.
 */
final class MultiTermExecutionEstimator implements LeafExecutionEstimator {

    static final String BREAKER_LABEL = "multiterm-execution";

    /** Mirrors Lucene's package-private {@code AbstractMultiTermQueryConstantScoreWrapper#BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD}. */
    private static final int BOOLEAN_REWRITE_TERM_COUNT_THRESHOLD = 16;

    @Nullable
    private final MultiTermQuery multiTermQuery;

    MultiTermExecutionEstimator(Query costlyQuery) {
        this.multiTermQuery = unwrapMultiTermQuery(costlyQuery);
    }

    @Override
    public long bytesForLeaf(LeafReaderContext ctx, ScorerSupplier inner, long leadCost) throws IOException {
        return skipsDocIdSet(ctx) ? 0L : TermsQueryCostEstimator.executionBytesForLeaf(inner.cost(), ctx.reader().maxDoc());
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
}
