/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MultiTermQuery;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.lucene.search.cost.FuzzyQueryCostEstimator;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Tests the breaker charge {@link FuzzyQueries#estimateBytes} derives from a {@link FuzzyQuery}'s rewrite
 * method. The expansion component of the charge is driven by an attacker-controllable {@code int}
 * ({@code max_expansions} / {@code top_terms_N}), so the clamp applied by {@code effectiveMaxExpansions} is
 * what keeps the estimate proportionate.
 */
public class FuzzyQueriesTests extends ESTestCase {

    private static final Term TERM = new Term("field", "value");

    /**
     * Regression test: {@link org.apache.lucene.search.TopTermsRewrite#getSize()} returns the raw configured
     * size, but Lucene bounds the rewrite with {@code min(size, getMaxSize())}. Charging the raw size made a
     * single fuzzy clause bill over a terabyte and trip the request breaker — {@code DfsProfilerIT} hit this
     * via {@code maxExpansions(Math.abs(randomInt()))}.
     */
    public void testHugeTopTermsSizeIsClampedToMaxChargedExpansions() {
        long huge = estimateWithTopTermsSize(Integer.MAX_VALUE);
        long atCap = estimateWithTopTermsSize(FuzzyQueryCostEstimator.MAX_CHARGED_EXPANSIONS);

        assertEquals("a size above the cap must charge exactly the same as a size at the cap", atCap, huge);
        assertThat("a single fuzzy clause must never charge an unreasonable amount", huge, lessThan(ByteSizeValue.ofMb(2).getBytes()));
    }

    /**
     * Sizes below the cap must still scale, so the clamp does not flatten the expansion signal the charge
     * exists to capture.
     */
    public void testSizesBelowTheCapStillScale() {
        assertThat(estimateWithTopTermsSize(FuzzyQueryCostEstimator.MAX_CHARGED_EXPANSIONS / 2), greaterThan(estimateWithTopTermsSize(10)));
    }

    /**
     * Neither {@code TopTermsRewrite} nor {@code QueryParsers} rejects {@code top_terms_0}, but
     * {@link FuzzyQueryCostEstimator} requires a positive expansion count. Without a lower clamp a
     * {@code "rewrite": "top_terms_0"} request fails with an {@link IllegalArgumentException}.
     */
    public void testZeroSizedTopTermsRewriteIsChargedNotRejected() {
        assertThat(estimateWithTopTermsSize(0), greaterThan(0L));
    }

    /**
     * The boolean-producing rewrites expand up to {@link IndexSearcher#getMaxClauseCount()}, which ES derives
     * from heap size and search thread count. The charge must stay bounded by the cap instead of tracking
     * that mutable global.
     */
    public void testBooleanRewriteChargeDoesNotTrackMaxClauseCount() {
        int original = IndexSearcher.getMaxClauseCount();
        try {
            IndexSearcher.setMaxClauseCount(FuzzyQueryCostEstimator.MAX_CHARGED_EXPANSIONS);
            long atCap = estimateWithRewrite(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);

            IndexSearcher.setMaxClauseCount(100_000);
            long wayAboveCap = estimateWithRewrite(MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE);

            assertEquals("raising maxClauseCount must not raise the per-clause charge", atCap, wayAboveCap);
            assertThat(wayAboveCap, lessThan(ByteSizeValue.ofMb(2).getBytes()));
        } finally {
            IndexSearcher.setMaxClauseCount(original);
        }
    }

    private static long estimateWithTopTermsSize(int size) {
        return estimateWithRewrite(new MultiTermQuery.TopTermsBlendedFreqScoringRewrite(size));
    }

    /**
     * Builds a fuzzy query carrying {@code rewrite}. {@code maxExpansions} is fixed at 1 because Lucene's
     * constructor validates that argument independently of the rewrite's size, and it is the rewrite the
     * charge is derived from.
     */
    private static long estimateWithRewrite(MultiTermQuery.RewriteMethod rewrite) {
        return FuzzyQueries.estimateBytes(new FuzzyQuery(TERM, 1, 0, 1, true, rewrite));
    }
}
