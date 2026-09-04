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
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.TopTermsRewrite;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.cost.FuzzyQueryCostEstimator;

import java.util.BitSet;

/**
 * Factory for {@link FuzzyQuery}. This class does not touch the request circuit breaker; the
 * retained {@link #queryRamBytes(FuzzyQuery)} and the parameter-driven
 * {@link FuzzyQueryCostEstimator} estimate are accumulated by {@code MaxClauseCountQueryVisitor}
 * during the once-per-phase walk in {@code AbstractQueryBuilder#toQuery}, alongside prefix,
 * wildcard, regexp, and range queries. Use {@link #estimateBytes(FuzzyQuery, int)} when accounting
 * outside that walk (e.g. ad-hoc field-type callers) needs the same total.
 */
public final class FuzzyQueries {

    /** Segment-count fallback for {@link FuzzyQueryCostEstimator} when the real leaf count isn't available. */
    public static final int DEFAULT_SEGMENT_COUNT_WHEN_UNKNOWN = 16;

    private FuzzyQueries() {}

    /**
     * Build a {@link FuzzyQuery}. Circuit-breaker accounting is performed by
     * {@code MaxClauseCountQueryVisitor} during the {@code AbstractQueryBuilder#toQuery} walk;
     * the {@code context} parameter is retained for backwards compatibility with callers that
     * also pass it to other parameter-driven query factories.
     *
     * @param rewriteMethod optional; defaults to {@link FuzzyQuery#defaultRewriteMethod(int)}
     * @param context       may be {@code null}; unused for charging — see class javadoc.
     */
    public static FuzzyQuery create(
        Term term,
        int maxEdits,
        int prefixLength,
        int maxExpansions,
        boolean transpositions,
        @Nullable MultiTermQuery.RewriteMethod rewriteMethod,
        @Nullable SearchExecutionContext context,
        String fieldLabel
    ) {
        MultiTermQuery.RewriteMethod effectiveRewrite = rewriteMethod != null
            ? rewriteMethod
            : FuzzyQuery.defaultRewriteMethod(maxExpansions);
        return new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions, effectiveRewrite);
    }

    public static long estimateBytes(FuzzyQuery query) {
        return estimateBytes(query, DEFAULT_SEGMENT_COUNT_WHEN_UNKNOWN);
    }

    /**
     * Total bytes the request circuit breaker should be charged for {@code query}: the retained
     * RAM of the query object plus the parameter-driven {@link FuzzyQueryCostEstimator} estimate.
     * Used by {@code MaxClauseCountQueryVisitor} for fuzzy clauses during the once-per-phase walk.
     */
    public static long estimateBytes(FuzzyQuery query, int segmentCount) {
        BytesRef bytes = query.getTerm().bytes();
        long cost = new FuzzyQueryCostEstimator(
            bytes.length,
            countDistinctUtf8Bytes(bytes),
            query.getMaxEdits(),
            query.getPrefixLength(),
            effectiveMaxExpansions(query),
            segmentCount
        ).estimate();
        return queryRamBytes(query) + cost;
    }

    /**
     * Effective expansion count for the breaker charge: a {@link TopTermsRewrite}'s configured size,
     * {@link FuzzyQueryCostEstimator#MAX_CHARGED_EXPANSIONS} for the boolean-producing rewrites that
     * can expand up to {@code IndexSearcher.getMaxClauseCount()} (which is never lower, since
     * {@code SearchUtils.calculateMaxClauseValue} floors it at that same value), or
     * {@link FuzzyQuery#defaultMaxExpansions} otherwise; clamped to
     * {@code [1, }{@link FuzzyQueryCostEstimator#MAX_CHARGED_EXPANSIONS}{@code ]}.
     */
    private static int effectiveMaxExpansions(FuzzyQuery query) {
        MultiTermQuery.RewriteMethod rewrite = query.getRewriteMethod();
        final int requested;
        if (rewrite instanceof TopTermsRewrite<?> topTerms) {
            requested = topTerms.getSize();
        } else if (rewrite == MultiTermQuery.SCORING_BOOLEAN_REWRITE || rewrite == MultiTermQuery.CONSTANT_SCORE_BOOLEAN_REWRITE) {
            requested = FuzzyQueryCostEstimator.MAX_CHARGED_EXPANSIONS;
        } else {
            requested = FuzzyQuery.defaultMaxExpansions;
        }
        return Math.clamp(requested, 1, FuzzyQueryCostEstimator.MAX_CHARGED_EXPANSIONS);
    }

    /** RAM bytes retained by the {@link FuzzyQuery} object (excluding compiled automata). */
    public static long queryRamBytes(FuzzyQuery query) {
        return RamUsageEstimator.shallowSizeOfInstance(query.getClass()) + query.getTerm().ramBytesUsed();
    }

    /** Distinct UTF-8 byte values in {@code bytes}; alphabet hint for {@link FuzzyQueryCostEstimator}. */
    private static int countDistinctUtf8Bytes(BytesRef bytes) {
        BitSet seen = new BitSet(256);
        int end = bytes.offset + bytes.length;
        for (int i = bytes.offset; i < end; i++) {
            seen.set(bytes.bytes[i] & 0xff);
        }
        return seen.cardinality();
    }
}
