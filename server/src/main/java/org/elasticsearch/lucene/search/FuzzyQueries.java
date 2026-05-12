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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.lucene.search.cost.FuzzyQueryCostEstimator;

import java.util.BitSet;

/**
 * Factory for {@link FuzzyQuery} that charges the request circuit breaker for both the query
 * object's RAM (constant per-clause cost) and a parameter-driven estimate from
 * {@link FuzzyQueryCostEstimator} (dynamic cost driven by term length, edit distance, etc.).
 */
public final class FuzzyQueries {

    private FuzzyQueries() {}

    /**
     * Build a {@link FuzzyQuery} and charge the circuit breaker on {@code context}.
     *
     * @param rewriteMethod optional; defaults to {@link FuzzyQuery#defaultRewriteMethod(int)}
     * @param context       may be {@code null} (no charging)
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
        FuzzyQuery query = new FuzzyQuery(term, maxEdits, prefixLength, maxExpansions, transpositions, effectiveRewrite);
        chargeQuery(query, context, fieldLabel);
        return query;
    }

    /**
     * Charge the circuit breaker for an already-constructed {@link FuzzyQuery}: the query
     * object's bytes plus the parameter-driven cost estimate. No-op when {@code context} or its
     * breaker is {@code null}.
     */
    public static void chargeQuery(FuzzyQuery query, @Nullable SearchExecutionContext context, String fieldLabel) {
        if (context == null || context.getCircuitBreaker() == null) {
            return;
        }
        String label = "fuzzy:" + fieldLabel;
        context.addCircuitBreakerMemory(queryRamBytes(query), label);
        BytesRef bytes = query.getTerm().bytes();
        new FuzzyQueryCostEstimator(bytes.length, countDistinctUtf8Bytes(bytes), query.getMaxEdits(), query.getPrefixLength()).charge(
            context,
            label
        );
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
