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
import org.apache.lucene.util.automaton.LevenshteinAutomata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.SearchExecutionContext;

import java.util.BitSet;

/**
 * Factory helpers for constructing fuzzy queries with circuit-breaker accounting. All
 * production paths that build a {@link FuzzyQuery} for search should go through here so the
 * per-clause constant cost and the parameter-driven estimation work are charged consistently.
 *
 * <p>Charges are split across two pools on {@link SearchExecutionContext}, mirroring the
 * framing in Jim Ferenczi's review of #148621 — &quot;a constant cost to every clause and
 * when we know that this constant cost can be higher depending on the parameters in the
 * query then we do the estimation work&quot;:
 *
 * <ol>
 *   <li>The query object's own RAM ({@link #queryRamBytes}) goes to the construction pool via
 *       {@link SearchExecutionContext#addCircuitBreakerMemory} — the per-clause constant
 *       charged at the field-type layer so parsers that bypass
 *       {@link org.elasticsearch.index.query.LeafQueryBuilder}
 *       (e.g. {@code QueryStringQueryParser}) still trip the breaker incrementally.</li>
 *   <li>A coarse, parameter-driven cost estimate from {@link FuzzyQueryCostEstimator} goes to
 *       the rewrite pool via {@link SearchExecutionContext#addRewriteCircuitBreakerMemory} —
 *       this is the &quot;estimation work&quot; that earns its keep when the query parameters
 *       (term length, edit distance, alphabet width) make the clause heavier than typical.</li>
 * </ol>
 *
 * <p>{@link #create} is the normal entry point; {@link #chargeQuery} exists for callers that
 * build their own {@link FuzzyQuery} subclass (e.g. {@code VersionStringFieldMapper}). Both
 * apply the same accounting and no-op when {@code context} or its circuit breaker is
 * {@code null}.
 */
public final class FuzzyQueries {

    private FuzzyQueries() {}

    /**
     * Build a {@link FuzzyQuery} and charge its full circuit-breaker cost upfront on
     * {@code context}.
     *
     * @param maxEdits      {@code 0..}{@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}
     * @param rewriteMethod optional; defaults to {@link FuzzyQuery#defaultRewriteMethod(int)}
     * @param context       may be {@code null} for non-search paths (no charging)
     * @param fieldLabel    label used in circuit-breaker messages (typically the field name)
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
     * Charge the full circuit-breaker cost of an already-constructed {@link FuzzyQuery} —
     * the per-clause constant (query object RAM) to the construction pool and the
     * parameter-driven cost estimate to the rewrite pool. No-op when {@code context} or
     * its breaker is {@code null}.
     */
    public static void chargeQuery(FuzzyQuery query, @Nullable SearchExecutionContext context, String fieldLabel) {
        if (context == null || context.getCircuitBreaker() == null) {
            return;
        }
        String label = "fuzzy:" + fieldLabel;
        context.addCircuitBreakerMemory(queryRamBytes(query), label);
        BytesRef bytes = query.getTerm().bytes();
        new FuzzyQueryCostEstimator(bytes.length, countDistinctUtf8Bytes(bytes), query.getMaxEdits(), query.getPrefixLength())
            .chargeRewrite(context, label);
    }

    /** RAM bytes retained by the {@link FuzzyQuery} object itself (excluding compiled automata). */
    public static long queryRamBytes(FuzzyQuery query) {
        return RamUsageEstimator.shallowSizeOfInstance(query.getClass()) + query.getTerm().ramBytesUsed();
    }

    /**
     * Number of distinct UTF-8 byte values in {@code bytes} — the alphabet hint for
     * {@link FuzzyQueryCostEstimator}. A tighter value tightens the estimate (e.g.
     * {@code "aaaaa..."} has {@code distinctUtf8Bytes = 1}).
     */
    private static int countDistinctUtf8Bytes(BytesRef bytes) {
        BitSet seen = new BitSet(256);
        int end = bytes.offset + bytes.length;
        for (int i = bytes.offset; i < end; i++) {
            seen.set(bytes.bytes[i] & 0xff);
        }
        return seen.cardinality();
    }
}
