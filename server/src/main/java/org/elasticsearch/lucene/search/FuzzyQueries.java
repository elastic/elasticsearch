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
 * query-object RAM and the Levenshtein automata RAM are charged consistently.
 *
 * <p>Both contributions are charged <em>upfront</em>, at query construction time, before any
 * automaton is built — this lets the breaker reject oversized queries before any allocation
 * happens. They live in different pools so they can be released independently:
 * <ol>
 *   <li>The query object's own RAM ({@link #queryRamBytes}) goes to the construction pool via
 *       {@link SearchExecutionContext#addCircuitBreakerMemory}.</li>
 *   <li>The compiled-automata RAM, estimated by {@link FuzzyAutomatonRamEstimator}, goes to
 *       the rewrite pool via {@link SearchExecutionContext#addRewriteCircuitBreakerMemory}.</li>
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
     * query-object RAM to the construction pool and an estimate of the Levenshtein automata
     * RAM to the rewrite pool. No-op when {@code context} or its breaker is {@code null}.
     */
    public static void chargeQuery(FuzzyQuery query, @Nullable SearchExecutionContext context, String fieldLabel) {
        if (context == null || context.getCircuitBreaker() == null) {
            return;
        }
        String label = "fuzzy:" + fieldLabel;
        context.addCircuitBreakerMemory(queryRamBytes(query), label);
        long automataBytes = estimateAutomataBytes(query);
        if (automataBytes > 0) {
            context.addRewriteCircuitBreakerMemory(automataBytes, label);
        }
    }

    /** RAM bytes retained by the {@link FuzzyQuery} object itself (excluding compiled automata). */
    public static long queryRamBytes(FuzzyQuery query) {
        return RamUsageEstimator.shallowSizeOfInstance(query.getClass()) + query.getTerm().ramBytesUsed();
    }

    private static long estimateAutomataBytes(FuzzyQuery query) {
        if (query.getMaxEdits() == 0) {
            return 0L;
        }
        BytesRef bytes = query.getTerm().bytes();
        return FuzzyAutomatonRamEstimator.estimate(
            countCodePoints(bytes),
            bytes.length,
            countDistinctUtf8Bytes(bytes),
            query.getMaxEdits(),
            query.getPrefixLength()
        );
    }

    /**
     * Code-point count of a UTF-8 {@link BytesRef} without materialising a {@code String}.
     * UTF-8 leading bytes are those <em>not</em> matching {@code 10xxxxxx}.
     */
    private static int countCodePoints(BytesRef bytes) {
        int count = 0;
        int end = bytes.offset + bytes.length;
        for (int i = bytes.offset; i < end; i++) {
            if ((bytes.bytes[i] & 0xC0) != 0x80) {
                count++;
            }
        }
        return count;
    }

    /**
     * Number of distinct UTF-8 byte values in {@code bytes} — the alphabet hint for
     * {@link FuzzyAutomatonRamEstimator}. A tighter value tightens the estimate (e.g.
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
