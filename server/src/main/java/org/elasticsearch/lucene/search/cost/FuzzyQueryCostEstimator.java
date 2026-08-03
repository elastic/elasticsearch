/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

import org.apache.lucene.util.automaton.LevenshteinAutomata;

/**
 * {@link QueryCostEstimator} for a Lucene {@link org.apache.lucene.search.FuzzyQuery}.
 */
public final class FuzzyQueryCostEstimator implements QueryCostEstimator {

    /** Flat per-fuzzy-clause add-on charged when {@code maxEdits >= 1}. */
    public static final long BASE_BYTES = 4096L;

    /**
     * Per-effective-byte coefficient. Sized so the formula stays a ceiling on Lucene's
     * {@code sum(CompiledAutomaton#ramBytesUsed)} across the parameter grid asserted by
     * {@code FuzzyQueryCostEstimatorTests}.
     */
    public static final long BYTES_PER_BYTE = 8192L;

    /** Distinct UTF-8 byte count above which the alphabet is treated as &quot;wide&quot;. */
    public static final int WIDE_ALPHABET_THRESHOLD = 64;

    /** Multiplier applied when the alphabet is wider than {@link #WIDE_ALPHABET_THRESHOLD}. */
    public static final long WIDE_ALPHABET_FACTOR = 2L;

    /**
     * Per-expanded-term add-on, charged once per {@code maxExpansions} to cover each expansion's
     * retained RAM, dominated by {@link org.apache.lucene.index.TermStates} (whose size grows with
     * the number of index segments a term appears in). Tuned against
     * {@code FuzzyQueryCostEstimatorBenchmark}, which measures actual rewrite RAM across term
     * lengths, alphabets, {@code maxExpansions} and index segment counts (1-16 segments); together
     * with {@link #BASE_BYTES} and the automaton term below, this value keeps {@link #estimate()}
     * a ceiling over every combination measured by that benchmark.
     */
    public static final long EXPANSION_BYTES_PER_TERM = 2500L;

    /** Maximum number of expansions charged per clause */
    public static final int MAX_CHARGED_EXPANSIONS = 1024;

    private final int termByteLength;
    private final int distinctUtf8Bytes;
    private final int maxEdits;
    private final int prefixByteLength;
    private final int maxExpansions;

    /**
     * @param termByteLength    UTF-8 byte length of the term. Must be {@code >= 0}.
     * @param distinctUtf8Bytes number of distinct byte values in the term's UTF-8 encoding,
     *                          used as a coarse alphabet-width signal. Conservative callers
     *                          may pass {@code 256}. Must be {@code >= 0}.
     * @param maxEdits          {@code [0, MAXIMUM_SUPPORTED_DISTANCE]}.
     * @param prefixByteLength  UTF-8 byte length of the common non-fuzzy prefix; clamped to
     *                          {@code termByteLength}. Must be {@code >= 0}.
     * @param maxExpansions     maximum number of index terms this clause rewrites to; the
     *                          downstream rewrite/expansion cost is bounded by it. Must be
     *                          {@code > 0}.
     */
    public FuzzyQueryCostEstimator(int termByteLength, int distinctUtf8Bytes, int maxEdits, int prefixByteLength, int maxExpansions) {
        if (termByteLength < 0) {
            throw new IllegalArgumentException("termByteLength must be >= 0, got: " + termByteLength);
        }
        if (distinctUtf8Bytes < 0) {
            throw new IllegalArgumentException("distinctUtf8Bytes must be >= 0, got: " + distinctUtf8Bytes);
        }
        if (maxEdits < 0 || maxEdits > LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE) {
            throw new IllegalArgumentException(
                "maxEdits must be 0.." + LevenshteinAutomata.MAXIMUM_SUPPORTED_DISTANCE + ", got: " + maxEdits
            );
        }
        if (prefixByteLength < 0) {
            throw new IllegalArgumentException("prefixByteLength must be >= 0, got: " + prefixByteLength);
        }
        if (maxExpansions <= 0) {
            throw new IllegalArgumentException("maxExpansions must be > 0, got: " + maxExpansions);
        }
        this.termByteLength = termByteLength;
        this.distinctUtf8Bytes = distinctUtf8Bytes;
        this.maxEdits = maxEdits;
        this.prefixByteLength = prefixByteLength;
        this.maxExpansions = maxExpansions;
    }

    /**
     * Coarse, conservative upper bound on the bytes a fuzzy clause should reserve from the
     * request circuit breaker, computed before any compiled automaton is built using a
     * closed-form formula over the query parameters:
     *
     * <pre>
     *     estimate = 0,                                                  if maxEdits == 0
     *              = BASE
     *              + BYTES_PER_BYTE
     *                · effectiveBytes
     *                · (2 · maxEdits + 1)
     *                · alphabetFactor
     *              + EXPANSION_BYTES_PER_TERM · maxExpansions,            otherwise
     *
     *     effectiveBytes = max(0, termByteLength - prefixByteLength)
     *     alphabetFactor = (distinctUtf8Bytes &gt; WIDE_ALPHABET_THRESHOLD) ? WIDE_ALPHABET_FACTOR : 1
     * </pre>
     *
     * <p>The {@code (2 · maxEdits + 1)} factor is the Levenshtein automaton's per-suffix-byte
     * state-count growth, so {@code effectiveBytes · (2·maxEdits+1)} is a state-count proxy that
     * upper-bounds both compiled-automaton retained RAM and the per-document automaton-walk work
     * paid at run time.
     *
     * <p>The {@code EXPANSION_BYTES_PER_TERM · maxExpansions} term covers the cost of expanding the
     * clause to up to {@code maxExpansions} index terms. Added regardless of term length, it gives
     * short "cheap" fuzzy clauses a per-clause floor that scales with the expansion fan-out.
     */
    @Override
    public long estimate() {
        if (maxEdits == 0) {
            return 0L;
        }
        long effectiveBytes = Math.max(0L, (long) termByteLength - Math.min(prefixByteLength, termByteLength));
        long alphabetFactor = distinctUtf8Bytes > WIDE_ALPHABET_THRESHOLD ? WIDE_ALPHABET_FACTOR : 1L;
        long stateProxy = effectiveBytes * (2L * maxEdits + 1L);
        long dynamic = Math.multiplyExact(Math.multiplyExact(BYTES_PER_BYTE, stateProxy), alphabetFactor);
        long expansion = Math.multiplyExact(EXPANSION_BYTES_PER_TERM, (long) maxExpansions);
        return Math.addExact(Math.addExact(BASE_BYTES, dynamic), expansion);
    }
}
