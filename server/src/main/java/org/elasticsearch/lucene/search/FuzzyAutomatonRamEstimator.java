/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search;

import org.apache.lucene.util.automaton.LevenshteinAutomata;

/**
 * Closed-form, pre-flight estimator for the RAM retained by the
 * {@link org.apache.lucene.util.automaton.CompiledAutomaton} instances Lucene builds for a
 * fuzzy query. Used to charge the circuit breaker <em>before</em> any Levenshtein automaton
 * is constructed.
 *
 * <p>For a query with {@code maxEdits = E} the total is the sum over edit levels
 * {@code e ∈ [0, E]} of:
 *
 * <pre>
 *     bytes(e)      = PER_AUTOMATON_OVERHEAD + stateCount(e) · perState(e)
 *     stateCount(e) = effectiveByteLen · (2·e + 1) + 1
 *     perState(e)   = max(0, BASE[e] + COEF[e] · sqrt(min(bd, 256)))
 * </pre>
 *
 * where {@code effectiveByteLen} is the term's UTF-8 byte length minus the prefix's byte
 * share and {@code bd = distinctUtf8Bytes} is the byte-level alphabet width. State count
 * is driven by UTF-8 byte length (not codepoints) because the compiled automaton wraps a
 * {@code ByteRunAutomaton}; the {@code sqrt(bd)} alphabet term lets one calibration cover
 * both ASCII and multi-byte alphabets.
 */
public final class FuzzyAutomatonRamEstimator {

    /**
     * {@code BASE[e]} in the per-state formula, indexed by edit level. {@code BASE[2]} is
     * negative on purpose — the level-2 fit is a {@code sqrt(bd)} line with a negative
     * intercept; {@code perState} is clamped to {@code >= 0} before use. Sized for
     * {@code maxEdits ≤ 2}; grow alongside {@link LevenshteinAutomata#MAXIMUM_SUPPORTED_DISTANCE}.
     */
    static final long[] BYTES_PER_STATE_BASE_BY_LEVEL = { 80L, 300L, -337L };

    /**
     * {@code COEF[e]} in the per-state formula, indexed by edit level. Multiplied by
     * {@code sqrt(min(bd, 256))} (see {@link #BYTE_DISTINCT_EXPONENT}).
     */
    static final long[] BYTES_PER_STATE_PER_DISTINCT_BYTE_BY_LEVEL = { 5L, 183L, 917L };

    /** Fixed bytes per {@link org.apache.lucene.util.automaton.CompiledAutomaton} that don't scale with state count. */
    static final long PER_AUTOMATON_OVERHEAD = 2000L;

    /** Exponent applied to {@code bd}; {@code 0.5} ({@code sqrt}) is the empirical best fit. */
    static final double BYTE_DISTINCT_EXPONENT = 0.5;

    /** Cap on {@code bd}: a UTF-8 transition table has at most one entry per byte value. */
    static final long MAX_BYTE_DISTINCT_BOUND = 256L;

    private FuzzyAutomatonRamEstimator() {}

    /**
     * Estimated aggregate RAM, in bytes, of the compiled automata for a
     * {@link org.apache.lucene.search.FuzzyQuery} with the given parameters. See the
     * class-level Javadoc for the formula. Returns {@code 0} when {@code maxEdits == 0}.
     *
     * @param termLength        term length in Unicode code points; used only to apportion
     *                          {@code prefixLength}. Must be {@code >= 0}.
     * @param termByteLength    term length in UTF-8 bytes (e.g. {@code BytesRef.length}).
     *                          Conservative callers may pass {@code 4 * termLength}. Must be
     *                          {@code >= 0}.
     * @param distinctUtf8Bytes number of distinct byte values in the UTF-8 encoding;
     *                          conservative callers may pass {@code 256}. Must be {@code >= 0}.
     * @param maxEdits          {@code [0, MAXIMUM_SUPPORTED_DISTANCE]}.
     * @param prefixLength      common non-fuzzy prefix length in code points; clamped to
     *                          {@code termLength}. Must be {@code >= 0}.
     */
    public static long estimate(int termLength, int termByteLength, int distinctUtf8Bytes, int maxEdits, int prefixLength) {
        if (termLength < 0) {
            throw new IllegalArgumentException("termLength must be >= 0, got: " + termLength);
        }
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
        if (prefixLength < 0) {
            throw new IllegalArgumentException("prefixLength must be >= 0, got: " + prefixLength);
        }
        if (maxEdits == 0) {
            return 0L;
        }

        int effectiveByteLen = effectiveSuffixByteLength(termLength, termByteLength, prefixLength);
        double byteDistinctSqrt = byteDistinctSqrt(distinctUtf8Bytes, effectiveByteLen);

        long total = 0L;
        for (int e = 0; e <= maxEdits; e++) {
            total = Math.addExact(total, levelBytes(e, effectiveByteLen, byteDistinctSqrt));
        }
        return total;
    }

    /**
     * UTF-8 byte length of the suffix fed to the Levenshtein automaton: {@code termByteLength}
     * minus the prefix's apportioned byte share. {@code prefixLength} is in code points but
     * state count scales with bytes, so the prefix is charged proportionally.
     */
    private static int effectiveSuffixByteLength(int termLength, int termByteLength, int prefixLength) {
        if (termLength == 0 || termByteLength == 0) {
            return 0;
        }
        int clampedPrefix = Math.min(prefixLength, termLength);
        long prefixByteShare = ((long) clampedPrefix * termByteLength + termLength / 2) / termLength;
        return (int) Math.max(0L, termByteLength - prefixByteShare);
    }

    /**
     * {@code sqrt(min(distinctUtf8Bytes, 256))} — the alphabet term of {@code perState(e)}.
     * Returns {@code 0} when the suffix is empty (single transitionless state, no alphabet
     * contribution). Kept in {@code double}; rounding happens once per level in
     * {@link #levelBytes}.
     */
    private static double byteDistinctSqrt(int distinctUtf8Bytes, int effectiveByteLen) {
        if (effectiveByteLen == 0) {
            return 0.0;
        }
        long byteDistinctBound = Math.min(MAX_BYTE_DISTINCT_BOUND, distinctUtf8Bytes);
        return Math.pow(byteDistinctBound, BYTE_DISTINCT_EXPONENT);
    }

    /**
     * Estimated bytes for one compiled automaton at edit level {@code e}:
     * {@code PER_AUTOMATON_OVERHEAD + stateCount(e) · perState(e)}, with {@code perState}
     * clamped to {@code >= 0} (see {@link #BYTES_PER_STATE_BASE_BY_LEVEL}).
     */
    private static long levelBytes(int e, int effectiveByteLen, double byteDistinctSqrt) {
        long stateCount = (long) effectiveByteLen * (2L * e + 1L) + 1L;
        double rawPerState = BYTES_PER_STATE_BASE_BY_LEVEL[e] + BYTES_PER_STATE_PER_DISTINCT_BYTE_BY_LEVEL[e] * byteDistinctSqrt;
        long bytesPerState = Math.max(0L, Math.round(rawPerState));
        return Math.addExact(PER_AUTOMATON_OVERHEAD, Math.multiplyExact(stateCount, bytesPerState));
    }
}
