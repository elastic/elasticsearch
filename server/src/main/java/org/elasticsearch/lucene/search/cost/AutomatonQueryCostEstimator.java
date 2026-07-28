/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.cost;

/**
 * {@link QueryCostEstimator} for {@link org.apache.lucene.search.AutomatonQuery} and its
 * subclasses (notably {@code WildcardQuery} and {@code RegexpQuery}).
 * <p>
 * Returns a conservative upper bound on the bytes a wildcard/regexp clause should reserve
 * from the request circuit breaker for the {@code CompiledAutomaton} construction window
 * (UTF-8 byte expansion + second determinize + {@code ByteRunAutomaton}), based on the
 * {@code ramBytesUsed()} of the already-determinized unicode DFA. Charge this on the breaker
 * before invoking {@code AutomatonQuery}'s super-constructor so the unguarded
 * {@code CompiledAutomaton} build window is visible to the breaker.
 * <p>
 * The estimate is {@link #COMPILED_AUTOMATON_PEAK_MULTIPLIER}{@code  × dfaRamBytes}, with
 * a {@link #COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES} lower bound and saturation to
 * {@link Long#MAX_VALUE} on overflow. A {@code Long.MAX_VALUE} reservation is guaranteed to
 * trip any real-world breaker, which is the correct behavior for inputs that large.
 */
public final class AutomatonQueryCostEstimator implements QueryCostEstimator {

    /**
     * Empirical multiplier applied to {@code dfa.ramBytesUsed()} to estimate the peak heap
     * footprint of Lucene's {@code CompiledAutomaton} construction (UTF-8 byte expansion +
     * second determinize + {@code ByteRunAutomaton}). The estimate is reserved on the breaker
     * before {@code AutomatonQuery}'s super-constructor runs, so the in-flight clause is
     * visible to the breaker across the otherwise-unguarded construction window.
     * <p>
     * Sized from heap-measurement data on the patterns that motivated #147428. Across an extended
     * harness corpus (ASCII wildcard, multi-byte UTF-8, regexp, wildcard-{@code ?}), peak-to-DFA
     * ratios cluster around ~70–120× for long ASCII literals and ~150–190× for interleaved
     * adversarial wildcards. {@code 200} covers these typical patterns with at least 1.2× margin;
     * see the known gaps below for adversarial inputs where it under-reserves.
     * <p>
     * Two known gaps where the multiplier under-reserves but the absolute heap impact is bounded:
     * <ul>
     *   <li>Multi-byte UTF-8 long literals (e.g. 500-char Cyrillic): peak ratio ~400×; a 14-clause
     *       request leaves ~140 MB unreserved. Significant but not OOM-causing on production heaps.</li>
     *   <li>Adversarial {@code ?}-only patterns (e.g. {@code ?×100}): peak ratio ~900× but small
     *       absolute peak (~2 MB per clause); cumulative impact is negligible.</li>
     * </ul>
     * The {@link #COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES} floor partially mitigates these
     * gaps by ensuring a minimum reservation regardless of DFA size. Production telemetry on the
     * reservation/actual ratio should be used to refine the multiplier over time.
     */
    static final int COMPILED_AUTOMATON_PEAK_MULTIPLIER = 200;

    /**
     * Lower bound on the pre-flight reservation. Prevents under-reservation for tiny DFAs that
     * disproportionately blow up during {@code CompiledAutomaton} construction (e.g. small
     * automatons with wide-alphabet transitions like {@code ?×N}).
     */
    static final long COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES = 128L * 1024L;

    private final long dfaRamBytes;

    /**
     * @param dfaRamBytes {@code ramBytesUsed()} of the already-determinized unicode DFA that
     *                    will be handed to {@code AutomatonQuery}. Must be {@code >= 0}.
     */
    public AutomatonQueryCostEstimator(long dfaRamBytes) {
        if (dfaRamBytes < 0) {
            throw new IllegalArgumentException("dfaRamBytes must be >= 0, got: " + dfaRamBytes);
        }
        this.dfaRamBytes = dfaRamBytes;
    }

    /**
     * Returns the pre-flight breaker reservation in bytes. The result is at least
     * {@link #COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES} and at most {@link Long#MAX_VALUE} — the
     * multiplier is applied with saturation so a pathologically large DFA cannot wrap to a small
     * reservation.
     */
    @Override
    public long estimate() {
        long peak;
        try {
            peak = Math.multiplyExact(dfaRamBytes, (long) COMPILED_AUTOMATON_PEAK_MULTIPLIER);
        } catch (ArithmeticException e) {
            peak = Long.MAX_VALUE;
        }
        return Math.max(peak, COMPILED_AUTOMATON_RESERVATION_FLOOR_BYTES);
    }
}
