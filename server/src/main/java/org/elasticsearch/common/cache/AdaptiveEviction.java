/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.cache;

/**
 * Configuration of the adaptive eviction mode of a {@link Cache}.
 * <p>
 * In this mode the cache does not decide evictions with a single policy. It simulates two policies as shadow caches
 * (LRU and LFU) on the same request stream, scores each of them by its hit/miss record with an exponential-weights (Hedge)
 * rule with fixed share, and lazily follows the policy that is currently trusted: entries the followed policy does not hold
 * are evicted first, and a new entry the followed policy would not admit is the first to go. When the traffic changes
 * character the trust moves to the other policy. Shadow caches keep only 32-bit hash codes of keys, never the keys or values.
 * <p>
 * The cache must have a maximum weight. All adaptive state is guarded by the cache's LRU lock.
 * <p>
 * Evaluated and not adopted (synthetic traffic, capacity 500 and 2000, 600k requests; see {@code AdaptiveCacheTrafficTests}). Their
 * code was removed so that it does not linger unused:
 * <ul>
 * <li>A popularity-prior shadow policy and a cost-aware (greedy-dual-size-frequency) shadow policy. The request cache has no
 * popularity or miss-cost source, since shard-level keys cannot be mapped back to a query fingerprint, and in earlier experiments
 * popularity weights added at most 0.015 hit ratio once the frequency sketch was well sized.</li>
 * <li>A fixed sketch width of 8192 counters. It is better only on abrupt re-ranking (+0.026) and 0.01 to 0.04 worse than 65536 on
 * every other shape at capacity 2000, so the sketch has to scale with the capacity.</li>
 * <li>Skipping the recency refresh of hot keys (level 4 and above) in the LFU shadow cache. It cost up to 0.001 hit ratio and saved
 * no measurable time (within 1% of 172 ns/op).</li>
 * <li>Updating the shadow caches for only 1 in 4 (or 1 in 8) keys, chosen by hash, with shadow caches a quarter the size. It cut the
 * overhead from about 47 to 30 ns/op, but most of the gain over LRU was lost: stationary zipf at capacity 2000 fell from 0.581 to
 * 0.486 (LRU 0.471) and zipf plus scans from 0.373 to 0.274 (LRU 0.262).</li>
 * <li>A third shadow policy, an undecayed LFU next to a decayed one (the LeCaR-inspired "pure LFU candidate"). With a 100k-access
 * window plus an undecayed sketch it matched the best of the two windows on every shape (abrupt re-rank at capacity 2000: 0.531
 * against 0.493 for the current single long window), but it costs about 19 ns/op (+12%) and another sketch, and only 0.003 to
 * 0.005 more than simply shortening the single window to 100k, which is free and is now the default (see
 * {@link #DEFAULT_SKETCH_SAMPLE}).</li>
 * <li>Age-discounted regret losses (LeCaR style: a shadow policy is penalised only when it misses a key it evicted recently,
 * by d^(age/capacity)) instead of the plain miss indicator, with d from 0.5 to 0.005 and eta 0.2 or 1.0. Within 0.001 of plain
 * Hedge everywhere except abrupt re-rank (+0.005). Updating the weights only on such regrettable misses is the same experiment.</li>
 * </ul>
 * <p>
 * Evaluated and kept as is: caches of 0.1% to 1% of the popular keys (capacity 50 to 500). The adaptive cache is never below LRU
 * there and roughly doubles its hit ratio on skewed and scan traffic (capacity 50, stationary zipf 0.263 against 0.129).
 * <p>
 * Not evaluated: probabilistic choice between the LRU and LFU victim at each eviction (it needs an outside list per shadow policy
 * instead of one) and hoarding-rate telemetry.
 *
 * @param <K> the key type
 */
public final class AdaptiveEviction<K> {

    public static final double DEFAULT_ETA = 0.2;
    public static final double DEFAULT_ALPHA = 1e-5;
    public static final int DEFAULT_SKETCH_WIDTH = 1 << 16;
    /**
     * How many counted accesses the LFU shadow cache remembers by default (capped at ten times the sketch width for small sketches).
     * A window of 100k instead of the ten sketch-widths of the first version (655k) recovers from a change of the popularity ranking
     * much faster (abrupt re-rank, capacity 2000: 0.531 against 0.493) and costs at most 0.005 on stationary traffic.
     */
    public static final long DEFAULT_SKETCH_SAMPLE = 100_000;

    private final double eta;
    private final double alpha;
    private final long seed;
    private final int sketchWidth;
    private final long sketchSample;

    private AdaptiveEviction(double eta, double alpha, long seed, int sketchWidth, long sketchSample) {
        this.eta = eta;
        this.alpha = alpha;
        this.seed = seed;
        this.sketchWidth = sketchWidth;
        this.sketchSample = sketchSample > 0 ? sketchSample : Math.min(DEFAULT_SKETCH_SAMPLE, 10L * sketchWidth);
    }

    public static <K> Builder<K> builder() {
        return new Builder<>();
    }

    double eta() {
        return eta;
    }

    double alpha() {
        return alpha;
    }

    long seed() {
        return seed;
    }

    int sketchWidth() {
        return sketchWidth;
    }

    long sketchSample() {
        return sketchSample;
    }

    public static final class Builder<K> {
        private double eta = DEFAULT_ETA;
        private double alpha = DEFAULT_ALPHA;
        private long seed = 0;
        private int sketchWidth = DEFAULT_SKETCH_WIDTH;
        private long sketchSample = 0;

        private Builder() {}

        /** Hedge learning rate applied to the per-request miss indicator of each shadow policy. */
        public Builder<K> eta(double eta) {
            if (eta <= 0) {
                throw new IllegalArgumentException("eta <= 0");
            }
            this.eta = eta;
            return this;
        }

        /** Fixed share: the fraction of weight redistributed uniformly after every request, so that trust can move back. */
        public Builder<K> alpha(double alpha) {
            if (alpha < 0 || alpha >= 1) {
                throw new IllegalArgumentException("alpha must be in [0, 1)");
            }
            this.alpha = alpha;
            return this;
        }

        public Builder<K> seed(long seed) {
            this.seed = seed;
            return this;
        }

        /** Counters per row of the frequency sketch used by the LFU shadow policy; a power of two, at least 16. */
        public Builder<K> sketchWidth(int sketchWidth) {
            if (sketchWidth < 16 || Integer.bitCount(sketchWidth) != 1) {
                throw new IllegalArgumentException("sketchWidth must be a power of two >= 16");
            }
            this.sketchWidth = sketchWidth;
            return this;
        }

        /**
         * How many counted accesses pass before the frequency sketch halves all its counters, that is how far back the LFU shadow cache
         * remembers; by default {@link #DEFAULT_SKETCH_SAMPLE}, or ten times the sketch width if that is less. A short memory favours
         * recent popularity, a long one stable popularity.
         */
        public Builder<K> sketchSample(long sketchSample) {
            if (sketchSample < 1) {
                throw new IllegalArgumentException("sketchSample must be positive");
            }
            this.sketchSample = sketchSample;
            return this;
        }

        public AdaptiveEviction<K> build() {
            return new AdaptiveEviction<>(eta, alpha, seed, sketchWidth, sketchSample);
        }
    }

    /**
     * A snapshot of what the adaptive controller did.
     *
     * @param experts        names of the shadow policies
     * @param followShare    fraction of accesses during which each policy was the followed one
     * @param expertHitRatio hit ratio of each shadow policy on the same accesses
     * @param switches       how many times the followed policy changed
     * @param accesses       the number of accesses (hits and insertions) seen
     */
    public record Stats(String[] experts, double[] followShare, double[] expertHitRatio, long switches, long accesses) {}
}
