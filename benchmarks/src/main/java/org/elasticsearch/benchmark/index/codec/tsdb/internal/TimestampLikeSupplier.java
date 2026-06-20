/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import java.util.Random;
import java.util.function.Supplier;

/**
 * Generates timestamp-like sequences with small, mostly-constant deltas.
 *
 * <p>Simulates real TSDB timestamps where consecutive values are typically
 * separated by a fixed interval (e.g., 1000ms) with occasional jitter.
 */
public class TimestampLikeSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final long baseDelta;
    private final double jitterRatio;
    private final double jitterProbability;

    private TimestampLikeSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.baseDelta = builder.baseDelta;
        this.jitterRatio = builder.jitterRatio;
        this.jitterProbability = builder.jitterProbability;
    }

    /**
     * Returns a new builder for this supplier.
     *
     * @param seed random seed for reproducibility
     * @param size number of values to generate
     * @return a new builder instance
     */
    public static Builder builder(int seed, int size) {
        return new Builder(seed, size);
    }

    @Override
    public long[] get() {
        final long[] data = new long[size];
        long maxJitter = (long) (baseDelta * jitterRatio);
        long current = random.nextLong(1_000_000L);

        for (int i = 0; i < size; i++) {
            data[i] = current;
            long jitter = 0;
            if (maxJitter > 0 && random.nextDouble() < jitterProbability) {
                jitter = random.nextLong(2 * maxJitter + 1) - maxJitter;
            }
            current += baseDelta + jitter;
        }
        return data;
    }

    /**
     * Returns the number of bits required to represent the value range.
     *
     * <p>Since the encoder removes the minimum offset, bits are based on range (max - min),
     * not absolute values. For timestamps, range = (size - 1) * (baseDelta + maxJitter).
     *
     * @return nominal bits per value based on the value range
     */
    public int getNominalBitsPerValue() {
        final long maxJitter = (long) (baseDelta * jitterRatio);
        final long range = (long) (size - 1) * (baseDelta + maxJitter);
        return 64 - Long.numberOfLeadingZeros(range);
    }

    /** Builder for {@link TimestampLikeSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private long baseDelta = 1000L;
        private double jitterRatio = 0.05;
        private double jitterProbability = 0.2;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the base delta between consecutive timestamps.
         *
         * @param baseDelta the base delta value (must be positive)
         * @return this builder
         */
        public Builder withBaseDelta(long baseDelta) {
            assert baseDelta >= 1 : "baseDelta must be positive";
            this.baseDelta = baseDelta;
            return this;
        }

        /**
         * Sets the jitter ratio as a fraction of baseDelta.
         *
         * @param jitterRatio the jitter ratio
         * @return this builder
         */
        public Builder withJitterRatio(double jitterRatio) {
            this.jitterRatio = jitterRatio;
            return this;
        }

        /**
         * Sets the probability of applying jitter to each delta.
         *
         * @param jitterProbability probability in [0.0, 1.0]
         * @return this builder
         */
        public Builder withJitterProbability(double jitterProbability) {
            assert jitterProbability >= 0.0 && jitterProbability <= 1.0 : "jitterProbability must be in [0.0, 1.0]";
            this.jitterProbability = jitterProbability;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public TimestampLikeSupplier build() {
            return new TimestampLikeSupplier(this);
        }
    }
}
