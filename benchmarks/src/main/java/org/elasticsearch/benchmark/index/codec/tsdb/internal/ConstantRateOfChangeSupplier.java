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
 * Generates strictly monotonic sequences where the interval between consecutive values grows
 * by a fixed amount at each step, so the second order difference is a constant.
 *
 * <p>Sequence shape: {@code data[i] = T_0 + i * d_0 + i * (i - 1) / 2 * dd}, equivalent to
 * walking from a starting value with an interval that begins at {@code firstDelta} and grows
 * by {@code rateOfChange} on every step.
 *
 * <p>Useful for benchmarks that need to exercise the {@code deltaOfDelta} apply path under a
 * clean, jitter free input. Unlike {@link TimestampLikeSupplier}, both passes of a
 * {@code delta>delta} cascade gate on here (the intervals themselves are also strictly
 * monotonic), so this is the right pattern to compare a single dedicated
 * {@code deltaOfDelta} stage against the full cost of two cascaded {@code delta} stages.
 */
public class ConstantRateOfChangeSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final long firstDelta;
    private final long rateOfChange;

    private ConstantRateOfChangeSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.firstDelta = builder.firstDelta;
        this.rateOfChange = builder.rateOfChange;
    }

    /**
     * Returns a new builder for this supplier.
     *
     * @param seed random seed for the starting offset
     * @param size number of values to generate
     * @return a new builder instance
     */
    public static Builder builder(int seed, int size) {
        return new Builder(seed, size);
    }

    @Override
    public long[] get() {
        final long[] data = new long[size];
        long current = random.nextLong(1_000_000L);
        long interval = firstDelta;
        for (int i = 0; i < size; i++) {
            data[i] = current;
            current += interval;
            interval += rateOfChange;
        }
        return data;
    }

    /** Builder for {@link ConstantRateOfChangeSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private long firstDelta = 1000L;
        private long rateOfChange = 10L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the first interval between consecutive values.
         *
         * @param firstDelta the starting interval value
         * @return this builder
         */
        public Builder withFirstDelta(long firstDelta) {
            this.firstDelta = firstDelta;
            return this;
        }

        /**
         * Sets the constant amount the interval grows by at each step. May be negative for
         * decelerating sequences.
         *
         * @param rateOfChange interval increment per step
         * @return this builder
         */
        public Builder withRateOfChange(long rateOfChange) {
            this.rateOfChange = rateOfChange;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public ConstantRateOfChangeSupplier build() {
            return new ConstantRateOfChangeSupplier(this);
        }
    }
}
