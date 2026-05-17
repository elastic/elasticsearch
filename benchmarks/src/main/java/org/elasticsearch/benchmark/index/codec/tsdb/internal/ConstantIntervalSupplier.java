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
 * Generates strictly monotonic timestamps spaced by a perfectly constant interval, with no jitter.
 *
 * <p>Models the idealized common case for scrape based metric ingestion at the source: every value
 * is exactly {@code baseDelta} ticks apart from the previous one. After a single {@code delta}
 * stage the array becomes uniform; after {@code offset} it is all zeros and {@code bitpack} pays
 * zero bits. {@code deltaOfDelta} reaches the same zero array with one stage instead of two.
 *
 * <p>Companion to {@link TimestampLikeSupplier} (same shape with small random jitter) and
 * {@link ConstantRateOfChangeSupplier} (intervals that grow by a fixed amount per step).
 */
public class ConstantIntervalSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final long baseDelta;

    private ConstantIntervalSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.baseDelta = builder.baseDelta;
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
        for (int i = 0; i < size; i++) {
            data[i] = current;
            current += baseDelta;
        }
        return data;
    }

    /** Builder for {@link ConstantIntervalSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private long baseDelta = 1000L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the constant interval between consecutive timestamps.
         *
         * @param baseDelta the interval value (must be positive)
         * @return this builder
         */
        public Builder withBaseDelta(long baseDelta) {
            assert baseDelta >= 1 : "baseDelta must be positive";
            this.baseDelta = baseDelta;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public ConstantIntervalSupplier build() {
            return new ConstantIntervalSupplier(this);
        }
    }
}
