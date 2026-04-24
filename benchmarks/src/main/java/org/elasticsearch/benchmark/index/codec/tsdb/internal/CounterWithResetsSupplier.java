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
 * Generates counter-like values that increase monotonically with occasional resets.
 *
 * <p>Simulates metrics like byte counters or packet counts that mostly increase
 * but occasionally reset to zero (e.g., on overflow or service restart).
 */
public class CounterWithResetsSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final double resetProbability;
    private final long maxValue;

    private CounterWithResetsSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.resetProbability = builder.resetProbability;
        this.maxValue = builder.maxValue;
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
        long maxIncrement = Math.max(1, maxValue / size);
        long current = 0;

        for (int i = 0; i < size; i++) {
            if (random.nextDouble() < resetProbability) {
                current = 0;
            } else {
                current += random.nextLong(maxIncrement) + 1;
                if (current >= maxValue) {
                    current = 0;
                }
            }
            data[i] = current;
        }
        return data;
    }

    /**
     * Returns the number of bits required to represent the maximum possible value.
     *
     * @return nominal bits per value based on configured maxValue
     */
    public int getNominalBitsPerValue() {
        return 64 - Long.numberOfLeadingZeros(maxValue - 1);
    }

    /** Builder for {@link CounterWithResetsSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private double resetProbability = 0.02;
        private long maxValue = 10_000_000L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the probability of a counter reset.
         *
         * @param resetProbability probability in [0.0, 1.0]
         * @return this builder
         */
        public Builder withResetProbability(double resetProbability) {
            assert resetProbability >= 0.0 && resetProbability <= 1.0 : "resetProbability must be in [0.0, 1.0]";
            this.resetProbability = resetProbability;
            return this;
        }

        /**
         * Sets the maximum counter value before overflow reset.
         *
         * @param maxValue the maximum value (must be positive)
         * @return this builder
         */
        public Builder withMaxValue(long maxValue) {
            assert maxValue >= 1 : "maxValue must be positive";
            this.maxValue = maxValue;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public CounterWithResetsSupplier build() {
            return new CounterWithResetsSupplier(this);
        }
    }
}
