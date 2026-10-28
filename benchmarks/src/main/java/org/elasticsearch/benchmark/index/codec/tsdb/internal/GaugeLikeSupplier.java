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
 * Generates gauge-like values that oscillate around a baseline using a bounded random walk.
 *
 * <p>Simulates metrics like CPU usage, temperature, or memory pressure
 * where values fluctuate within a bounded range around a mean.
 */
public class GaugeLikeSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final double varianceRatio;
    private final long maxValue;

    private GaugeLikeSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.varianceRatio = builder.varianceRatio;
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
        long baseline = maxValue / 2;
        long maxVariance = (long) (maxValue * varianceRatio);
        long current = baseline;

        for (int i = 0; i < size; i++) {
            long delta = maxVariance > 0 ? random.nextLong(2 * maxVariance + 1) - maxVariance : 0;
            current = Math.max(0, Math.min(maxValue - 1, current + delta));
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

    /** Builder for {@link GaugeLikeSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private double varianceRatio = 0.1;
        private long maxValue = 10_000L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the variance ratio controlling fluctuation intensity.
         *
         * @param varianceRatio ratio in [0.0, 1.0]
         * @return this builder
         */
        public Builder withVarianceRatio(double varianceRatio) {
            assert varianceRatio >= 0.0 && varianceRatio <= 1.0 : "varianceRatio must be in [0.0, 1.0]";
            this.varianceRatio = varianceRatio;
            return this;
        }

        /**
         * Sets the maximum value for the gauge.
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
        public GaugeLikeSupplier build() {
            return new GaugeLikeSupplier(this);
        }
    }
}
