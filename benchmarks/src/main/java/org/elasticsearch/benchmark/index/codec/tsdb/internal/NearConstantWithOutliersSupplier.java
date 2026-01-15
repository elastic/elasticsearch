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
 * Generates mostly-constant values with occasional outliers.
 *
 * <p>Simulates metrics like error counts or idle CPU where values are
 * typically zero or constant, with occasional spikes.
 */
public class NearConstantWithOutliersSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final double outlierProbability;
    private final long baseValue;
    private final long maxOutlier;

    private NearConstantWithOutliersSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.outlierProbability = builder.outlierProbability;
        this.baseValue = builder.baseValue;
        this.maxOutlier = builder.maxOutlier;
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
        for (int i = 0; i < size; i++) {
            if (random.nextDouble() < outlierProbability) {
                data[i] = random.nextLong(maxOutlier);
            } else {
                data[i] = baseValue;
            }
        }
        return data;
    }

    /**
     * Returns the number of bits required to represent the maximum possible value.
     *
     * @return nominal bits per value based on max of baseValue and maxOutlier
     */
    public int getNominalBitsPerValue() {
        long maxPossibleValue = Math.max(baseValue, maxOutlier - 1);
        return 64 - Long.numberOfLeadingZeros(maxPossibleValue);
    }

    /** Builder for {@link NearConstantWithOutliersSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private double outlierProbability = 0.05;
        private long baseValue = 0L;
        private long maxOutlier = 10_000_000L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the probability of generating an outlier.
         *
         * @param outlierProbability probability in [0.0, 1.0]
         * @return this builder
         */
        public Builder withOutlierProbability(double outlierProbability) {
            assert outlierProbability >= 0.0 && outlierProbability <= 1.0 : "outlierProbability must be in [0.0, 1.0]";
            this.outlierProbability = outlierProbability;
            return this;
        }

        /**
         * Sets the constant base value.
         *
         * @param baseValue the base value (must be non-negative)
         * @return this builder
         */
        public Builder withBaseValue(long baseValue) {
            assert baseValue >= 0 : "baseValue must be non-negative";
            this.baseValue = baseValue;
            return this;
        }

        /**
         * Sets the maximum outlier value.
         *
         * @param maxOutlier the maximum outlier value (must be positive)
         * @return this builder
         */
        public Builder withMaxOutlier(long maxOutlier) {
            assert maxOutlier >= 1 : "maxOutlier must be positive";
            this.maxOutlier = maxOutlier;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public NearConstantWithOutliersSupplier build() {
            return new NearConstantWithOutliersSupplier(this);
        }
    }
}
