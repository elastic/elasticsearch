/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import java.util.Arrays;
import java.util.Random;
import java.util.function.Supplier;

/**
 * Generates values from a small set of distinct values with skewed frequency.
 *
 * <p>Simulates metrics like HTTP status codes or enum fields where only
 * a handful of distinct values appear with varying frequencies (Zipf distribution).
 */
public class LowCardinalitySupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final int distinctValues;
    private final double skew;
    private final long maxValue;

    private LowCardinalitySupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.distinctValues = builder.distinctValues;
        this.skew = builder.skew;
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
        long[] values = generateDistinctValues();
        double[] cumulativeWeights = computeZipfWeights();
        return sampleValues(values, cumulativeWeights);
    }

    private long[] generateDistinctValues() {
        long[] values = new long[distinctValues];
        long step = maxValue / distinctValues;
        for (int i = 0; i < distinctValues; i++) {
            long base = i * step;
            long jitter = step > 1 ? random.nextLong(step) : 0;
            values[i] = base + jitter;
        }
        return values;
    }

    private double[] computeZipfWeights() {
        double[] cumulativeWeights = new double[distinctValues];
        double totalWeight = 0;
        for (int i = 0; i < distinctValues; i++) {
            totalWeight += 1.0 / Math.pow(i + 1, skew);
            cumulativeWeights[i] = totalWeight;
        }
        return cumulativeWeights;
    }

    private long[] sampleValues(long[] values, double[] cumulativeWeights) {
        final long[] data = new long[size];
        double totalWeight = cumulativeWeights[cumulativeWeights.length - 1];
        for (int i = 0; i < size; i++) {
            double r = random.nextDouble() * totalWeight;
            int idx = Arrays.binarySearch(cumulativeWeights, r);
            if (idx < 0) {
                idx = -idx - 1;
            }
            data[i] = values[idx];
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

    /** Builder for {@link LowCardinalitySupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private int distinctValues = 10;
        private double skew = 2.0;
        private long maxValue = 10_000L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the number of distinct values.
         *
         * @param distinctValues number of distinct values (must be positive)
         * @return this builder
         */
        public Builder withDistinctValues(int distinctValues) {
            assert distinctValues >= 1 : "distinctValues must be positive";
            this.distinctValues = distinctValues;
            return this;
        }

        /**
         * Sets the Zipf skew parameter.
         *
         * @param skew the skew parameter (must be non-negative)
         * @return this builder
         */
        public Builder withSkew(double skew) {
            assert skew >= 0.0 : "skew must be non-negative";
            this.skew = skew;
            return this;
        }

        /**
         * Sets the maximum value range.
         *
         * @param maxValue the maximum value
         * @return this builder
         */
        public Builder withMaxValue(long maxValue) {
            this.maxValue = maxValue;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public LowCardinalitySupplier build() {
            assert maxValue >= distinctValues : "maxValue must be >= distinctValues";
            return new LowCardinalitySupplier(this);
        }
    }
}
