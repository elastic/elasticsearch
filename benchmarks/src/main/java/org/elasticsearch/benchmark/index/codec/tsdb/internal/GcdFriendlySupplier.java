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
 * Generates values that share a common divisor (GCD).
 *
 * <p>All values are multiples of the specified divisor. Simulates metrics
 * in milliseconds or KB where values share a common factor.
 */
public class GcdFriendlySupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final long gcd;
    private final long maxMultiplier;

    private GcdFriendlySupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.gcd = builder.gcd;
        this.maxMultiplier = builder.maxMultiplier;
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
            long multiplier = random.nextLong(maxMultiplier) + 1;
            data[i] = multiplier * gcd;
        }
        return data;
    }

    /**
     * Returns the number of bits required to represent the value range.
     *
     * <p>Values are in [gcd, maxMultiplier*gcd], so range = (maxMultiplier-1)*gcd.
     *
     * @return nominal bits per value based on the value range
     */
    public int getNominalBitsPerValue() {
        return 64 - Long.numberOfLeadingZeros((maxMultiplier - 1) * gcd);
    }

    /** Builder for {@link GcdFriendlySupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private long gcd = 1000L;
        private long maxMultiplier = 10_000L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the GCD that all values will share.
         *
         * @param gcd the greatest common divisor (must be positive)
         * @return this builder
         */
        public Builder withGcd(long gcd) {
            assert gcd >= 1 : "gcd must be positive";
            this.gcd = gcd;
            return this;
        }

        /**
         * Sets the maximum multiplier for the GCD.
         *
         * @param maxMultiplier the maximum multiplier (must be positive)
         * @return this builder
         */
        public Builder withMaxMultiplier(long maxMultiplier) {
            assert maxMultiplier >= 1 : "maxMultiplier must be positive";
            this.maxMultiplier = maxMultiplier;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public GcdFriendlySupplier build() {
            return new GcdFriendlySupplier(this);
        }
    }
}
