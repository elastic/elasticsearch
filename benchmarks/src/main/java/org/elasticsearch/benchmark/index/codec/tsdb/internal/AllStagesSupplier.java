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
 * Generates data that triggers ALL stages of the ES87 encoding pipeline.
 *
 * <p>The ES87 codec applies stages in order: Delta -> Offset -> GCD -> FOR (bit-pack).
 * This supplier generates monotonically increasing values with carefully chosen
 * deltas that ensure every stage is applied:
 *
 * <ul>
 *   <li><b>Delta</b>: Values are strictly monotonically increasing</li>
 *   <li><b>Offset</b>: Deltas have min >= max/4 (small range relative to base)</li>
 *   <li><b>GCD</b>: After offset removal, all values share a common divisor > 1</li>
 *   <li><b>FOR</b>: Final values are bit-packed</li>
 * </ul>
 *
 * <p>Pattern: Deltas are [baseDelta, baseDelta+step, baseDelta+2*step, ...] where
 * step > 1 ensures GCD applies, and baseDelta is large enough for offset to apply.
 *
 * <h2>Example (using blockSize=8):</h2>
 * <pre>
 * Original values:    [5000000, 5000100, 5000202, 5000306, 5000412, 5000520, 5000630, 5000742]
 * Deltas:             [   100,     102,     104,     106,     108,     110,     112]
 *
 * After Delta stage:  [100, 100, 102, 104, 106, 108, 110, 112]  (min=100, max=112)
 * After Offset stage: [  0,   0,   2,   4,   6,   8,  10,  12]  (subtracted min=100)
 * After GCD stage:    [  0,   0,   1,   2,   3,   4,   5,   6]  (divided by GCD=2)
 * After FOR stage:    bit-packed with 3 bits per value
 *
 * Original size:      64 bytes (8 longs x 8 bytes)
 * Compressed size:    ~8 bytes (token + 3 bytes packed + metadata)
 * Compression ratio:  ~8x
 * </pre>
 */
public class AllStagesSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final long baseDelta;
    private final long step;

    private AllStagesSupplier(final Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.baseDelta = builder.baseDelta;
        this.step = builder.step;
    }

    /**
     * Returns a new builder for this supplier.
     *
     * @param seed random seed for reproducibility
     * @param size number of values to generate (block size)
     * @return a new builder instance
     */
    public static Builder builder(int seed, int size) {
        return new Builder(seed, size);
    }

    @Override
    public long[] get() {
        final long[] data = new long[size];

        // Start with a random large base value
        long current = random.nextLong(1_000_000L, 10_000_000L);
        data[0] = current;

        // Generate monotonically increasing values with deltas: baseDelta, baseDelta+step, ...
        for (int i = 1; i < size; i++) {
            long delta = baseDelta + (i - 1) * step;
            current += delta;
            data[i] = current;
        }

        return data;
    }

    /** Builder for {@link AllStagesSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private long baseDelta = 100L;
        private long step = 2L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
            // Validate default parameters work for this size
            validateParameters();
        }

        /**
         * Sets the base delta (minimum delta between consecutive values).
         *
         * <p>Must be large enough relative to max delta for offset to trigger.
         * For offset: baseDelta >= step * (size - 1) / 3
         *
         * @param baseDelta the base delta value (must be positive)
         * @return this builder
         */
        public Builder withBaseDelta(long baseDelta) {
            assert baseDelta >= 1 : "baseDelta must be positive";
            this.baseDelta = baseDelta;
            validateParameters();
            return this;
        }

        /**
         * Sets the step by which deltas increase.
         *
         * <p>Must be > 1 for GCD encoding to apply after offset removal.
         *
         * @param step the delta increment (must be >= 2)
         * @return this builder
         */
        public Builder withStep(long step) {
            assert step >= 2 : "step must be >= 2 for GCD to apply";
            this.step = step;
            validateParameters();
            return this;
        }

        private void validateParameters() {
            // For offset to trigger: baseDelta >= step * (size - 1) / 3
            long minBaseDelta = (step * (size - 1)) / 3 + 1;
            if (baseDelta < minBaseDelta) {
                throw new IllegalArgumentException(
                    "baseDelta (" + baseDelta + ") too small for size " + size + " and step " + step + ". " + "Minimum: " + minBaseDelta
                );
            }
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public AllStagesSupplier build() {
            return new AllStagesSupplier(this);
        }
    }
}
