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
 * Generates TSDB-flavored timestamp blocks shaped like {@code @timestamp} doc values
 * in indices sorted by {@code [_tsid asc, @timestamp desc]}.
 *
 * <p>The block is composed of {@code flips + 1} descending sub runs separated by
 * upward boundary jumps. Each sub run decreases at a roughly fixed sampling interval
 * (default 10 sec) with optional ms-scale jitter; the upward jump between sub runs
 * simulates a {@code _tsid} transition (default 240 minutes).
 *
 * <p>This is the input shape that {@code SplitDelta} targets: at {@code flips=0} the
 * block is a strict descending run that {@code SplitDelta} declines (yielding a
 * baseline-equivalent encoding); at {@code flips>=1} the block crosses one or more
 * boundaries that the baseline {@code delta} stage rejects.
 */
public class BoundaryBlockSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final int flips;
    private final long baseTimestamp;
    private final long intervalMs;
    private final long boundaryJumpMs;
    private final long jitterMs;

    private BoundaryBlockSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.flips = builder.flips;
        this.baseTimestamp = builder.baseTimestamp;
        this.intervalMs = builder.intervalMs;
        this.boundaryJumpMs = builder.boundaryJumpMs;
        this.jitterMs = builder.jitterMs;
    }

    /**
     * Returns a new builder for this supplier.
     *
     * @param seed random seed for reproducibility
     * @param size number of values to generate (the block size)
     * @return a new builder instance
     */
    public static Builder builder(int seed, int size) {
        return new Builder(seed, size);
    }

    /**
     * Returns the bit width required to bit-pack the value range of this block.
     *
     * <p>The encoder removes the per-block minimum offset, so the bound is set by
     * {@code max - min}. For this supplier the range is dominated by the boundary
     * jumps plus the span of one sub run.
     *
     * @return nominal bits per value at the bit-pack stage with no transforms
     */
    public int getNominalBitsPerValue() {
        final int subRunCount = flips + 1;
        final int baseLen = size / subRunCount;
        final long range = (long) flips * boundaryJumpMs + (long) Math.max(0, baseLen - 1) * intervalMs + 2 * jitterMs;
        return range > 0 ? 64 - Long.numberOfLeadingZeros(range) : 1;
    }

    @Override
    public long[] get() {
        final long[] data = new long[size];
        final int subRunCount = flips + 1;
        final int baseLen = size / subRunCount;
        final int remainder = size % subRunCount;
        int pos = 0;
        for (int s = 0; s < subRunCount; s++) {
            final int subLen = baseLen + (s < remainder ? 1 : 0);
            final long subBase = baseTimestamp + (long) s * boundaryJumpMs;
            for (int i = 0; i < subLen; i++) {
                final long jitter = jitterMs > 0 ? random.nextLong(2 * jitterMs + 1) - jitterMs : 0L;
                data[pos++] = subBase - (long) i * intervalMs + jitter;
            }
        }
        return data;
    }

    /** Builder for {@link BoundaryBlockSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private int flips = 1;
        private long baseTimestamp = 1_700_000_000_000L;
        private long intervalMs = 10_000L;
        private long boundaryJumpMs = 240L * 60L * 1000L;
        private long jitterMs = 0L;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the number of direction flips in the block. {@code flips=0} produces a
         * strict descending run; {@code flips=k} produces {@code k+1} descending sub runs
         * separated by upward boundary jumps.
         *
         * @param flips the number of upward boundary jumps in the block
         * @return this builder
         */
        public Builder withFlips(int flips) {
            assert flips >= 0 : "flips must be non-negative";
            this.flips = flips;
            return this;
        }

        /**
         * Sets the descending sampling interval (ms) within each sub run.
         *
         * @param intervalMs interval between consecutive timestamps (must be positive)
         * @return this builder
         */
        public Builder withIntervalMs(long intervalMs) {
            assert intervalMs >= 1 : "intervalMs must be positive";
            this.intervalMs = intervalMs;
            return this;
        }

        /**
         * Sets the upward boundary jump (ms) between consecutive sub runs.
         *
         * @param boundaryJumpMs the jump magnitude (must be positive)
         * @return this builder
         */
        public Builder withBoundaryJumpMs(long boundaryJumpMs) {
            assert boundaryJumpMs >= 1 : "boundaryJumpMs must be positive";
            this.boundaryJumpMs = boundaryJumpMs;
            return this;
        }

        /**
         * Sets the maximum per-sample jitter (ms). {@code 0} produces deterministic
         * intervals; otherwise jitter is uniform in {@code [-jitterMs, +jitterMs]}.
         *
         * <p>Jitter must be strictly less than {@code intervalMs / 2} so all inter-sample
         * diffs within a sub run remain negative, preserving the descending direction
         * that {@code SplitDelta} expects.
         *
         * @param jitterMs the maximum absolute jitter
         * @return this builder
         */
        public Builder withJitterMs(long jitterMs) {
            assert jitterMs >= 0 : "jitterMs must be non-negative";
            this.jitterMs = jitterMs;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public BoundaryBlockSupplier build() {
            assert jitterMs < intervalMs / 2 : "jitterMs must be strictly less than intervalMs / 2";
            return new BoundaryBlockSupplier(this);
        }
    }
}
