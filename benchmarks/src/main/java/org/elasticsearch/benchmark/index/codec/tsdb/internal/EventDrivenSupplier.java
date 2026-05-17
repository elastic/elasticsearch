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
 * Generates strictly monotonic timestamps with irregular bursty intervals, modeling event driven
 * workloads such as logs, traces, and security events.
 *
 * <p>Each step draws either a short interval (the burst path, with probability {@code burstProbability})
 * or a long interval (the quiet gap path). The mixture produces clusters of close together
 * timestamps separated by occasional larger gaps, which is the typical shape of event ingest:
 * traffic spikes followed by quieter periods.
 *
 * <p>The pattern stresses both {@code delta} and {@code deltaOfDelta} differently from regular
 * scrape data: timestamps are still strictly monotonic so the gate passes, but the post {@code delta}
 * interval array has a wide value range, leaving more work for {@code offset} and {@code bitpack}
 * than constant interval inputs do.
 */
public class EventDrivenSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final long burstMin;
    private final long burstMax;
    private final long gapMin;
    private final long gapMax;
    private final double burstProbability;

    private EventDrivenSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.burstMin = builder.burstMin;
        this.burstMax = builder.burstMax;
        this.gapMin = builder.gapMin;
        this.gapMax = builder.gapMax;
        this.burstProbability = builder.burstProbability;
    }

    /**
     * Returns a new builder for this supplier.
     *
     * @param seed random seed
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
            final long interval;
            if (random.nextDouble() < burstProbability) {
                interval = burstMin + random.nextLong(burstMax - burstMin + 1);
            } else {
                interval = gapMin + random.nextLong(gapMax - gapMin + 1);
            }
            current += interval;
        }
        return data;
    }

    /** Builder for {@link EventDrivenSupplier}. */
    public static class Builder {
        private final int seed;
        private final int size;
        private long burstMin = 1L;
        private long burstMax = 100L;
        private long gapMin = 1000L;
        private long gapMax = 10000L;
        private double burstProbability = 0.8;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /**
         * Sets the range of short burst intervals.
         *
         * @param min lower bound (inclusive, must be positive)
         * @param max upper bound (inclusive, must be at least {@code min})
         * @return this builder
         */
        public Builder withBurstRange(long min, long max) {
            assert min >= 1 : "min must be positive";
            assert max >= min : "max must be at least min";
            this.burstMin = min;
            this.burstMax = max;
            return this;
        }

        /**
         * Sets the range of long gap intervals between bursts.
         *
         * @param min lower bound (inclusive, must be positive)
         * @param max upper bound (inclusive, must be at least {@code min})
         * @return this builder
         */
        public Builder withGapRange(long min, long max) {
            assert min >= 1 : "min must be positive";
            assert max >= min : "max must be at least min";
            this.gapMin = min;
            this.gapMax = max;
            return this;
        }

        /**
         * Sets the probability of drawing a short burst interval on each step. The complement is the
         * probability of drawing a long gap interval.
         *
         * @param burstProbability probability in [0.0, 1.0]
         * @return this builder
         */
        public Builder withBurstProbability(double burstProbability) {
            assert burstProbability >= 0.0 && burstProbability <= 1.0 : "burstProbability must be in [0.0, 1.0]";
            this.burstProbability = burstProbability;
            return this;
        }

        /**
         * Builds the supplier.
         *
         * @return the configured supplier
         */
        public EventDrivenSupplier build() {
            return new EventDrivenSupplier(this);
        }
    }
}
