/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.tsdb.internal;

import org.apache.lucene.util.NumericUtils;

import java.util.Random;
import java.util.function.Supplier;

/**
 * Generates ALP-friendly double values encoded as sortable-longs. Values are produced by
 * picking an integer mantissa around a configurable midpoint and dividing by
 * {@code 10^scale}, optionally replacing a configurable fraction of positions with
 * irrationals to exercise the ALP exception path.
 */
public final class AlpDecimalDoubleSupplier implements Supplier<long[]> {

    private final Random random;
    private final int size;
    private final int scale;
    private final long midpoint;
    private final long spread;
    private final double exceptionFraction;

    private AlpDecimalDoubleSupplier(Builder builder) {
        this.random = new Random(builder.seed);
        this.size = builder.size;
        this.scale = builder.scale;
        this.midpoint = builder.midpoint;
        this.spread = builder.spread;
        this.exceptionFraction = builder.exceptionFraction;
    }

    public static Builder builder(int seed, int size) {
        return new Builder(seed, size);
    }

    @Override
    public long[] get() {
        final long[] data = new long[size];
        final double divisor = Math.pow(10, scale);
        for (int i = 0; i < size; i++) {
            final double value;
            if (exceptionFraction > 0 && random.nextDouble() < exceptionFraction) {
                value = Math.sqrt(i + 2) * Math.PI;
            } else {
                final long mantissa = midpoint + (spread > 0 ? random.nextLong(2 * spread + 1) - spread : 0);
                value = mantissa / divisor;
            }
            data[i] = NumericUtils.doubleToSortableLong(value);
        }
        return data;
    }

    /** Nominal bit width for the generated sortable-longs at the configured midpoint. */
    public int getNominalBitsPerValue() {
        return Long.SIZE - Long.numberOfLeadingZeros(midpoint + spread + 1);
    }

    /** Builder for {@link AlpDecimalDoubleSupplier}. */
    public static final class Builder {
        private final int seed;
        private final int size;
        private int scale = 2;
        private long midpoint = 10_000L;
        private long spread = 5_000L;
        private double exceptionFraction = 0.0;

        private Builder(int seed, int size) {
            assert size >= 1 : "size must be positive";
            this.seed = seed;
            this.size = size;
        }

        /** Sets the decimal precision: values are {@code mantissa / 10^scale}. */
        public Builder withScale(int scale) {
            assert scale >= 0 && scale <= 18 : "scale must be in [0, 18]";
            this.scale = scale;
            return this;
        }

        /** Sets the integer mantissa midpoint around which values are sampled. */
        public Builder withMidpoint(long midpoint) {
            assert midpoint >= 0 : "midpoint must be non-negative";
            this.midpoint = midpoint;
            return this;
        }

        /** Half-width of the uniform mantissa spread around the midpoint. */
        public Builder withSpread(long spread) {
            assert spread >= 0 : "spread must be non-negative";
            this.spread = spread;
            return this;
        }

        /** Fraction of positions overwritten with irrational values (ALP exceptions). */
        public Builder withExceptionFraction(double exceptionFraction) {
            assert exceptionFraction >= 0.0 && exceptionFraction <= 1.0 : "exceptionFraction must be in [0, 1]";
            this.exceptionFraction = exceptionFraction;
            return this;
        }

        public AlpDecimalDoubleSupplier build() {
            return new AlpDecimalDoubleSupplier(this);
        }
    }
}
