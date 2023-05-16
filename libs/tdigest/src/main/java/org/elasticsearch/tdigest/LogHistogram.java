/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import static java.lang.Math.sqrt;

/**
 * Non-linear histogram that uses floating point representation plus a quadratic correction to
 * bin width to achieve tighter fit to the ideal log2 sizing.
 */
public class LogHistogram extends Histogram {
    private double logFactor;
    private double logOffset;

    @SuppressWarnings("WeakerAccess")
    public LogHistogram(double min, double max) {
        this(min, max, 0.1);
    }

    @SuppressWarnings("WeakerAccess")
    public LogHistogram(double min, double max, double epsilonFactor) {
        super(min, max);
        logFactor = Math.log(2) / Math.log(1 + epsilonFactor);
        logOffset = LogHistogram.approxLog2(min) * logFactor;

        if (max <= 2 * min) {
            throw new IllegalArgumentException(String.format("Illegal/nonsensical min, max (%.2f, %.2g)", min, max));
        }
        if (min <= 0 || max <= 0) {
            throw new IllegalArgumentException("Min and max must be positive");
        }
        if (epsilonFactor < 1e-6 || epsilonFactor > 0.5) {
            throw new IllegalArgumentException(
                    String.format("Unreasonable number of bins per decade %.2g. Expected value in range [1e-6,0.5]",
                            epsilonFactor));
        }

        setupBins(min, max);
    }

    /**
     * Approximates log_2(value) by abusing floating point hardware. The floating point exponent
     * is used to get the integer part of the log. The mantissa is then adjusted with a second order
     * polynomial to get a better approximation. The error is bounded to be less than ±0.01 and is
     * zero at every power of two (which also implies the approximation is continuous).
     *
     * @param value The argument of the log
     * @return log_2(value) (within an error of about ± 0.01)
     */
    @SuppressWarnings("WeakerAccess")
    public static double approxLog2(double value) {
        final long valueBits = Double.doubleToRawLongBits(value);
        final long exponent = ((valueBits & 0x7ff0_0000_0000_0000L) >>> 52) - 1024;
        final double m = Double.longBitsToDouble((valueBits & 0x800fffffffffffffL) | 0x3ff0000000000000L);
        return (m * (2 - (1.0 / 3) * m) + exponent - (2.0 / 3.0));
    }

    /**
     * Computes an approximate value of 2^x. This is done as an exact inverse of #approxLog2 so
     * that bin boundaries can be computed exactly.
     *
     * @param x The power of 2 desired.
     * @return 2^x approximately.
     */
    @SuppressWarnings("WeakerAccess")
    public static double pow2(double x) {
        final double exponent = Math.floor(x) - 1;
        x = x - exponent;
        double m = 3 - sqrt(7 - 3 * x);
        return Math.pow(2, exponent + 1) * m;
    }

    @Override
    protected int bucketIndex(double x) {
        return (int) (LogHistogram.approxLog2(x) * logFactor - logOffset);
    }

    @Override
    double lowerBound(int k) {
        return LogHistogram.pow2((k + logOffset) / logFactor);
    }

    @Override
    long[] getCompressedCounts() {
        return new long[0];
    }

    @Override
    void writeObject(ObjectOutputStream out) throws IOException {

    }

    @Override
    void readObject(ObjectInputStream in) throws IOException {

    }

    @Override
    void add(Iterable<Histogram> others) {
        for (Histogram other : others) {
            if (!this.getClass().equals(other.getClass())) {
                throw new IllegalArgumentException(String.format("Cannot add %s to LogHistogram", others.getClass()));
            }
            LogHistogram actual = (LogHistogram) other;
            if (actual.min != min || actual.max != max || actual.counts.length != counts.length) {
                throw new IllegalArgumentException("Can only merge histograms with identical bounds and precision");
            }
            for (int i = 0; i < counts.length; i++) {
                counts[i] += other.counts[i];
            }
        }
    }
}
