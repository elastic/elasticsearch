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

import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.nio.LongBuffer;
import java.util.Locale;

/**
 * Maintains histogram buckets that are constant width
 * in base-2 floating point representation space. This is close
 * to exponential binning, but should be much faster.
 */
public class FloatHistogram extends Histogram {
    private int bitsOfPrecision;
    private int shift;
    private int offset;

    @SuppressWarnings("WeakerAccess")
    public FloatHistogram(double min, double max) {
        this(min, max, 50);
    }

    @SuppressWarnings("WeakerAccess")
    public FloatHistogram(double min, double max, double binsPerDecade) {
        super(min, max);
        if (max <= 2 * min) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "Illegal/nonsensical min, max (%.2f, %.2g)", min, max));
        }
        if (min <= 0 || max <= 0) {
            throw new IllegalArgumentException("Min and max must be positive");
        }
        if (binsPerDecade < 5 || binsPerDecade > 10000) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unreasonable number of bins per decade %.2g. Expected value in range [5,10000]", binsPerDecade)
            );
        }

        // convert binsPerDecade into bins per octave, then figure out how many bits that takes
        bitsOfPrecision = (int) Math.ceil(Math.log(binsPerDecade * Math.log10(2)) / Math.log(2));
        // we keep just the required amount of the mantissa
        shift = 52 - bitsOfPrecision;
        // The exponent in a floating point number is offset
        offset = 0x3ff << bitsOfPrecision;

        setupBins(min, max);
    }

    @Override
    protected int bucketIndex(double x) {
        x = x / min;
        long floatBits = Double.doubleToLongBits(x);
        return (int) (floatBits >>> shift) - offset;
    }

    // exposed for testing
    @Override
    double lowerBound(int k) {
        return min * Double.longBitsToDouble((k + (0x3ffL << bitsOfPrecision)) << (52 - bitsOfPrecision)) /* / fuzz */;
    }

    @Override
    @SuppressWarnings("WeakerAccess")
    public long[] getCompressedCounts() {
        LongBuffer buf = LongBuffer.allocate(counts.length);
        Simple64.compress(buf, counts, 0, counts.length);
        long[] r = new long[buf.position()];
        buf.flip();
        buf.get(r);
        return r;
    }

    private void readObjectNoData() throws ObjectStreamException {
        throw new InvalidObjectException("Stream data required");
    }

    @Override
    void add(Iterable<Histogram> others) {
        for (Histogram other : others) {
            if (this.getClass().equals(other.getClass()) == false) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "Cannot add %s to FloatHistogram", others.getClass()));
            }
            FloatHistogram actual = (FloatHistogram) other;
            if (actual.min != min || actual.max != max || actual.counts.length != counts.length) {
                throw new IllegalArgumentException("Can only merge histograms with identical bounds and precision");
            }
            for (int i = 0; i < counts.length; i++) {
                counts[i] += other.counts[i];
            }
        }
    }
}
