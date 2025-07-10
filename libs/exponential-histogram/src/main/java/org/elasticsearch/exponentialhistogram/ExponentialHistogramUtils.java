/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX_BITS;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;

public class ExponentialHistogramUtils {

    /** Bit mask used to isolate exponent of IEEE 754 double precision number. */
    private static final long EXPONENT_BIT_MASK = 0x7FF0000000000000L;

    /** Bit mask used to isolate the significand of IEEE 754 double precision number. */
    private static final long SIGNIFICAND_BIT_MASK = 0xFFFFFFFFFFFFFL;

    /** Bias used in representing the exponent of IEEE 754 double precision number. */
    private static final int EXPONENT_BIAS = 1023;

    /**
     * The number of bits used to represent the significand of IEEE 754 double precision number,
     * excluding the implicit bit.
     */
    private static final int SIGNIFICAND_WIDTH = 52;

    /** The number of bits used to represent the exponent of IEEE 754 double precision number. */
    private static final int EXPONENT_WIDTH = 11;

    private static final double LN_2 = Math.log(2);
    private static final double LOG_BASE2_E = 1D / LN_2;

    // Magic number, computed via log(4/3)/log(2^(2^-64)), but exact
    private static final long SCALE_UP_64_OFFSET = 7656090530189244512L;

    static long adjustScale(long index, int scaleAdjustment) {
        if (scaleAdjustment <= 0) {
            return index >> -scaleAdjustment;
        } else {
            // ((index << 64) + SCALE_UP_64_OFFSET)) >> (64-scaleAdjustment)
            // = index << scaleAdjustment + SCALE_UP_64_OFFSET >> (64-scaleAdjustment)
            return (index << scaleAdjustment) + (SCALE_UP_64_OFFSET >> (64 - scaleAdjustment));
        }
    }

    /**
     * Equivalent to mathematically correct comparison of the lower bucket boundaries of the given buckets
     */
    public static int compareLowerBoundaries(long idxA, int scaleA, long idxB, int scaleB) {
        if (scaleA > scaleB) {
            return -compareLowerBoundaries(idxB, scaleB, idxA, scaleA);
        }
        // scaleA <= scaleB
        int shifts = scaleB - scaleA;
        int maxScaleAdjustment = getMaximumScaleIncrease(idxA);
        if (maxScaleAdjustment < shifts) {
            // we would overflow if we adjust A to the scale of B
            // so if A is negative, scaling would produce a number less than Long.MIN_VALUE, therefore it is definitely smaller than B
            // if A is positive, scaling would produce a number bigger than Long.MAX_VALUE, therefore it is definitely bigger than B
            // if A is zero => shifting and therefore scale adjustment would not have any effect
            if (idxA == 0) {
                return Long.compare(0, idxB);
            } else {
                return idxA < 0 ? -1 : +1;
            }
        } else {
            long adjustedIdxA = idxA << shifts;
            return Long.compare(adjustedIdxA, idxB);
        }
    }

    /**
     * Returns the maximum permissible scale-increase which does not cause an overflow
     * of the index.
     */
    public static int getMaximumScaleIncrease(long index) {
        if (index < MIN_INDEX || index > MAX_INDEX) {
            throw new IllegalArgumentException("index must be in range ["+MIN_INDEX+".."+MAX_INDEX+"]");
        }
        if (index < 0) {
            index = ~index;
        }
        return Long.numberOfLeadingZeros(index) - (64 - MAX_INDEX_BITS);
    }

    public static double getUpperBucketBoundary(long index, int scale) {
        return getLowerBucketBoundary(index + 1, scale);
    }

    public static double getLowerBucketBoundary(long index, int scale) {
        // TODO: handle numeric limits, implement by splitting the index into two 32 bit integers
        double inverseFactor = Math.scalb(LN_2, -scale);
        return Math.exp(inverseFactor * index);
    }

    public static double getPointOfLeastRelativeError(long bucketIndex, int scale) {
        double inverseFactor = Math.scalb(LN_2, -scale);
        return Math.exp(inverseFactor * (bucketIndex + 1/3.0));
    }

    static long computeIndex(double value, int scale) {
        double absValue = Math.abs(value);
        // For positive scales, compute the index by logarithm, which is simpler but may be
        // inaccurate near bucket boundaries
        if (scale > 0) {
            return getIndexByLogarithm(absValue, scale);
        }
        // For scale zero, compute the exact index by extracting the exponent
        if (scale == 0) {
            return mapToIndexScaleZero(absValue);
        }
        // For negative scales, compute the exact index by extracting the exponent and shifting it to
        // the right by -scale
        return mapToIndexScaleZero(absValue) >> -scale;
    }

    /**
     * Compute the bucket index using a logarithm based approach.
     *
     * @see <a
     *     href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#all-scales-use-the-logarithm-function">All
     *     Scales: Use the Logarithm Function</a>
     */
    private static long getIndexByLogarithm(double value, int scale) {
        double scaleFactor = Math.scalb(LOG_BASE2_E, scale);
        return (long) Math.ceil(Math.scalb(Math.log(value) * LOG_BASE2_E, scale)) - 1;
    }

    /**
     * Compute the exact bucket index for scale zero by extracting the exponent.
     *
     * @see <a
     *     href="https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/data-model.md#scale-zero-extract-the-exponent">Scale
     *     Zero: Extract the Exponent</a>
     */
    private static long mapToIndexScaleZero(double value) {
        long rawBits = Double.doubleToLongBits(value);
        long rawExponent = (rawBits & EXPONENT_BIT_MASK) >> SIGNIFICAND_WIDTH;
        long rawSignificand = rawBits & SIGNIFICAND_BIT_MASK;
        if (rawExponent == 0) {
            rawExponent -= Long.numberOfLeadingZeros(rawSignificand - 1) - EXPONENT_WIDTH - 1;
        }
        int ieeeExponent = (int) (rawExponent - EXPONENT_BIAS);
        if (rawSignificand == 0) {
            return ieeeExponent - 1;
        }
        return ieeeExponent;
    }

}
