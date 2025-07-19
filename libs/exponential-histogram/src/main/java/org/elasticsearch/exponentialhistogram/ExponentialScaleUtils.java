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
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;

/**
 * A collection of utility methods for working with indices and scales of exponential bucket histograms.
 */
public class ExponentialScaleUtils {

    private static final double LN_2 = Math.log(2);

    /**
     * This table is visible for testing to ensure it is up-to-date.
     * <br>
     * For each scale from {@link ExponentialHistogram#MIN_SCALE} to {@link ExponentialHistogram#MAX_SCALE},
     * the table contains a pre-computed constant for up-scaling bucket indices.
     * The constant is computed using the following formula:
     * {@code 2^63 * (1 + 2^scale * (1 - log2(1 + 2^(2^-scale))))}
     */
    static final long[] SCALE_UP_CONSTANT_TABLE = new long[] {
        4503599627370495L,
        9007199254740991L,
        18014398509481983L,
        36028797018963967L,
        72057594037927935L,
        144115188075855871L,
        288230376054894118L,
        576448062320457790L,
        1146436840887505800L,
        2104167428150631728L,
        3127054724296373505L,
        3828045265094622256L,
        4214097751025163417L,
        4412149414858430624L,
        4511824212543271281L,
        4561743405547877994L,
        4586713247558758689L,
        4599199449917992829L,
        4605442711287634239L,
        4608564361996858084L,
        4610125189854540715L,
        4610905604096266504L,
        4611295811256239977L,
        4611490914841115537L,
        4611588466634164420L,
        4611637242530765249L,
        4611661630479075212L,
        4611673824453231387L,
        4611679921440309624L,
        4611682969933848761L,
        4611684494180618332L,
        4611685256304003118L,
        4611685637365695511L,
        4611685827896541707L,
        4611685923161964805L,
        4611685970794676354L,
        4611685994611032129L,
        4611686006519210016L,
        4611686012473298960L,
        4611686015450343432L,
        4611686016938865668L,
        4611686017683126786L,
        4611686018055257345L,
        4611686018241322624L,
        4611686018334355264L,
        4611686018380871584L,
        4611686018404129744L,
        4611686018415758824L,
        4611686018421573364L,
        4611686018424480634L };

    /**
     * Computes the new index for a bucket when adjusting the scale of the histogram.
     * This method supports both down-scaling (reducing the scale) and up-scaling.
     * When up-scaling, it returns the bucket containing the point of least error of the original bucket.
     *
     * @param index           the current bucket index to be adjusted
     * @param currentScale    the current scale
     * @param scaleAdjustment the adjustment to make; the new scale will be {@code currentScale + scaleAdjustment}
     * @return the index of the bucket in the new scale
     */
    static long adjustScale(long index, int currentScale, int scaleAdjustment) {
        checkIndexAndScaleBounds(index, currentScale);

        int newScale = currentScale + scaleAdjustment;
        if (newScale < MIN_SCALE || newScale > MAX_SCALE) {
            throw new IllegalArgumentException("adjusted scale must be in the range [" + MIN_SCALE + "..." + MAX_SCALE + "]");
        }

        if (scaleAdjustment <= 0) {
            return index >> -scaleAdjustment;
        } else {
            if (scaleAdjustment > MAX_INDEX_BITS) {
                throw new IllegalArgumentException("Scaling up more than " + MAX_INDEX_BITS + " does not make sense");
            }
            // When scaling up, we want to return the bucket containing the point of least relative error.
            // This bucket index can be computed as (index << adjustment) + offset.
            // The offset is a constant that depends only on the scale and adjustment, not the index.
            // The mathematically correct formula for the offset is:
            // 2^adjustment * (1 + 2^currentScale * (1 - log2(1 + 2^(2^-currentScale))))
            // This is hard to compute with double-precision floating-point numbers due to rounding errors and is also expensive.
            // Therefore, we precompute 2^63 * (1 + 2^currentScale * (1 - log2(1 + 2^(2^-currentScale)))) and store it
            // in SCALE_UP_CONSTANT_TABLE for each scale.
            // This can then be converted to the correct offset by dividing with (2^(63-adjustment)),
            // which is equivalent to a right shift with (63-adjustment)
            long offset = SCALE_UP_CONSTANT_TABLE[currentScale - MIN_SCALE] >> (63 - scaleAdjustment);
            return (index << scaleAdjustment) + offset;
        }
    }

    /**
     * Compares the lower boundaries of two buckets, which may have different scales.
     * This is equivalent to a mathematically correct comparison of the lower bucket boundaries.
     * Note that this method allows for scales and indices of the full numeric range of the types.
     *
     * @param idxA           the index of the first bucket
     * @param scaleA         the scale of the first bucket
     * @param idxB           the index of the second bucket
     * @param scaleB         the scale of the second bucket
     * @return a negative integer, zero, or a positive integer as the first bucket's lower boundary is
     *         less than, equal to, or greater than the second bucket's lower boundary
     */
    public static int compareExponentiallyScaledValues(long idxA, int scaleA, long idxB, int scaleB) {
        if (scaleA > scaleB) {
            return -compareExponentiallyScaledValues(idxB, scaleB, idxA, scaleA);
        }
        // scaleA <= scaleB
        int shifts = scaleB - scaleA;
        int maxScaleAdjustment = getMaximumScaleIncreaseIgnoringIndexLimits(idxA);
        if (maxScaleAdjustment < shifts) {
            // We would overflow if we adjusted A to the scale of B.
            // If A is negative, scaling would produce a number less than Long.MIN_VALUE, so it is smaller than B.
            // If A is positive, scaling would produce a number greater than Long.MAX_VALUE, so it is larger than B.
            // If A is zero, shifting and scale adjustment have no effect.
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
     * Returns the maximum permissible scale increase that does not cause the index to grow out
     * of the [{@link ExponentialHistogram#MIN_INDEX}, {@link ExponentialHistogram#MIN_INDEX}] range.
     *
     * @param index the index to check
     * @return the maximum permissible scale increase
     */
    public static int getMaximumScaleIncrease(long index) {
        checkIndexBounds(index);
        return getMaximumScaleIncreaseIgnoringIndexLimits(index);
    }

    private static int getMaximumScaleIncreaseIgnoringIndexLimits(long index) {
        if (index < 0) {
            index = ~index;
        }
        return Long.numberOfLeadingZeros(index) - (64 - MAX_INDEX_BITS);
    }

    /**
     * Returns the upper boundary of the bucket with the given index and scale.
     *
     * @param index the index of the bucket
     * @param scale the scale of the bucket
     * @return the upper boundary of the bucket
     */
    public static double getUpperBucketBoundary(long index, int scale) {
        checkIndexAndScaleBounds(index, scale);
        return exponentiallyScaledToDoubleValue(index + 1, scale);
    }

    /**
     * Returns the lower boundary of the bucket with the given index and scale.
     *
     * @param index the index of the bucket in the [{@link ExponentialHistogram#MIN_INDEX}, {@link ExponentialHistogram#MAX_INDEX}] range.
     * @param scale the scale of the bucket
     * @return the lower boundary of the bucket
     */
    public static double getLowerBucketBoundary(long index, int scale) {
        checkIndexAndScaleBounds(index, scale);
        return exponentiallyScaledToDoubleValue(index, scale);
    }

    /**
     * Computes (2^2^(-scale))^index,
     * allowing also indices outside of the [{@link ExponentialHistogram#MIN_INDEX}, {@link ExponentialHistogram#MAX_INDEX}] range.
     */
    static double exponentiallyScaledToDoubleValue(long index, int scale) {
        double inverseFactor = Math.scalb(LN_2, -scale);
        return Math.exp(inverseFactor * index);
    }

    /**
     * For a bucket with the given index, computes the point {@code x} in the bucket such that
     * {@code (x - l) / l} equals {@code (u - x) / u}, where {@code l} is the lower bucket boundary and {@code u}
     * is the upper bucket boundary.
     * <br>
     * In other words, we select the point in the bucket that has the least relative error with respect to any other point in the bucket.
     *
     * @param bucketIndex the index of the bucket
     * @param scale       the scale of the bucket
     * @return the point of least relative error
     */
    public static double getPointOfLeastRelativeError(long bucketIndex, int scale) {
        checkIndexAndScaleBounds(bucketIndex, scale);
        double upperBound = getUpperBucketBoundary(bucketIndex, scale);
        double histogramBase = Math.pow(2, Math.scalb(1, -scale));
        return 2 / (histogramBase + 1) * upperBound;
    }

    /**
     * Provides the index of the bucket of the exponential histogram with the given scale that contains the provided value.
     *
     * @param value the value to find the bucket for
     * @param scale the scale of the histogram
     * @return the index of the bucket
     */
    public static long computeIndex(double value, int scale) {
        checkScaleBounds(scale);
        return Indexing.computeIndex(value, scale);
    }

    private static void checkIndexAndScaleBounds(long index, int scale) {
        checkIndexBounds(index);
        checkScaleBounds(scale);
    }

    private static void checkScaleBounds(int scale) {
        if (scale < MIN_SCALE || scale > MAX_SCALE) {
            throw new IllegalArgumentException("scale must be in range [" + MIN_SCALE + ".." + MAX_SCALE + "]");
        }
    }

    private static void checkIndexBounds(long index) {
        if (index < MIN_INDEX || index > MAX_INDEX) {
            throw new IllegalArgumentException("index must be in range [" + MIN_INDEX + ".." + MAX_INDEX + "]");
        }
    }

    /**
     * The code in this class was copied and slightly adapted from the
     * <a href="https://github.com/open-telemetry/opentelemetry-java/blob/78a917da2e8f4bc3645f4fb10361e3e844aab9fb/sdk/metrics/src/main/java/io/opentelemetry/sdk/metrics/internal/aggregator/Base2ExponentialHistogramIndexer.java">OpenTelemetry Base2ExponentialHistogramIndexer implementation</a>,
     * licensed under the Apache License 2.0.
     */
    private static class Indexing {

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

        private static final double LOG_BASE2_E = 1D / LN_2;

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
}
