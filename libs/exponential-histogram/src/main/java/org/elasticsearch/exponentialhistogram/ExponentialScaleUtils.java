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
 * Utils for working with indices and scales of exponential bucket histograms.
 */
public class ExponentialScaleUtils {

    private static final double LN_2 = Math.log(2);

    /**
     * Only visible for testing, the test ensures that this table is up-to-date.
     * <br>
     * For each scale from {@link ExponentialHistogram#MIN_SCALE} to {@link ExponentialHistogram#MAX_SCALE}
     * the table contains a pre-computed constant for performing up-scaling of bucket indices.
     * The constant is computed using the following formula:
     * <code>(1 + 2^scale * ( 1 - log2(1 + 2^(2^-scale))))</code>
     */
   static final double[] SCALE_UP_CONSTANT_TABLE = new double[]{
       4.8828125E-4,
       9.765625E-4,
       0.001953125,
       0.00390625,
       0.0078125,
       0.015625,
       0.03124999998950301,
       0.06249862414928998,
       0.12429693135076524,
       0.22813428968741514,
       0.33903595255631885,
       0.4150374992788438,
       0.45689339367277604,
       0.47836619809201575,
       0.4891729613112115,
       0.49458521106164327,
       0.497292446757125,
       0.4986462035295225,
       0.4993230992835585,
       0.4996615493316266,
       0.49983077462704417,
       0.49991538730867596,
       0.4999576936537322,
       0.4999788468267904,
       0.4999894234133857,
       0.4999947117066917,
       0.4999973558533457,
       0.49999867792667285,
       0.4999993389633364,
       0.4999996694816682,
       0.4999998347408341,
       0.49999991737041705,
       0.4999999586852085,
       0.49999997934260426,
       0.49999998967130216,
       0.49999999483565105,
       0.4999999974178255,
       0.49999999870891276,
       0.4999999993544564,
       0.4999999996772282,
       0.4999999998386141,
       0.49999999991930705,
       0.49999999995965355,
       0.49999999997982675,
       0.4999999999899134,
       0.4999999999949567,
       0.49999999999747835,
       0.4999999999987392,
       0.49999999999936956,
       0.4999999999996848
   };

    /**
     * Computes the new index for a bucket when adjusting the scale of the histogram by the given amount.
     * Note that this method does not only support down-scaling (=reducing the scale), but also upscaling.
     * When scaling up, it will provide the bucket containing the point of least error of the original bucket.
     *
     * @param index the current bucket index to be upscaled
     * @param currentScale the current scale
     * @param scaleAdjustment the adjustment to make, the new scale will be <code>currentScale + scaleAdjustment</code>
     * @return the index of the bucket in the new scale
     */
    static long adjustScale(long index, int currentScale, int scaleAdjustment) {
        if (scaleAdjustment <= 0) {
            return index >> -scaleAdjustment;
        } else {
            // When scaling up, we want to return the bucket containing the point of least relative error.
            // This bucket index can be computed as (index << adjustment) + offset
            // Hereby offset is a constant which does not depend on the index, but only on the scale and adjustment
            // The mathematically correct formula for offset is as follows:
            // 2^adjustment * (1 + 2^currentScale * ( 1 - log2(1 + 2^(2^-scale))))
            // This is hard to compute in double precision, as it causes rounding errors, also it is quite expensive
            // Therefore we precompute (1 + 2^currentScale * ( 1 - log2(1 + 2^(2^-scale)))) and store it
            // in SCALE_UP_CONSTANT_TABLE for each scale
            double offset = Math.scalb(SCALE_UP_CONSTANT_TABLE[currentScale - MIN_SCALE], scaleAdjustment);
            return (index << scaleAdjustment) + (long) Math.floor(offset);
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
            throw new IllegalArgumentException("index must be in range [" + MIN_INDEX + ".." + MAX_INDEX + "]");
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
        double inverseFactor = Math.scalb(LN_2, -scale);
        return Math.exp(inverseFactor * index);
    }

    /**
     * For a bucket with the given index, computes the point <code>x</code> in the bucket so that
     *  <code>(x - l) / l</code> equals <code>(u - x) / u</code> where <code>l</code> is the lower bucket boundary and where <code>u</code>
     *  is the upper bucket boundary.
     * <br>
     *  In other words we select the point in the bucket which is guaranteed to have the least relative error towards any point in the bucket.
     *
     * @param bucketIndex the bucket index
     * @param scale the scale of the histogram
     * @return the point of least relative error
     */
    public static double getPointOfLeastRelativeError(long bucketIndex, int scale) {
        double upperBound = getUpperBucketBoundary(bucketIndex, scale);
        double histogramBase = Math.pow(2, Math.scalb(1, -scale));
        return 2 / (histogramBase + 1) * upperBound;
    }

    /**
     * Provides the index of the bucket of the exponential histogram with the given scale
     * containing the provided value.
     */
    public static long computeIndex(double value, int scale) {
        return Indexing.computeIndex(value, scale);
    }

    /**
     * The code in this class has been copied and slightly adapted from the
     * <a href="https://github.com/open-telemetry/opentelemetry-java/blob/78a917da2e8f4bc3645f4fb10361e3e844aab9fb/sdk/metrics/src/main/java/io/opentelemetry/sdk/metrics/internal/aggregator/Base2ExponentialHistogramIndexer.java">OpenTelemetry Base2ExponentialHistogramIndexer implementation</a>
     *  licensed under Apache License 2.0.
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

}
