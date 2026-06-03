/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;

/**
 * A collection of utility methods for working with indices and scales of exponential bucket histograms.
 */
public class ExponentialScaleUtils {

    private static final double LN_2 = Math.log(2);

    /**
     * Computes the new index for a bucket when reducing the scale of the histogram.
     *
     * @param index           the current bucket index to be adjusted
     * @param scaleReduction  the amount by which the scale is reduced (must be non-negative)
     */
    static long reduceScale(long index, int scaleReduction) {
        assert scaleReduction >= 0;
        return index >> scaleReduction;
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

        long scaledDownB = idxB >> shifts;
        int result = Long.compare(idxA, scaledDownB);
        if (result == 0) {
            // the scaled down values are equal
            // this means that b is bigger if it has a "fractional" part, which corresponds to the bits that were removed on the right-shift
            assert (1L << shifts) > 0;
            long shiftedAway = idxB & ((1L << shifts) - 1);
            if (shiftedAway > 0) {
                return -1;
            } else {
                return 0;
            }
        }
        return result;
    }

    /**
     * Returns a scale at to which the given index can be scaled down without changing the exponentially scaled number it represents.
     * @param index the index of the number
     * @param scale the current scale of the number
     * @return the new scale
     */
    static int normalizeScale(long index, int scale) {
        return Math.max(MIN_SCALE, scale - Long.numberOfTrailingZeros(index));
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
     * Computes (2^(2^-scale))^index,
     * allowing also indices outside of the [{@link ExponentialHistogram#MIN_INDEX}, {@link ExponentialHistogram#MAX_INDEX}] range.
     */
    static double exponentiallyScaledToDoubleValue(long index, int scale) {
        // Math.exp is expected to be faster and more accurate than Math.pow
        // For that reason we use (2^(2^-scale))^index = 2^( (2^-scale) * index) = (e^ln(2))^( (2^-scale) * index)
        // = e^( ln(2) * (2^-scale) * index)
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
        double histogramBase = Math.pow(2, Math.scalb(1, -scale));
        if (Double.isFinite(histogramBase)) {
            double upperBound = getUpperBucketBoundary(bucketIndex, scale);
            return 2 / (histogramBase + 1) * upperBound;
        } else {
            if (bucketIndex >= 0) {
                // the bucket is (1, +inf), approximate point of least error as inf
                return Double.POSITIVE_INFINITY;
            } else {
                // the bucket is (1/(Inf), 1), approximate point of least error as 0
                return 0;
            }
        }
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
        return Base2ExponentialHistogramIndexer.computeIndex(value, scale);
    }

    private static void checkIndexAndScaleBounds(long index, int scale) {
        checkIndexBounds(index);
        checkScaleBounds(scale);
    }

    private static void checkScaleBounds(int scale) {
        assert scale >= MIN_SCALE && scale <= MAX_SCALE : "scale must be in range [" + MIN_SCALE + ".." + MAX_SCALE + "]";
    }

    private static void checkIndexBounds(long index) {
        assert index >= MIN_INDEX && index <= MAX_INDEX : "index must be in range [" + MIN_INDEX + ".." + MAX_INDEX + "]";
    }

}
