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
        assert newScale >= MIN_SCALE && newScale <= MAX_SCALE
            : "adjusted scale must be in the range [" + MIN_SCALE + ", " + MAX_SCALE + "]";

        if (scaleAdjustment <= 0) {
            return index >> -scaleAdjustment;
        } else {
            assert scaleAdjustment <= MAX_INDEX_BITS : "Scaling up more than " + MAX_INDEX_BITS + " does not make sense";
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
     * Returns the maximum permissible scale increase that does not cause the index to grow out
     * of the [{@link ExponentialHistogram#MIN_INDEX}, {@link ExponentialHistogram#MIN_INDEX}] range.
     *
     * @param index the index to check
     * @return the maximum permissible scale increase
     */
    public static int getMaximumScaleIncrease(long index) {
        checkIndexBounds(index);
        // Scale increase by one corresponds to a left shift, which in turn is the same as multiplying by two.
        // Because we know that MIN_INDEX = -MAX_INDEX, we can just compute the maximum increase of the absolute index.
        // This allows us to reason only about non-negative indices further below.
        index = Math.abs(index);
        // the maximum scale increase is defined by how many left-shifts we can do without growing beyond MAX_INDEX
        // MAX_INDEX is defined as a number where the left MAX_INDEX_BITS are all ones.
        // So in other words, we must ensure that the leftmost (64 - MAX_INDEX_BITS) remain zero,
        // which is exactly what the formula below does.
        return Long.numberOfLeadingZeros(index) - (64 - MAX_INDEX_BITS);
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
