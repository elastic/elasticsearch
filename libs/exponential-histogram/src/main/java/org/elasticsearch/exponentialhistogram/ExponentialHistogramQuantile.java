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

import java.util.OptionalLong;

/**
 * Provides quantile estimation for {@link ExponentialHistogram} instances.
 * All algorithms assume that the values of each histogram bucket have exactly the same value,
 * which is defined by {@link ExponentialScaleUtils#getPointOfLeastRelativeError(long, int)}.
 */
public class ExponentialHistogramQuantile {

    /**
     * Estimates a quantile for the distribution represented by the given histogram.
     *
     * It returns the value of the element at rank {@code max(0, min(n - 1, (quantile * (n + 1)) - 1))}, where n is the total number of
     * values and rank starts at 0. If the rank is fractional, the result is linearly interpolated from the values of the two
     * neighboring ranks.
     * The result is clamped to the histogram's minimum and maximum values.
     *
     * @param histo    the histogram representing the distribution
     * @param quantile the quantile to query, in the range [0, 1]
     * @return the estimated quantile value, or {@link Double#NaN} if the histogram is empty
     */
    public static double getQuantile(ExponentialHistogram histo, double quantile) {
        if (quantile < 0 || quantile > 1) {
            throw new IllegalArgumentException("quantile must be in range [0, 1]");
        }

        long zeroCount = histo.zeroBucket().count();
        long negCount = histo.negativeBuckets().valueCount();
        long posCount = histo.positiveBuckets().valueCount();

        long totalCount = zeroCount + negCount + posCount;
        if (totalCount == 0) {
            // Can't compute quantile on an empty histogram
            return Double.NaN;
        }

        double exactRank = quantile * (totalCount - 1);
        long lowerRank = (long) Math.floor(exactRank);
        long upperRank = (long) Math.ceil(exactRank);
        double upperFactor = exactRank - lowerRank;

        ValueAndPreviousValue values = getElementAtRank(histo, upperRank);
        // Ensure we don't return values outside the histogram's range
        values = values.clampTo(histo.min(), histo.max());

        double result;
        if (lowerRank == upperRank) {
            result = values.valueAtRank();
        } else {
            result = values.valueAtPreviousRank() * (1 - upperFactor) + values.valueAtRank() * upperFactor;
        }
        return removeNegativeZero(result);
    }

    /**
     * Estimates the rank of a given value in the distribution represented by the histogram.
     * In other words, returns the number of values which are less than (or less-or-equal, if {@code inclusive} is true)
     * the provided value.
     *
     * @param histo the histogram to query
     * @param value the value to estimate the rank for
     * @param inclusive if true, counts values equal to the given value as well
     * @return the number of elements less than (or less-or-equal, if {@code inclusive} is true) the given value
     */
    public static long estimateRank(ExponentialHistogram histo, double value, boolean inclusive) {
        if (Double.isNaN(histo.min()) || value < histo.min()) {
            return 0;
        }
        if (value > histo.max()) {
            return histo.valueCount();
        }
        if (value >= 0) {
            long rank = histo.negativeBuckets().valueCount();
            if (value > 0 || inclusive) {
                rank += histo.zeroBucket().count();
            }
            rank += estimateRank(histo.positiveBuckets().iterator(), value, inclusive, histo.max());
            return rank;
        } else {
            long numValuesGreater = estimateRank(histo.negativeBuckets().iterator(), -value, inclusive == false, -histo.min());
            return histo.negativeBuckets().valueCount() - numValuesGreater;
        }
    }

    private static long estimateRank(BucketIterator buckets, double value, boolean inclusive, double maxValue) {
        long rank = 0;
        while (buckets.hasNext()) {
            double bucketMidpoint = ExponentialScaleUtils.getPointOfLeastRelativeError(buckets.peekIndex(), buckets.scale());
            bucketMidpoint = Math.min(bucketMidpoint, maxValue);
            if (bucketMidpoint < value || (inclusive && bucketMidpoint == value)) {
                rank += buckets.peekCount();
                buckets.advance();
            } else {
                break;
            }
        }
        return rank;
    }

    private static double removeNegativeZero(double result) {
        return result == 0.0 ? 0.0 : result;
    }

    private static ValueAndPreviousValue getElementAtRank(ExponentialHistogram histo, long rank) {
        long negativeValuesCount = histo.negativeBuckets().valueCount();
        long zeroCount = histo.zeroBucket().count();
        if (rank < negativeValuesCount) {
            if (rank == 0) {
                return new ValueAndPreviousValue(Double.NaN, -getLastBucketMidpoint(histo.negativeBuckets()));
            } else {
                return getBucketMidpointForRank(histo.negativeBuckets().iterator(), negativeValuesCount - rank).negateAndSwap();
            }
        } else if (rank < (negativeValuesCount + zeroCount)) {
            if (rank == negativeValuesCount) {
                // the element at the previous rank falls into the negative bucket range
                return new ValueAndPreviousValue(-getFirstBucketMidpoint(histo.negativeBuckets()), 0.0);
            } else {
                return new ValueAndPreviousValue(0.0, 0.0);
            }
        } else {
            ValueAndPreviousValue result = getBucketMidpointForRank(
                histo.positiveBuckets().iterator(),
                rank - negativeValuesCount - zeroCount
            );
            if ((rank - 1) < negativeValuesCount) {
                // previous value falls into the negative bucket range or has rank -1 and therefore doesn't exist
                return new ValueAndPreviousValue(-getFirstBucketMidpoint(histo.negativeBuckets()), result.valueAtRank);
            } else if ((rank - 1) < (negativeValuesCount + zeroCount)) {
                // previous value falls into the zero bucket
                return new ValueAndPreviousValue(0.0, result.valueAtRank);
            } else {
                return result;
            }
        }
    }

    private static double getFirstBucketMidpoint(ExponentialHistogram.Buckets buckets) {
        CopyableBucketIterator iterator = buckets.iterator();
        if (iterator.hasNext()) {
            return ExponentialScaleUtils.getPointOfLeastRelativeError(iterator.peekIndex(), iterator.scale());
        } else {
            return Double.NaN;
        }
    }

    private static double getLastBucketMidpoint(ExponentialHistogram.Buckets buckets) {
        OptionalLong highestIndex = buckets.maxBucketIndex();
        if (highestIndex.isPresent()) {
            return ExponentialScaleUtils.getPointOfLeastRelativeError(highestIndex.getAsLong(), buckets.iterator().scale());
        } else {
            return Double.NaN;
        }
    }

    private static ValueAndPreviousValue getBucketMidpointForRank(BucketIterator buckets, long rank) {
        long prevIndex = Long.MIN_VALUE;
        long seenCount = 0;
        while (buckets.hasNext()) {
            seenCount += buckets.peekCount();
            if (rank < seenCount) {
                double center = ExponentialScaleUtils.getPointOfLeastRelativeError(buckets.peekIndex(), buckets.scale());
                double prevCenter;
                if (rank > 0) {
                    if ((rank - 1) >= (seenCount - buckets.peekCount())) {
                        // element at previous rank is in same bucket
                        prevCenter = center;
                    } else {
                        // element at previous rank is in the previous bucket
                        prevCenter = ExponentialScaleUtils.getPointOfLeastRelativeError(prevIndex, buckets.scale());
                    }
                } else {
                    // there is no previous element
                    prevCenter = Double.NaN;
                }
                return new ValueAndPreviousValue(prevCenter, center);
            }
            prevIndex = buckets.peekIndex();
            buckets.advance();
        }
        throw new IllegalStateException("The total number of elements in the buckets is less than the desired rank.");
    }

    /**
     * @param valueAtPreviousRank the value at the rank before the desired rank, NaN if not applicable.
     * @param valueAtRank         the value at the desired rank
     */
    private record ValueAndPreviousValue(double valueAtPreviousRank, double valueAtRank) {

        ValueAndPreviousValue negateAndSwap() {
            return new ValueAndPreviousValue(-valueAtRank, -valueAtPreviousRank);
        }

        ValueAndPreviousValue clampTo(double min, double max) {
            return new ValueAndPreviousValue(Math.clamp(valueAtPreviousRank, min, max), Math.clamp(valueAtRank, min, max));
        }
    }
}
