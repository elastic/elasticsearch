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

import org.apache.lucene.util.RamUsageEstimator;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.compareExponentiallyScaledValues;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.computeIndex;
import static org.elasticsearch.exponentialhistogram.ExponentialScaleUtils.exponentiallyScaledToDoubleValue;

/**
 * Represents the bucket for values around zero in an exponential histogram.
 * The range of this bucket is {@code [-zeroThreshold, +zeroThreshold]}.
 * To allow efficient comparison with bucket boundaries, this class internally
 * represents the zero threshold as a exponential histogram bucket index with a scale,
 * computed via {@link ExponentialScaleUtils#computeIndex(double, int)}.
 */
public final class ZeroBucket {

    public static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(ZeroBucket.class);

    /**
     * The exponential histogram scale used for {@link #index}
     */
    private final int scale;

    /**
     * The exponential histogram bucket index whose upper boundary corresponds to the zero threshold.
     * Might be computed lazily from {@link #realThreshold}, uses {@link Long#MAX_VALUE} as placeholder in this case.
     */
    private long index;

    /**
     * Might be computed lazily from {@link #realThreshold}, uses {@link Double#NaN} as placeholder in this case.
     */
    private double realThreshold;

    private final long count;
    // A singleton for an empty zero bucket with the smallest possible threshold.
    private static final ZeroBucket MINIMAL_EMPTY = new ZeroBucket(MIN_INDEX, MIN_SCALE, 0);

    /**
     * Creates a new zero bucket with a specific threshold and count.
     *
     * @param zeroThreshold The threshold defining the bucket's range [-zeroThreshold, +zeroThreshold].
     * @param count         The number of values in the bucket.
     */
    public ZeroBucket(double zeroThreshold, long count) {
        assert zeroThreshold >= 0.0 : "zeroThreshold must not be negative";
        this.index = Long.MAX_VALUE; // compute lazily when needed
        this.scale = MAX_SCALE;
        this.realThreshold = zeroThreshold;
        this.count = count;
    }

    private ZeroBucket(long index, int scale, long count) {
        assert index >= MIN_INDEX && index <= MAX_INDEX : "index must be in range [" + MIN_INDEX + ", " + MAX_INDEX + "]";
        assert scale >= MIN_SCALE && scale <= MAX_SCALE : "scale must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "]";
        this.index = index;
        this.scale = scale;
        this.realThreshold = Double.NaN; // compute lazily when needed
        this.count = count;
    }

    private ZeroBucket(double realThreshold, long index, int scale, long count) {
        this.realThreshold = realThreshold;
        this.index = index;
        this.scale = scale;
        this.count = count;
    }

    /**
     * @return A singleton instance of an empty zero bucket with the smallest possible threshold.
     */
    public static ZeroBucket minimalEmpty() {
        return MINIMAL_EMPTY;
    }

    /**
     * Creates a zero bucket with the smallest possible threshold and a given count.
     *
     * @param count The number of values in the bucket.
     * @return A new {@link ZeroBucket}.
     */
    public static ZeroBucket minimalWithCount(long count) {
        if (count == 0) {
            return MINIMAL_EMPTY;
        } else {
            return new ZeroBucket(MINIMAL_EMPTY.zeroThreshold(), MINIMAL_EMPTY.index(), MINIMAL_EMPTY.scale(), count);
        }
    }

    /**
     * @return The value of the zero threshold.
     */
    public double zeroThreshold() {
        if (Double.isNaN(realThreshold)) {
            realThreshold = exponentiallyScaledToDoubleValue(index(), scale());
        }
        return realThreshold;
    }

    public long index() {
        if (index == Long.MAX_VALUE) {
            index = computeIndex(zeroThreshold(), scale()) + 1;
        }
        return index;
    }

    public int scale() {
        return scale;
    }

    public long count() {
        return count;
    }

    /**
     * Merges this zero bucket with another one.
     * <ul>
     *     <li>If the other zero bucket or both are empty, this instance is returned unchanged.</li>
     *     <li>If the this zero bucket is empty and the other one is populated, the other instance is returned unchanged.</li>
     *     <li>Otherwise, the zero threshold is increased if necessary (by taking the maximum of the two), and the counts are summed.</li>
     * </ul>
     *
     * @param other The other zero bucket to merge with.
     * @return A new {@link ZeroBucket} representing the merged result.
     */
    public ZeroBucket merge(ZeroBucket other) {
        if (other.count == 0) {
            return this;
        } else if (count == 0) {
            return other;
        } else {
            long totalCount = count + other.count;
            // Both are populated, so we need to use the higher zero-threshold.
            if (this.compareZeroThreshold(other) >= 0) {
                return new ZeroBucket(realThreshold, index, scale, totalCount);
            } else {
                return new ZeroBucket(other.realThreshold, other.index, other.scale, totalCount);
            }
        }
    }

    /**
     * Collapses all buckets from the given iterators whose lower boundaries are smaller than the zero threshold.
     * The iterators are advanced to point at the first, non-collapsed bucket.
     *
     * @param bucketIterators The iterators whose buckets may be collapsed.
     * @return A potentially updated {@link ZeroBucket} with the collapsed buckets' counts and an adjusted threshold.
     */
    public ZeroBucket collapseOverlappingBucketsForAll(BucketIterator... bucketIterators) {
        ZeroBucket current = this;
        ZeroBucket previous;
        do {
            previous = current;
            for (BucketIterator buckets : bucketIterators) {
                current = current.collapseOverlappingBuckets(buckets);
            }
        } while (previous.compareZeroThreshold(current) != 0);
        return current;
    }

    /**
     * Compares the zero threshold of this bucket with another one.
     *
     * @param other The other zero bucket to compare against.
     * @return A negative integer, zero, or a positive integer if this bucket's threshold is less than,
     *         equal to, or greater than the other's.
     */
    public int compareZeroThreshold(ZeroBucket other) {
        return compareExponentiallyScaledValues(index(), scale(), other.index(), other.scale());
    }

    /**
     * Collapses all buckets from the given iterator whose lower boundaries are smaller than the zero threshold.
     * The iterator is advanced to point at the first, non-collapsed bucket.
     *
     * @param buckets The iterator whose buckets may be collapsed.
     * @return A potentially updated {@link ZeroBucket} with the collapsed buckets' counts and an adjusted threshold.
     */
    public ZeroBucket collapseOverlappingBuckets(BucketIterator buckets) {

        long collapsedCount = 0;
        long highestCollapsedIndex = 0;
        while (buckets.hasNext() && compareExponentiallyScaledValues(buckets.peekIndex(), buckets.scale(), index(), scale()) < 0) {
            highestCollapsedIndex = buckets.peekIndex();
            collapsedCount += buckets.peekCount();
            buckets.advance();
        }
        if (collapsedCount == 0) {
            return this;
        } else {
            long newZeroCount = count + collapsedCount;
            // +1 because we need to adjust the zero threshold to the upper boundary of the collapsed bucket
            long collapsedUpperBoundIndex = highestCollapsedIndex + 1;
            if (compareExponentiallyScaledValues(index(), scale(), collapsedUpperBoundIndex, buckets.scale()) >= 0) {
                // Our current zero-threshold is larger than the upper boundary of the largest collapsed bucket, so we keep it.
                return new ZeroBucket(realThreshold, index, scale, newZeroCount);
            } else {
                return new ZeroBucket(collapsedUpperBoundIndex, buckets.scale(), newZeroCount);
            }
        }
    }
}
