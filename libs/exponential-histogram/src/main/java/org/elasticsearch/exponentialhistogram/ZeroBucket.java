/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.exponentialhistogram;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.compareLowerBoundaries;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.getLowerBucketBoundary;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogramUtils.computeIndex;
import static org.elasticsearch.exponentialhistogram.FixedSizeExponentialHistogram.DEFAULT_BUCKET_SCALE;

public record ZeroBucket(long index, int scale, long count) {

    private static final ZeroBucket MINIMAL_EMPTY = new ZeroBucket(Long.MIN_VALUE, Integer.MIN_VALUE/256, 0);

    public ZeroBucket(double zeroThreshold, long count) {
        this(computeIndex(zeroThreshold, DEFAULT_BUCKET_SCALE) + 1, DEFAULT_BUCKET_SCALE, count);
    }

    public static ZeroBucket minimalEmpty() {
        return MINIMAL_EMPTY;
    }

    public static ZeroBucket minimalWithCount(long count) {
        if (count == 0) {
            return MINIMAL_EMPTY;
        } else {
            return new ZeroBucket(MINIMAL_EMPTY.index, MINIMAL_EMPTY.scale(), count);
        }
    }

    /**
     * Merges this zero-bucket with a given other one:
     *  * If the other zero-bucket is empty, the current one is returned unchanged
     *  * Otherwise the zero-threshold is increased if required and the counts are summed up
     */
    public ZeroBucket merge(ZeroBucket other) {
        if (other.count == 0) {
            return this;
        } else {
            long totalCount = count + other.count;
            // both are populate, we need to use the higher zero-threshold
            if (this.compareZeroThreshold(other) >= 0) {
                return new ZeroBucket(index, scale, totalCount);
            } else {
                return new ZeroBucket(other.index, other.scale, totalCount);
            }
        }
    }

    public ZeroBucket collapseOverlappingBuckets(ExponentialHistogram.BucketIterator... bucketIterators) {
        ZeroBucket current = this;
        ZeroBucket previous;
        do {
            previous = current;
            for (ExponentialHistogram.BucketIterator buckets: bucketIterators) {
                current = current.collapseOverlappingBuckets(buckets);
            }
        } while (previous.compareZeroThreshold(current) != 0);
        return current;
    }

    public int compareZeroThreshold(ZeroBucket other) {
        return compareLowerBoundaries(index, scale, other.index, other.scale);
    }

    public double zeroThreshold() {
        return getLowerBucketBoundary(index, scale);
    }

    /**
     * Collapses all buckets from the given iterator whose lower boundary is smaller than the zero threshold.
     * The iterator is advanced to point at the first, non-collapsed bucket.
     */
    public ZeroBucket collapseOverlappingBuckets(ExponentialHistogram.BucketIterator buckets) {

        long collapsedCount = 0;
        long highestCollapsedIndex = 0;
        while (buckets.hasNext() && compareLowerBoundaries(buckets.peekIndex(), buckets.scale(), index, scale) < 0) {
            highestCollapsedIndex = buckets.peekIndex();
            collapsedCount += buckets.peekCount();
            buckets.advance();
        }
        if (collapsedCount == 0) {
            return this;
        } else {
            long newZeroCount = count + collapsedCount;
            // +1 because we need to adjust the zero threshold to the upper boundary of the collapsed bucket
            long collapsedUpperBoundIndex = Math.addExact(highestCollapsedIndex , 1);
            if (compareLowerBoundaries(index, scale, collapsedUpperBoundIndex, buckets.scale()) >= 0) {
                // we still have a larger zero-threshold than the largest collapsed bucket's upper boundary
                return new ZeroBucket(index, scale, newZeroCount);
            } else {
                return new ZeroBucket(collapsedUpperBoundIndex, buckets.scale(), newZeroCount);
            }
        }
    }
}
