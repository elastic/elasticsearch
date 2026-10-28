/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.metrics;

import java.util.Arrays;
import java.util.concurrent.atomic.LongAdder;

/**
 * A histogram with a fixed number of buckets of exponentially increasing width.
 * <p>
 * The bucket boundaries are defined by increasing powers of two, e.g.
 * <code>
 *     (-&infin;, 1), [1, 2), [2, 4), [4, 8), ..., [2^({@link #bucketCount}-2), &infin;)
 * </code>
 * There are {@link #bucketCount} buckets.
 */
public class ExponentialBucketHistogram {

    private final int bucketCount;
    private final long lastBucketLowerBound;

    public static int[] getBucketUpperBounds(int bucketCount) {
        int[] bounds = new int[bucketCount - 1];
        for (int i = 0; i < bounds.length; i++) {
            bounds[i] = 1 << i;
        }
        return bounds;
    }

    private int getBucket(long observedValue) {
        if (observedValue <= 0) {
            return 0;
        } else if (lastBucketLowerBound <= observedValue) {
            return bucketCount - 1;
        } else {
            return Long.SIZE - Long.numberOfLeadingZeros(observedValue);
        }
    }

    private final LongAdder[] buckets;

    public ExponentialBucketHistogram(int bucketCount) {
        if (bucketCount < 2 || bucketCount > Integer.SIZE) {
            throw new IllegalArgumentException("Bucket count must be in [2, " + Integer.SIZE + "], got " + bucketCount);
        }
        this.bucketCount = bucketCount;
        this.lastBucketLowerBound = getBucketUpperBounds(bucketCount)[bucketCount - 2];
        buckets = new LongAdder[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            buckets[i] = new LongAdder();
        }
    }

    public int[] calculateBucketUpperBounds() {
        return getBucketUpperBounds(bucketCount);
    }

    public void addObservation(long observedValue) {
        buckets[getBucket(observedValue)].increment();
    }

    /**
     * @return An array of frequencies of handling times in buckets with upper bounds as returned by {@link #calculateBucketUpperBounds()},
     *         plus an extra bucket for handling times longer than the longest upper bound.
     */
    public long[] getSnapshot() {
        final long[] histogram = new long[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            histogram[i] = buckets[i].longValue();
        }
        return histogram;
    }

    /**
     * Calculate the Nth percentile value
     *
     * @param percentile The percentile as a fraction (in [0, 1.0])
     * @return A value greater than the specified fraction of values in the histogram
     * @throws IllegalArgumentException if the requested percentile is invalid
     */
    public long getPercentile(float percentile) {
        return getPercentile(percentile, getSnapshot(), calculateBucketUpperBounds());
    }

    /**
     * Calculate the Nth percentile value
     *
     * @param percentile The percentile as a fraction (in [0, 1.0])
     * @param snapshot An array of frequencies of handling times in buckets with upper bounds as per {@link #calculateBucketUpperBounds()}
     * @param bucketUpperBounds The upper bounds of the buckets in the histogram, as per {@link #calculateBucketUpperBounds()}
     * @return A value greater than the specified fraction of values in the histogram
     * @throws IllegalArgumentException if the requested percentile is invalid
     */
    public long getPercentile(float percentile, long[] snapshot, int[] bucketUpperBounds) {
        assert snapshot.length == bucketCount && bucketUpperBounds.length == bucketCount - 1;
        if (percentile < 0 || percentile > 1) {
            throw new IllegalArgumentException("Requested percentile must be in [0, 1.0], percentile=" + percentile);
        }
        final long totalCount = Arrays.stream(snapshot).sum();
        long percentileIndex = (long) Math.ceil(totalCount * percentile);
        // Find which bucket has the Nth percentile value and return the upper bound value.
        for (int i = 0; i < bucketCount; i++) {
            percentileIndex -= snapshot[i];
            if (percentileIndex <= 0) {
                if (i == snapshot.length - 1) {
                    return Long.MAX_VALUE;
                } else {
                    return bucketUpperBounds[i];
                }
            }
        }
        assert false : "We shouldn't ever get here";
        return Long.MAX_VALUE;
    }

    /**
     * Clear all values in the histogram (non-atomic)
     */
    public void clear() {
        for (int i = 0; i < bucketCount; i++) {
            buckets[i].reset();
        }
    }
}
