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
 *
 * The upper bounds of the buckets are defined by increasing powers of two, e.g.
 * <code>
 *     1, 2, 4, 8, 16...
 * </code>
 * There are {@link #BUCKET_COUNT} buckets
 */
public class ExponentialBucketHistogram {

    public static int[] getBucketUpperBounds() {
        int[] bounds = new int[17];
        for (int i = 0; i < bounds.length; i++) {
            bounds[i] = 1 << i;
        }
        return bounds;
    }

    private static int getBucket(long handlingTimeMillis) {
        if (handlingTimeMillis <= 0) {
            return 0;
        } else if (LAST_BUCKET_LOWER_BOUND <= handlingTimeMillis) {
            return BUCKET_COUNT - 1;
        } else {
            return Long.SIZE - Long.numberOfLeadingZeros(handlingTimeMillis);
        }
    }

    public static final int BUCKET_COUNT = getBucketUpperBounds().length + 1;

    private static final long LAST_BUCKET_LOWER_BOUND = getBucketUpperBounds()[BUCKET_COUNT - 2];

    private final LongAdder[] buckets;

    public ExponentialBucketHistogram() {
        buckets = new LongAdder[BUCKET_COUNT];
        for (int i = 0; i < BUCKET_COUNT; i++) {
            buckets[i] = new LongAdder();
        }
    }

    public void addObservation(long observedValue) {
        buckets[getBucket(observedValue)].increment();
    }

    /**
     * @return An array of frequencies of handling times in buckets with upper bounds as returned by {@link #getBucketUpperBounds()}, plus
     *         an extra bucket for handling times longer than the longest upper bound.
     */
    public long[] getHistogram() {
        final long[] histogram = new long[BUCKET_COUNT];
        for (int i = 0; i < BUCKET_COUNT; i++) {
            histogram[i] = buckets[i].longValue();
        }
        return histogram;
    }

    /**
     * Calculate the Nth percentile value
     *
     * @param percentile The percentile as a fraction (in [0, 1.0])
     * @return A value greater than or equal to the specified fraction of values in the histogram
     */
    public long getPercentile(float percentile) {
        assert percentile >= 0 && percentile <= 1;
        final long[] snapshot = getHistogram();
        final long totalCount = Arrays.stream(snapshot).reduce(0L, Long::sum);
        long percentileIndex = (long) Math.ceil(totalCount * percentile);
        for (int i = 0; i < BUCKET_COUNT; i++) {
            percentileIndex -= snapshot[i];
            if (percentileIndex <= 0) {
                return getBucketUpperBounds()[i];
            }
        }
        assert false : "We shouldn't ever get here";
        return Long.MAX_VALUE;
    }

    /**
     * Clear all values in the histogram (non-atomic)
     */
    public void clear() {
        for (int i = 0; i < BUCKET_COUNT; i++) {
            buckets[i].reset();
        }
    }
}
