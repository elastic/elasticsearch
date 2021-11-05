/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.network;

import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks how long message handling takes on a transport thread as a histogram with fixed buckets.
 */
public class HandlingTimeTracker {

    public static int[] getBucketUpperBounds() {
        // Default clock resolution is 200ms so we have buckets for the 0-tick and 1-tick cases, then go up in powers of two
        return new int[] { 100, 300, 1 << 9, 1 << 10, 1 << 11, 1 << 12, 1 << 13, 1 << 14, 1 << 15, 1 << 16 };
    }

    private static int getBucket(long handlingTimeMillis) {
        if (handlingTimeMillis < 100L) {
            return 0;
        } else if (handlingTimeMillis < 300L) {
            return 1;
        } else if (1L << 16 <= handlingTimeMillis) {
            return BUCKET_COUNT - 1;
        } else {
            return Long.SIZE - 7 - Long.numberOfLeadingZeros(handlingTimeMillis);
        }
    }

    public static final int BUCKET_COUNT = getBucketUpperBounds().length + 1;

    private final LongAdder[] buckets;

    public HandlingTimeTracker() {
        buckets = new LongAdder[BUCKET_COUNT];
        for (int i = 0; i < BUCKET_COUNT; i++) {
            buckets[i] = new LongAdder();
        }
    }

    public void addHandlingTime(long handlingTimeMillis) {
        buckets[getBucket(handlingTimeMillis)].increment();
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

}
