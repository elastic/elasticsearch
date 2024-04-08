/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

public class TransportActionStatsTracker {

    /*
     *  network messages are at least 6 bytes ('E' + 'S' + 4-byte size) so the histogram buckets start from '<8':
     *
     *   0:  < 8
     *   1:  < 16
     * ...
     *   n:  < 2^(n+3)
     * ...
     *  27:  < 2^30
     *  28: ≥ 2^30 == 1GiB
     */

    public static int[] getBucketUpperBounds() {
        var bounds = new int[28];
        for (int i = 0; i < 28; i++) {
            bounds[i] = 8 << i;
        }
        return bounds;
    }

    private static final int MAX_BUCKET = getBucketUpperBounds().length;

    private record StatsTracker(LongAdder count, LongAdder totalSize, AtomicLongArray histogram) {
        StatsTracker {
            assert count.longValue() == 0L;
            assert totalSize.longValue() == 0L;
            assert histogram.length() == MAX_BUCKET + 1;
        }

        StatsTracker() {
            this(new LongAdder(), new LongAdder(), new AtomicLongArray(MAX_BUCKET + 1));
        }

        void addStats(int messageSize) {
            count().increment();
            totalSize().add(messageSize);
            histogram().incrementAndGet(bucket(messageSize));
        }

        long[] getHistogram() {
            long[] histogramCopy = new long[MAX_BUCKET + 1];
            for (int i = 0; i <= MAX_BUCKET; i++) {
                histogramCopy[i] = histogram().get(i);
            }
            return histogramCopy;
        }
    }

    private static int bucket(int messageSize) {
        return Math.min(Math.max(29 - Integer.numberOfLeadingZeros(messageSize), 0), MAX_BUCKET);
    }

    private final StatsTracker requestStats = new StatsTracker();
    private final StatsTracker responseStats = new StatsTracker();

    public void addRequestStats(int messageSize) {
        requestStats.addStats(messageSize);
    }

    public void addResponseStats(int messageSize) {
        responseStats.addStats(messageSize);
    }

    public TransportActionStats getStats() {
        return new TransportActionStats(
            requestStats.count().longValue(),
            requestStats.totalSize().longValue(),
            requestStats.getHistogram(),
            responseStats.count().longValue(),
            responseStats.totalSize().longValue(),
            responseStats.getHistogram()
        );
    }

}
