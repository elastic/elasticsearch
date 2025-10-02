/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.network.HandlingTimeTracker;

import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;

public class HttpRouteStatsTracker {

    /*
     * Default http.max_content_length is 100 MB. But response size can be much larger.
     * So we choose the last histogram bucket to be > 1GB (2^30)
     */

    public static int[] getBucketUpperBounds() {
        var bounds = new int[31];
        for (int i = 0; i < bounds.length; i++) {
            bounds[i] = 1 << i;
        }
        return bounds;
    }

    private static final int BUCKET_COUNT = getBucketUpperBounds().length + 1;

    private static final int LAST_BUCKET_LOWER_BOUND = getBucketUpperBounds()[BUCKET_COUNT - 2];

    private record StatsTracker(LongAdder count, LongAdder totalSize, AtomicLongArray histogram) {
        StatsTracker {
            assert count.longValue() == 0L;
            assert totalSize.longValue() == 0L;
            assert histogram.length() == BUCKET_COUNT;
        }

        StatsTracker() {
            this(new LongAdder(), new LongAdder(), new AtomicLongArray(BUCKET_COUNT));
        }

        void addStats(long contentLength) {
            count().increment();
            totalSize().add(contentLength);
            histogram().incrementAndGet(bucket(contentLength));
        }

        long[] getHistogram() {
            long[] histogramCopy = new long[BUCKET_COUNT];
            for (int i = 0; i < BUCKET_COUNT; i++) {
                histogramCopy[i] = histogram().get(i);
            }
            return histogramCopy;
        }
    }

    private static int bucket(long contentLength) {
        if (contentLength > Integer.MAX_VALUE) {
            return bucket(Integer.MAX_VALUE);
        } else {
            assert contentLength >= 0;
            return bucket((int) contentLength);
        }
    }

    private static int bucket(int contentLength) {
        if (contentLength <= 0) {
            return 0;
        } else if (LAST_BUCKET_LOWER_BOUND <= contentLength) {
            return BUCKET_COUNT - 1;
        } else {
            return Integer.SIZE - Integer.numberOfLeadingZeros(contentLength);
        }
    }

    private final StatsTracker requestStats = new StatsTracker();
    private final StatsTracker responseStats = new StatsTracker();
    private final HandlingTimeTracker responseTimeTracker = new HandlingTimeTracker();

    public void addRequestStats(int contentLength) {
        requestStats.addStats(contentLength);
    }

    public void addResponseStats(long contentLength) {
        responseStats.addStats(contentLength);
    }

    public void addResponseTime(long timeMillis) {
        responseTimeTracker.addObservation(timeMillis);
    }

    public HttpRouteStats getStats() {
        return new HttpRouteStats(
            requestStats.count().longValue(),
            requestStats.totalSize().longValue(),
            requestStats.getHistogram(),
            responseStats.count().longValue(),
            responseStats.totalSize().longValue(),
            responseStats.getHistogram(),
            responseTimeTracker.getSnapshot()
        );
    }
}
