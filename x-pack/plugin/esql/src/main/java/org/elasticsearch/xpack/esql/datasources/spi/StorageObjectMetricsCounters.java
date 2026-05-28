/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.concurrent.atomic.LongAdder;

/**
 * Mutable, thread-safe counter struct for storage I/O. Provider implementations
 * hold one of these per {@link StorageObject} instance, increment it around each
 * I/O call, and surface the latest values via {@link #snapshot()} from
 * {@link StorageObject#metrics()}.
 * <p>
 * The split between this mutable struct and the immutable {@link StorageObjectMetrics}
 * snapshot mirrors the {@code Collector(LongAdder, LongAdder)} / {@code BlobStoreActionStats}
 * pattern used by ES repository plugins (see {@code GcsRepositoryStatsCollector}).
 * <p>
 * {@link LongAdder} is preferred over {@code AtomicLong} because async SDK callbacks
 * may concurrently increment from multiple threads and contention on a single AtomicLong
 * would dominate hot paths in object-store reads.
 */
public final class StorageObjectMetricsCounters {

    private final LongAdder requestCount = new LongAdder();
    private final LongAdder requestNanos = new LongAdder();
    private final LongAdder bytesRead = new LongAdder();
    private final LongAdder retryCount = new LongAdder();

    /** Records one completed request with its duration and the bytes returned. */
    public void addRequest(long durationNanos, long bytes) {
        requestCount.increment();
        if (durationNanos > 0) {
            requestNanos.add(durationNanos);
        }
        if (bytes > 0) {
            bytesRead.add(bytes);
        }
    }

    /** Records one automatic retry triggered inside an in-flight request. */
    public void addRetry() {
        retryCount.increment();
    }

    /** Returns an immutable snapshot of the current counter values. */
    public StorageObjectMetrics snapshot() {
        return new StorageObjectMetrics(requestCount.sum(), requestNanos.sum(), bytesRead.sum(), retryCount.sum());
    }
}
