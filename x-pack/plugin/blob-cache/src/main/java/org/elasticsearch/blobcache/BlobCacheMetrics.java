/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BlobCacheMetrics {
    private static final String CACHE_POPULATION_REASON_ATTRIBUTE_KEY = "cachePopulationReason";
    private static final String SHARD_ID_ATTRIBUTE_KEY = "shardId";

    private final LongCounter cacheMissCounter;
    private final LongCounter evictedCountNonZeroFrequency;
    private final LongHistogram cacheMissLoadTimes;
    private final LongHistogram cachePopulateReadThroughput;
    private final LongHistogram cachePopulateWriteThroughput;
    private final LongHistogram cachePopulationElapsedTime;

    public enum CachePopulationReason {
        /**
         * When warming the cache
         */
        Warming,
        /**
         * When fetching a new commit
         */
        LoadCommit,
        /**
         * When the data we need is not in the cache
         */
        CacheMiss
    }

    public BlobCacheMetrics(MeterRegistry meterRegistry) {
        this(
            meterRegistry.registerLongCounter(
                "es.blob_cache.miss_that_triggered_read.total",
                "The number of times there was a cache miss that triggered a read from the blob store",
                "count"
            ),
            meterRegistry.registerLongCounter(
                "es.blob_cache.count_of_evicted_used_regions.total",
                "The number of times a cache entry was evicted where the frequency was not zero",
                "entries"
            ),
            meterRegistry.registerLongHistogram(
                "es.blob_cache.cache_miss_load_times.histogram",
                "The time in milliseconds for populating entries in the blob store resulting from a cache miss, expressed as a histogram.",
                "ms"
            ),
            meterRegistry.registerLongHistogram(
                "es.blob_cache.populate_read_throughput.histogram",
                "The read throughput when reading from the blob store to populate the cache",
                "bytes/second"
            ),
            meterRegistry.registerLongHistogram(
                "es.blob_cache.populate_write_throughput.histogram",
                "The write throughput when writing data from the blobstore to the cache",
                "bytes/second"
            ),
            meterRegistry.registerLongHistogram(
                "es.blob_cache.populate_elapsed_time.histogram",
                "The time taken to copy a chunk from the blob store to the cache",
                "ms"
            )
        );
    }

    BlobCacheMetrics(
        LongCounter cacheMissCounter,
        LongCounter evictedCountNonZeroFrequency,
        LongHistogram cacheMissLoadTimes,
        LongHistogram cachePopulateReadThroughput,
        LongHistogram cachePopulateWriteThroughput,
        LongHistogram cachePopulationElapsedTime
    ) {
        this.cacheMissCounter = cacheMissCounter;
        this.evictedCountNonZeroFrequency = evictedCountNonZeroFrequency;
        this.cacheMissLoadTimes = cacheMissLoadTimes;
        this.cachePopulateReadThroughput = cachePopulateReadThroughput;
        this.cachePopulateWriteThroughput = cachePopulateWriteThroughput;
        this.cachePopulationElapsedTime = cachePopulationElapsedTime;
    }

    public static BlobCacheMetrics NOOP = new BlobCacheMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    public LongCounter getCacheMissCounter() {
        return cacheMissCounter;
    }

    public LongCounter getEvictedCountNonZeroFrequency() {
        return evictedCountNonZeroFrequency;
    }

    public LongHistogram getCacheMissLoadTimes() {
        return cacheMissLoadTimes;
    }

    /**
     * Record the various cache population metrics after a chunk is copied to the cache
     *
     * @param totalBytesRead The total number of bytes read
     * @param totalReadTimeNanos The time taken to read the bytes in nanoseconds
     * @param totalBytesWritten The total number of bytes written
     * @param totalWriteTimeNanos The time taken to write the bytes in nanoseconds
     * @param elapsedTimeNanoseconds The time taken to copy the entire chunk in nanoseconds
     * @param shardId The shard ID to which the chunk belonged
     * @param cachePopulationReason The reason for the cache being populated
     */
    public void recordCachePopulationMetrics(
        int totalBytesRead,
        long totalReadTimeNanos,
        int totalBytesWritten,
        long totalWriteTimeNanos,
        long elapsedTimeNanoseconds,
        ShardId shardId,
        CachePopulationReason cachePopulationReason
    ) {
        Map<String, Object> metricAttributes = Map.of(
            SHARD_ID_ATTRIBUTE_KEY,
            shardId,
            CACHE_POPULATION_REASON_ATTRIBUTE_KEY,
            cachePopulationReason
        );
        recordBytesPerSecondMetric(cachePopulateReadThroughput, totalBytesRead, totalReadTimeNanos, metricAttributes);
        recordBytesPerSecondMetric(cachePopulateWriteThroughput, totalBytesWritten, totalWriteTimeNanos, metricAttributes);
        cachePopulationElapsedTime.record(TimeUnit.NANOSECONDS.toMillis(elapsedTimeNanoseconds), metricAttributes);
    }

    private void recordBytesPerSecondMetric(
        LongHistogram histogram,
        int totalBytes,
        long totalTimeNanos,
        Map<String, Object> metricAttributes
    ) {
        // Protect against divide-by-zero (nano timestamps can be very coarse on some platforms)
        if (totalTimeNanos == 0) {
            return;
        }
        histogram.record(toBytesPerSecond(totalBytes, totalTimeNanos), metricAttributes);
    }

    /**
     * Calculate throughput as bytes/second
     *
     * @param totalBytes The total number of bytes transferred
     * @param totalNanoseconds The time to transfer in nanoseconds
     * @return The throughput as bytes/second
     */
    private int toBytesPerSecond(int totalBytes, long totalNanoseconds) {
        double totalSeconds = totalNanoseconds / 1_000_000_000.0;
        return (int) (totalBytes / totalSeconds);
    }
}
