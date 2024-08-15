/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class BlobCacheMetrics {
    public static final String CACHE_POPULATION_REASON_ATTRIBUTE_KEY = "reason";
    public static final String CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY = "source";
    public static final String SHARD_ID_ATTRIBUTE_KEY = "shard_id";
    public static final String INDEX_ATTRIBUTE_KEY = "index";

    private final LongCounter cacheMissCounter;
    private final LongCounter evictedCountNonZeroFrequency;
    private final LongHistogram cacheMissLoadTimes;
    private final DoubleHistogram cachePopulateThroughput;
    private final LongCounter cachePopulationBytes;
    private final LongCounter cachePopulationTime;

    public enum CachePopulationReason {
        /**
         * When warming the cache
         */
        Warming,
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
            meterRegistry.registerDoubleHistogram(
                "es.blob_cache.populate_throughput.histogram",
                "The throughput when populating the blob store from the cache",
                "MiB/second"
            ),
            meterRegistry.registerLongCounter(
                "es.blob_cache.populate_bytes.total",
                "The number of bytes that have been loaded into the cache",
                "bytes"
            ),
            meterRegistry.registerLongCounter(
                "es.blob_cache.populate_time.total",
                "The time spent copying data into the cache",
                "milliseconds"
            )
        );
    }

    BlobCacheMetrics(
        LongCounter cacheMissCounter,
        LongCounter evictedCountNonZeroFrequency,
        LongHistogram cacheMissLoadTimes,
        DoubleHistogram cachePopulateThroughput,
        LongCounter cachePopulationBytes,
        LongCounter cachePopulationTime
    ) {
        this.cacheMissCounter = cacheMissCounter;
        this.evictedCountNonZeroFrequency = evictedCountNonZeroFrequency;
        this.cacheMissLoadTimes = cacheMissLoadTimes;
        this.cachePopulateThroughput = cachePopulateThroughput;
        this.cachePopulationBytes = cachePopulationBytes;
        this.cachePopulationTime = cachePopulationTime;
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
     * @param totalBytesCopied The total number of bytes copied
     * @param totalCopyTimeNanos The time taken to copy the bytes in nanoseconds
     * @param index The index being loaded
     * @param shardId The ID of the shard being loaded
     * @param cachePopulationReason The reason for the cache being populated
     * @param cachePopulationSource The source from which the data is being loaded
     */
    public void recordCachePopulationMetrics(
        int totalBytesCopied,
        long totalCopyTimeNanos,
        String index,
        int shardId,
        CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource
    ) {
        Map<String, Object> metricAttributes = Map.of(
            INDEX_ATTRIBUTE_KEY,
            index,
            SHARD_ID_ATTRIBUTE_KEY,
            shardId,
            CACHE_POPULATION_REASON_ATTRIBUTE_KEY,
            cachePopulationReason.name(),
            CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY,
            cachePopulationSource.name()
        );
        assert totalBytesCopied > 0 : "We shouldn't be recording zero-sized copies";
        cachePopulationBytes.incrementBy(totalBytesCopied, metricAttributes);

        // This is almost certainly paranoid, but if we had a very fast/small copy with a very coarse nanosecond timer it might happen?
        if (totalCopyTimeNanos > 0) {
            cachePopulateThroughput.record(toMebibytesPerSecond(totalBytesCopied, totalCopyTimeNanos), metricAttributes);
            cachePopulationTime.incrementBy(TimeUnit.NANOSECONDS.toMillis(totalCopyTimeNanos), metricAttributes);
        }
    }

    /**
     * Calculate throughput as MiB/second
     *
     * @param totalBytes The total number of bytes transferred
     * @param totalNanoseconds The time to transfer in nanoseconds
     * @return The throughput as MiB/second
     */
    private double toMebibytesPerSecond(int totalBytes, long totalNanoseconds) {
        double totalSeconds = totalNanoseconds / 1_000_000_000.0;
        double totalMegabytes = totalBytes / 1_048_576.0;
        return totalMegabytes / totalSeconds;
    }
}
