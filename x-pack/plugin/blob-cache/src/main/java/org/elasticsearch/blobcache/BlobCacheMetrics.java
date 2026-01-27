/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;

public class BlobCacheMetrics {
    private static final Logger logger = LogManager.getLogger(BlobCacheMetrics.class);

    private static final double BYTES_PER_NANOSECONDS_TO_MEBIBYTES_PER_SECOND = 1e9D / (1 << 20);
    public static final String CACHE_POPULATION_REASON_ATTRIBUTE_KEY = "reason";
    public static final String CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY = "source";
    public static final String LUCENE_FILE_EXTENSION_ATTRIBUTE_KEY = "file_extension";
    public static final String ES_EXECUTOR_ATTRIBUTE_KEY = "executor";
    public static final String NON_LUCENE_EXTENSION_TO_RECORD = "other";
    public static final String NON_ES_EXECUTOR_TO_RECORD = "other";
    public static final String BLOB_CACHE_COUNT_OF_EVICTED_REGIONS_TOTAL = "es.blob_cache.count_of_evicted_regions.total";
    public static final String SEARCH_ORIGIN_REMOTE_STORAGE_DOWNLOAD_TOOK_TIME = "es.blob_cache.search_origin.download_took_time.total";

    private final LongCounter cacheMissCounter;
    private final LongCounter evictedCountNonZeroFrequency;
    private final LongCounter totalEvictedCount;
    private final LongHistogram cacheMissLoadTimes;
    private final DoubleHistogram cachePopulationThroughput;
    private final LongCounter cachePopulationBytes;
    private final LongCounter cachePopulationTime;

    private final LongAdder missCount = new LongAdder();
    private final LongAdder readCount = new LongAdder();
    private final LongCounter epochChanges;
    private final LongHistogram searchOriginDownloadTime;

    public enum CachePopulationReason {
        /**
         * When warming the cache
         */
        Warming,
        /**
         * When warming the cache as a result of an incoming request
         */
        OnlinePrewarming,
        /**
         * When the data we need is not in the cache
         */
        CacheMiss,
        /**
         * When data is prefetched upon new commit notifications
         */
        PreFetchingNewCommit
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
            meterRegistry.registerLongCounter(
                BLOB_CACHE_COUNT_OF_EVICTED_REGIONS_TOTAL,
                "The number of times a cache entry was evicted, irrespective of the frequency",
                "entries"
            ),
            meterRegistry.registerLongHistogram(
                "es.blob_cache.cache_miss_load_times.histogram",
                "The time in milliseconds for populating entries in the blob store resulting from a cache miss, expressed as a histogram.",
                "ms"
            ),
            meterRegistry.registerDoubleHistogram(
                "es.blob_cache.population.throughput.histogram",
                "The throughput observed when populating the cache",
                "MiB/second"
            ),
            meterRegistry.registerLongCounter(
                "es.blob_cache.population.bytes.total",
                "The number of bytes that have been copied into the cache",
                "bytes"
            ),
            meterRegistry.registerLongCounter(
                "es.blob_cache.population.time.total",
                "The time spent copying data into the cache",
                "milliseconds"
            ),
            meterRegistry.registerLongCounter("es.blob_cache.epoch.total", "The epoch changes of the LFU cache", "count"),
            meterRegistry.registerLongHistogram(
                SEARCH_ORIGIN_REMOTE_STORAGE_DOWNLOAD_TOOK_TIME,
                "The distribution of time in millis taken to download data from remote storage for search requests",
                "milliseconds"
            )
        );

        meterRegistry.registerLongGauge(
            "es.blob_cache.read.total",
            "The number of cache reads (warming not included)",
            "count",
            () -> new LongWithAttributes(readCount.longValue())
        );
        // notice that this is different from `miss_that_triggered_read` in that `miss_that_triggered_read` will count once per gap
        // filled for a single read. Whereas this one only counts whenever a read provoked populating data from the object store, though
        // once per region for multi-region reads. This allows reasoning about hit ratio too.
        meterRegistry.registerLongGauge(
            "es.blob_cache.miss.total",
            "The number of cache misses (warming not included)",
            "count",
            () -> new LongWithAttributes(missCount.longValue())
        );
        // adding this helps search for high or low miss ratio. It will be since boot of the node though. More advanced queries can use
        // deltas of the totals to see miss ratio over time.
        meterRegistry.registerDoubleGauge(
            "es.blob_cache.miss.ratio",
            "The fraction of cache reads that missed data (warming not included)",
            "fraction",
            // read misses before reads on purpose
            () -> new DoubleWithAttributes(Math.min((double) missCount.longValue() / Math.max(readCount.longValue(), 1L), 1.0d))
        );
    }

    BlobCacheMetrics(
        LongCounter cacheMissCounter,
        LongCounter evictedCountNonZeroFrequency,
        LongCounter totalEvictedCount,
        LongHistogram cacheMissLoadTimes,
        DoubleHistogram cachePopulationThroughput,
        LongCounter cachePopulationBytes,
        LongCounter cachePopulationTime,
        LongCounter epochChanges,
        LongHistogram searchOriginDownloadTime
    ) {
        this.cacheMissCounter = cacheMissCounter;
        this.evictedCountNonZeroFrequency = evictedCountNonZeroFrequency;
        this.totalEvictedCount = totalEvictedCount;
        this.cacheMissLoadTimes = cacheMissLoadTimes;
        this.cachePopulationThroughput = cachePopulationThroughput;
        this.cachePopulationBytes = cachePopulationBytes;
        this.cachePopulationTime = cachePopulationTime;
        this.epochChanges = epochChanges;
        this.searchOriginDownloadTime = searchOriginDownloadTime;
    }

    public static final BlobCacheMetrics NOOP = new BlobCacheMetrics(TelemetryProvider.NOOP.getMeterRegistry());

    public LongCounter getCacheMissCounter() {
        return cacheMissCounter;
    }

    public LongCounter getEvictedCountNonZeroFrequency() {
        return evictedCountNonZeroFrequency;
    }

    public LongCounter getTotalEvictedCount() {
        return totalEvictedCount;
    }

    public LongHistogram getCacheMissLoadTimes() {
        return cacheMissLoadTimes;
    }

    public LongHistogram getSearchOriginDownloadTime() {
        return searchOriginDownloadTime;
    }

    /**
     * Record the various cache population metrics after a chunk is copied to the cache
     *
     * @param fileName The actual (lucene) file that's requested from the blob location
     * @param bytesCopied The number of bytes copied
     * @param copyTimeNanos The time taken to copy the bytes in nanoseconds
     * @param cachePopulationReason The reason for the cache being populated
     * @param cachePopulationSource The source from which the data is being loaded
     */
    public void recordCachePopulationMetrics(
        String fileName,
        int bytesCopied,
        long copyTimeNanos,
        CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource
    ) {
        LuceneFilesExtensions luceneFilesExtensions = LuceneFilesExtensions.fromFile(fileName);
        String luceneFileExt = luceneFilesExtensions != null ? luceneFilesExtensions.getExtension() : NON_LUCENE_EXTENSION_TO_RECORD;
        String executorName = EsExecutors.executorName(Thread.currentThread());
        Map<String, Object> metricAttributes = Map.of(
            CACHE_POPULATION_REASON_ATTRIBUTE_KEY,
            cachePopulationReason.name(),
            CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY,
            cachePopulationSource.name(),
            LUCENE_FILE_EXTENSION_ATTRIBUTE_KEY,
            luceneFileExt,
            ES_EXECUTOR_ATTRIBUTE_KEY,
            executorName != null ? executorName : NON_ES_EXECUTOR_TO_RECORD
        );
        assert bytesCopied > 0 : "We shouldn't be recording zero-sized copies";
        cachePopulationBytes.incrementBy(bytesCopied, metricAttributes);

        // This is almost certainly paranoid, but if we had a very fast/small copy with a very coarse nanosecond timer it might happen?
        if (copyTimeNanos > 0) {
            cachePopulationThroughput.record(toMebibytesPerSecond(bytesCopied, copyTimeNanos), metricAttributes);
            cachePopulationTime.incrementBy(TimeUnit.NANOSECONDS.toMillis(copyTimeNanos), metricAttributes);
        } else {
            logger.warn("Zero-time copy being reported, ignoring");
        }
    }

    public void recordEpochChange() {
        epochChanges.increment();
    }

    public void recordRead() {
        readCount.increment();
    }

    public void recordMiss() {
        missCount.increment();
    }

    public long readCount() {
        return readCount.sum();
    }

    public long missCount() {
        return missCount.sum();
    }

    /**
     * Calculate throughput as MiB/second
     *
     * @param numberOfBytes The number of bytes transferred
     * @param timeInNanoseconds The time taken to transfer in nanoseconds
     * @return The throughput as MiB/second
     */
    private double toMebibytesPerSecond(int numberOfBytes, long timeInNanoseconds) {
        return ((double) numberOfBytes / timeInNanoseconds) * BYTES_PER_NANOSECONDS_TO_MEBIBYTES_PER_SECOND;
    }
}
