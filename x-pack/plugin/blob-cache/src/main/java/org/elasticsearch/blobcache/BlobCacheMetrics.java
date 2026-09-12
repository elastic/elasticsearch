/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.TimeRangeBucket;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
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
    public static final String BLOB_CACHE_COUNT_OF_EVICTED_USED_REGIONS_TOTAL = "es.blob_cache.count_of_evicted_used_regions.total";
    public static final String BLOB_CACHE_COUNT_OF_EVICTED_REGIONS_TOTAL = "es.blob_cache.count_of_evicted_regions.total";
    public static final String SEARCH_ORIGIN_REMOTE_STORAGE_DOWNLOAD_TOOK_TIME = "es.blob_cache.search_origin.download_took_time.total";
    public static final String BLOB_CACHE_BYPASS_READ_TOTAL = "es.blob_cache.bypass_read.total";
    public static final String BLOB_CACHE_PREFETCH_TOTAL = "es.blob_cache.prefetch.total";
    public static final String PREFETCH_RESULT_ATTRIBUTE_KEY = "es_prefetch_result";
    public static final String BLOB_CACHE_EVICTION_SCAN_TIME = "es.blob_cache.eviction.scan_time.histogram";
    public static final String BLOB_CACHE_EVICTION_SCANNED_ENTRIES = "es.blob_cache.eviction.scanned_entries.histogram";
    public static final String EVICTION_SCAN_MODE_ATTRIBUTE_KEY = "es_eviction_scan_mode";
    public static final String EVICTION_SCAN_OUTCOME_ATTRIBUTE_KEY = "es_eviction_scan_outcome";
    public static final String BLOB_CACHE_LOCK_ACQUIRE_TIME = "es.blob_cache.lock_acquire_time.histogram";
    public static final String LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY = "es_lock_acquire_site";

    /**
     * Attribute key carrying the age-bucket of the cache region's data timestamp relative to now.
     * Possible values are documented on {@link #resolveBucket}.
     *
     * <p><strong>Observability aggregation note:</strong> {@code es.blob_cache.read.total} and
     * {@code es.blob_cache.miss.total} are cumulative async gauges. They emit one observation per
     * {@code timestamp_age} bucket. When aggregating across time in APM/Kibana (e.g. to
     * compute a rate or a total), the values for all buckets within the same collection interval must
     * first be <em>summed</em> to obtain the node-level total, and <em>then</em> averaged/aggregated
     * across nodes.
     */
    public static final String REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY = "timestamp_age";

    private final LongCounter cacheMissCounter;
    private final LongCounter evictedCountNonZeroFrequency;
    private final LongCounter totalEvictedCount;
    private final LongHistogram cacheMissLoadTimes;
    private final DoubleHistogram cachePopulationThroughput;
    private final LongCounter cachePopulationBytes;
    private final LongCounter cachePopulationTime;
    private final LongCounter cacheBypassCounter;
    private final LongCounter prefetchCounter;
    private final DoubleHistogram evictionScanTime;
    private final LongHistogram evictionScannedEntries;
    private final DoubleHistogram lockAcquireTime;

    private final Map<String, LongAdder> missCountByBucket;
    private final Map<String, LongAdder> readCountByBucket;
    private final TimeProvider timeProvider;
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

    /**
     * The outcome of a {@code tryPrefetch} attempt, used as the {@code result} attribute on
     * {@link #BLOB_CACHE_PREFETCH_TOTAL}.
     */
    public enum PrefetchResult {
        AlreadyCached,
        Fetched,
        Failed
    }

    /// The scope of an LFU eviction scan
    public enum EvictionScanMode {
        /// Scan walks only the lowest-frequency LFU list (best-effort prefetch path).
        LowestFrequency,
        /// Scan walks every frequency bucket from lowest to highest until a victim is found or the cache is exhausted.
        AllFrequencies
    }

    /// The outcome of an LFU eviction scan
    public enum EvictionScanOutcome {
        /// Scan evicted a chunk and returned its IO slot.
        Evicted,
        /// Scan was interrupted by a free region appearing in the free-region queue mid-scan.
        /// Currently, can't happen under [EvictionScanMode#LowestFrequency].
        Free,
        /// Scan exhausted its frequency buckets without freeing a region.
        None
    }

    /// The call site at which the SharedBlobCacheService monitor was acquired. Lets us attribute lock-wait time to the
    /// operation requesting the lock so contention can be tracked per code path as eviction work grows.
    public enum LockAcquireSite {
        /// Cache-miss path: scanning the LFU for an eviction victim (maybeEvictAndTake via initChunk).
        CacheMissEviction,
        /// Cache-miss path: assigning a free IO slot to a freshly initialized region (assignToSlot).
        SlotAssignment,
        /// Cache-hit path: promoting a region's frequency on first access within an epoch (maybePromote).
        Promote,
        /// Best-effort prefetch/warming path: lowest-frequency eviction scan (maybeEvictLeastUsed).
        LowestFrequencyEviction,
        /// Bulk eviction via any of the forceEvict methods.
        ForceEvict,
        /// Bulk demotion of a relocated shard's regions to frequency 0 (demoteAll).
        Demote,
        /// Background LFU decay / new-epoch task (computeDecay).
        Decay
    }

    @SuppressWarnings("this-escape")
    public BlobCacheMetrics(MeterRegistry meterRegistry, TimeProvider timeProvider) {
        this(
            meterRegistry.registerLongCounter(
                "es.blob_cache.miss_that_triggered_read.total",
                "The number of times there was a cache miss that triggered a read from the blob store",
                "count"
            ),
            meterRegistry.registerLongCounter(
                BLOB_CACHE_COUNT_OF_EVICTED_USED_REGIONS_TOTAL,
                "The number of cache entries with non-zero frequency evicted under LFU pressure to make room for another region; "
                    + "excludes forced evictions",
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
            ),
            meterRegistry.registerLongCounter(
                BLOB_CACHE_BYPASS_READ_TOTAL,
                "The number of reads that bypassed the cache entirely due to eviction",
                "count"
            ),
            meterRegistry.registerLongCounter(
                BLOB_CACHE_PREFETCH_TOTAL,
                "The number of prefetch attempts, broken down by outcome via the [" + PREFETCH_RESULT_ATTRIBUTE_KEY + "] attribute",
                "count"
            ),
            meterRegistry.registerDoubleHistogram(
                BLOB_CACHE_EVICTION_SCAN_TIME,
                "The time spent scanning the LFU cache for an eviction victim, broken down by ["
                    + EVICTION_SCAN_MODE_ATTRIBUTE_KEY
                    + "] and ["
                    + EVICTION_SCAN_OUTCOME_ATTRIBUTE_KEY
                    + "]",
                "microseconds"
            ),
            meterRegistry.registerLongHistogram(
                BLOB_CACHE_EVICTION_SCANNED_ENTRIES,
                "The number of LFU entries iterated during an eviction scan, broken down by ["
                    + EVICTION_SCAN_MODE_ATTRIBUTE_KEY
                    + "] and ["
                    + EVICTION_SCAN_OUTCOME_ATTRIBUTE_KEY
                    + "]",
                "entries"
            ),
            meterRegistry.registerDoubleHistogram(
                BLOB_CACHE_LOCK_ACQUIRE_TIME,
                "The time spent waiting to acquire the SharedBlobCacheService monitor, broken down by ["
                    + LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY
                    + "]",
                "microseconds"
            ),
            timeProvider
        );

        // notice that this is different from `miss_that_triggered_read` in that `miss_that_triggered_read` will count once per gap
        // filled for a single read. Whereas this one only counts whenever a read provoked populating data from the object store, though
        // once per region for multi-region reads. This allows reasoning about hit ratio too.
        meterRegistry.registerLongsAsyncGauge(
            "es.blob_cache.read.total",
            "The number of cache reads (warming not included), broken down by " + REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY + " attribute.",
            "count",
            () -> observeBuckets(readCountByBucket)
        );
        meterRegistry.registerLongsAsyncGauge(
            "es.blob_cache.miss.total",
            "The number of cache misses (warming not included), broken down by " + REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY + " attribute.",
            "count",
            () -> observeBuckets(missCountByBucket)
        );
        // adding this helps search for high or low miss ratio. It will be since boot of the node though. More advanced queries can use
        // deltas of the totals to see miss ratio over time.
        meterRegistry.registerDoubleAsyncGauge(
            "es.blob_cache.miss.ratio",
            "The fraction of cache reads that missed data (warming not included)",
            "fraction",
            // read misses before reads on purpose
            () -> new DoubleWithAttributes(Math.min((double) missCount() / Math.max(readCount(), 1L), 1.0d))
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
        LongHistogram searchOriginDownloadTime,
        LongCounter cacheBypassCounter,
        LongCounter prefetchCounter,
        DoubleHistogram evictionScanTime,
        LongHistogram evictionScannedEntries,
        DoubleHistogram lockAcquireTime,
        TimeProvider timeProvider
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
        this.cacheBypassCounter = cacheBypassCounter;
        this.prefetchCounter = prefetchCounter;
        this.evictionScanTime = evictionScanTime;
        this.evictionScannedEntries = evictionScannedEntries;
        this.lockAcquireTime = lockAcquireTime;
        this.timeProvider = timeProvider;
        this.readCountByBucket = initBucketMap();
        this.missCountByBucket = initBucketMap();
    }

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

    /**
     * Record a cache read for a region carrying the given data timestamp.
     *
     * @param regionTimestampMillis the representative data timestamp of the region (epoch millis), or one of
     *                              the sentinel values defined in {@code SharedBlobCacheService} which
     *                              are negative and map to the {@code "other"} bucket; non-negative values
     *                              are bucketed by age.
     */
    public void recordRead(long regionTimestampMillis) {
        incrementBucket(readCountByBucket, regionTimestampMillis, timeProvider.absoluteTimeInMillis());
    }

    /**
     * Record a cache miss for a region carrying the given data timestamp.
     *
     * @param regionTimestampMillis see {@link #recordRead(long)}
     */
    public void recordMiss(long regionTimestampMillis) {
        incrementBucket(missCountByBucket, regionTimestampMillis, timeProvider.absoluteTimeInMillis());
    }

    /**
     * Record metrics for a read that bypassed the cache entirely (e.g. due to eviction or no free region).
     * This counts as both a read and a miss, in addition to incrementing the bypass counter.
     *
     * @param regionTimestampMillis see {@link #recordRead(long)}
     */
    public void recordBypassRead(long regionTimestampMillis) {
        final long nowMillis = timeProvider.absoluteTimeInMillis();
        incrementBucket(readCountByBucket, regionTimestampMillis, nowMillis);
        incrementBucket(missCountByBucket, regionTimestampMillis, nowMillis);
        cacheBypassCounter.increment();
    }

    private static void incrementBucket(Map<String, LongAdder> counts, long regionTimestampMillis, long nowMillis) {
        counts.get(resolveBucket(regionTimestampMillis, nowMillis)).increment();
    }

    /**
     * Record the outcome of a prefetch attempt. The {@code result} attribute on the resulting metric allows
     * computing per-outcome rates (e.g. fast-path hit ratio, async failure ratio) without needing separate counters.
     */
    public void recordPrefetch(PrefetchResult result) {
        prefetchCounter.incrementBy(1L, Map.of(PREFETCH_RESULT_ATTRIBUTE_KEY, result.name()));
    }

    /// Record both eviction-scan histograms time taken and entries scanned for a single LFU eviction scan invocation.
    /// @param elapsedNanos elapsed time of the scan in nanoseconds. Recorded as fractional microseconds, which based on APM value buckets,
    /// gives a possible metric range of ~3.9ns to ~131ms
    /// @param scannedEntries number of LFU list iterations performed across all frequency buckets touched
    /// @param mode the scope of the scan (see [EvictionScanMode])
    /// @param outcome whether the scan evicted, got a free region, or exhausted its buckets (see [EvictionScanOutcome])
    public void recordEvictionScan(long elapsedNanos, long scannedEntries, EvictionScanMode mode, EvictionScanOutcome outcome) {
        Map<String, Object> attrs = Map.of(
            EVICTION_SCAN_MODE_ATTRIBUTE_KEY,
            mode.name(),
            EVICTION_SCAN_OUTCOME_ATTRIBUTE_KEY,
            outcome.name()
        );
        evictionScanTime.record((double) elapsedNanos / 1000, attrs); // nanos -> micros
        evictionScannedEntries.record(scannedEntries, attrs);
    }

    /// Record the time spent waiting to acquire the SharedBlobCacheService monitor, attributed by call site.
    /// Contrast with recordEvictionScan, which times work performed while the lock is already held.
    /// @param elapsedNanos wait time between requesting and acquiring the monitor, in nanoseconds (recorded as fractional microseconds)
    /// @param site the operation that acquired the lock (see [LockAcquireSite])
    public void recordLockAcquire(long elapsedNanos, LockAcquireSite site) {
        lockAcquireTime.record((double) elapsedNanos / 1000, Map.of(LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY, site.name()));
    }

    public long readCount() {
        return readCountByBucket.values().stream().mapToLong(LongAdder::sum).sum();
    }

    public long missCount() {
        return missCountByBucket.values().stream().mapToLong(LongAdder::sum).sum();
    }

    /**
     * Resolve the {@link #REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY} bucket for the given region timestamp.
     *
     * <p>Negative sentinel values map to {@code "other"}.
     * All non-negative timestamps are bucketed by age relative to {@code nowMillis}; epoch-zero and
     * pre-field-rollout timestamps land in {@code "older_than_14_days"}.
     */
    static String resolveBucket(long regionTimestampMillis, long nowMillis) {
        if (regionTimestampMillis < 0) {
            return "other";
        }
        return TimeRangeBucket.resolve(nowMillis - regionTimestampMillis);
    }

    private static Map<String, LongAdder> initBucketMap() {
        Map<String, LongAdder> map = new LinkedHashMap<>();
        map.put("other", new LongAdder());
        for (TimeRangeBucket bucket : TimeRangeBucket.values()) {
            map.put(bucket.label(), new LongAdder());
        }
        return map;
    }

    private static Collection<LongWithAttributes> observeBuckets(Map<String, LongAdder> bucketMap) {
        // Zero-count buckets are omitted: these counters are monotonically increasing, so a bucket that
        // is zero has simply never been accessed yet and will enter the stream naturally once it does.
        // Absent observations are treated as implicitly 0 in Observability aggregations, so summing
        // across buckets to get a node-level total is unaffected.
        // As a natural consequence, nodes that never write real timestamps into regions (indexing-tier,
        // non-stateless) only emit the "other" bucket, since sentinel timestamps always route there and
        // the time-range buckets remain perpetually zero.
        List<LongWithAttributes> result = new ArrayList<>(bucketMap.size());
        for (Map.Entry<String, LongAdder> entry : bucketMap.entrySet()) {
            long count = entry.getValue().sum();
            if (count != 0L) {
                result.add(new LongWithAttributes(count, Map.of(REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY, entry.getKey())));
            }
        }
        return result;
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
