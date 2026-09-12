/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.elasticsearch.action.search.TimeRangeBucket;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.time.TimeProvider;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_EVICTION_SCANNED_ENTRIES;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_EVICTION_SCAN_TIME;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_LOCK_ACQUIRE_TIME;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_PREFETCH_TOTAL;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY;
import static org.elasticsearch.blobcache.BlobCacheMetrics.NON_ES_EXECUTOR_TO_RECORD;
import static org.elasticsearch.blobcache.BlobCacheMetrics.PREFETCH_RESULT_ATTRIBUTE_KEY;
import static org.elasticsearch.blobcache.BlobCacheMetrics.REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class BlobCacheMetricsTests extends ESTestCase {

    private RecordingMeterRegistry recordingMeterRegistry;
    private BlobCacheMetrics metrics;
    /** Controllable "now" for deterministic bucket assertions. */
    private final AtomicLong fakeNowMillis = new AtomicLong(System.currentTimeMillis());

    @Before
    public void createMetrics() {
        recordingMeterRegistry = new RecordingMeterRegistry();
        metrics = new BlobCacheMetrics(recordingMeterRegistry, timeProvider(fakeNowMillis));
    }

    private static TimeProvider timeProvider(AtomicLong clock) {
        return new TimeProvider() {
            @Override
            public long relativeTimeInMillis() {
                return clock.get();
            }

            @Override
            public long relativeTimeInNanos() {
                return clock.get() * 1_000_000L;
            }

            @Override
            public long rawRelativeTimeInMillis() {
                return clock.get();
            }

            @Override
            public long absoluteTimeInMillis() {
                return clock.get();
            }
        };
    }

    public void testRecordCachePopulationMetricsRecordsThroughput() {
        int mebiBytesSent = randomIntBetween(1, 4);
        int secondsTaken = randomIntBetween(1, 5);
        BlobCacheMetrics.CachePopulationReason cachePopulationReason = randomFrom(BlobCacheMetrics.CachePopulationReason.values());
        CachePopulationSource cachePopulationSource = randomFrom(CachePopulationSource.values());
        String fileExtension = randomFrom(Arrays.stream(LuceneFilesExtensions.values()).map(LuceneFilesExtensions::getExtension).toList());
        String luceneBlobFile = randomAlphanumericOfLength(15) + "." + fileExtension;
        metrics.recordCachePopulationMetrics(
            luceneBlobFile,
            Math.toIntExact(ByteSizeValue.ofMb(mebiBytesSent).getBytes()),
            TimeUnit.SECONDS.toNanos(secondsTaken),
            cachePopulationReason,
            cachePopulationSource
        );
        String threadName = NON_ES_EXECUTOR_TO_RECORD;

        // throughput histogram
        Measurement throughputMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, "es.blob_cache.population.throughput.histogram")
            .get(0);
        assertEquals(throughputMeasurement.getDouble(), (double) mebiBytesSent / secondsTaken, 0.0);
        assertExpectedAttributesPresent(throughputMeasurement, cachePopulationReason, cachePopulationSource, fileExtension, threadName);

        // bytes counter
        Measurement totalBytesMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.population.bytes.total")
            .get(0);
        assertEquals(totalBytesMeasurement.getLong(), ByteSizeValue.ofMb(mebiBytesSent).getBytes());
        assertExpectedAttributesPresent(totalBytesMeasurement, cachePopulationReason, cachePopulationSource, fileExtension, threadName);

        // time counter
        Measurement totalTimeMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.population.time.total")
            .get(0);
        assertEquals(totalTimeMeasurement.getLong(), TimeUnit.SECONDS.toMillis(secondsTaken));
        assertExpectedAttributesPresent(totalTimeMeasurement, cachePopulationReason, cachePopulationSource, fileExtension, threadName);

        // Use UNKNOWN_TIMESTAMP as a stable timestamp for read/miss tracking in this test.
        // let us check for 0, avoid div by 0.
        checkReadsAndMisses(0, 0, 1);
        int reads = between(1, 100);
        int misses = between(1, reads);
        recordMisses(metrics, misses);
        checkReadsAndMisses(0, misses, misses);
        IntStream.range(0, reads).forEach(i -> metrics.recordRead(SharedBlobCacheService.UNKNOWN_TIMESTAMP));
        checkReadsAndMisses(reads, misses, reads);
        recordMisses(metrics, reads);
        checkReadsAndMisses(reads, misses + reads, misses + reads);
    }

    public void testRecordPrefetch() {
        int alreadyCached = between(0, 5);
        int asyncFetched = between(1, 5);
        int asyncFailed = between(0, asyncFetched);
        IntStream.range(0, alreadyCached).forEach(i -> metrics.recordPrefetch(BlobCacheMetrics.PrefetchResult.AlreadyCached));
        IntStream.range(0, asyncFetched).forEach(i -> metrics.recordPrefetch(BlobCacheMetrics.PrefetchResult.Fetched));
        IntStream.range(0, asyncFailed).forEach(i -> metrics.recordPrefetch(BlobCacheMetrics.PrefetchResult.Failed));

        long observedAlreadyCached = sumPrefetchMeasurementsFor(BlobCacheMetrics.PrefetchResult.AlreadyCached);
        long observedAsyncFetched = sumPrefetchMeasurementsFor(BlobCacheMetrics.PrefetchResult.Fetched);
        long observedAsyncFailed = sumPrefetchMeasurementsFor(BlobCacheMetrics.PrefetchResult.Failed);

        assertEquals(alreadyCached, observedAlreadyCached);
        assertEquals(asyncFetched, observedAsyncFetched);
        assertEquals(asyncFailed, observedAsyncFailed);

        // Each call records exactly one measurement carrying the result attribute
        Measurement first = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, BLOB_CACHE_PREFETCH_TOTAL)
            .stream()
            .findFirst()
            .orElseThrow();
        assertThat(first.attributes().keySet(), contains(PREFETCH_RESULT_ATTRIBUTE_KEY));
    }

    public void testRecordEvictionScan() {
        long elapsedNanos = randomNonNegativeLong();
        long scannedEntries = randomNonNegativeLong();
        BlobCacheMetrics.EvictionScanMode mode = randomFrom(BlobCacheMetrics.EvictionScanMode.values());
        BlobCacheMetrics.EvictionScanOutcome outcome = randomFrom(BlobCacheMetrics.EvictionScanOutcome.values());

        metrics.recordEvictionScan(elapsedNanos, scannedEntries, mode, outcome);

        // the scan-time histogram records the elapsed time as fractional microseconds
        var scanTimeMeasurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, BLOB_CACHE_EVICTION_SCAN_TIME);
        assertThat(scanTimeMeasurements, hasSize(1));
        assertThat(scanTimeMeasurements.getFirst().getDouble(), is(elapsedNanos / 1000.0));
        assertEvictionScanAttributes(scanTimeMeasurements.getFirst(), mode, outcome);

        // the scanned-entries histogram records the raw count
        var scannedEntriesMeasurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_HISTOGRAM, BLOB_CACHE_EVICTION_SCANNED_ENTRIES);
        assertThat(scannedEntriesMeasurements, hasSize(1));
        assertThat(scannedEntriesMeasurements.getFirst().getLong(), is(scannedEntries));
        assertEvictionScanAttributes(scannedEntriesMeasurements.getFirst(), mode, outcome);
    }

    public void testRecordLockAcquire() {
        final long elapsedNanos = randomNonNegativeLong();
        final BlobCacheMetrics.LockAcquireSite site = randomFrom(BlobCacheMetrics.LockAcquireSite.values());

        metrics.recordLockAcquire(elapsedNanos, site);

        final var measurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, BLOB_CACHE_LOCK_ACQUIRE_TIME);
        assertThat(measurements, hasSize(1));
        assertThat(measurements.getFirst().getDouble(), closeTo(elapsedNanos / 1000.0, 1e-9));
        assertThat(measurements.getFirst().attributes().get(LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY), is(site.name()));
        assertThat(measurements.getFirst().attributes().keySet(), contains(LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY));
    }

    public void testSentinelTimestampsRoutedToCorrectBucket() {
        long now = fakeNowMillis.get();

        // Negative sentinels are mapped to "other" directly by resolveBucket.
        assertEquals("other", BlobCacheMetrics.resolveBucket(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP, now));
        assertEquals("other", BlobCacheMetrics.resolveBucket(SharedBlobCacheService.UNKNOWN_TIMESTAMP, now));
        // MINIMAL_CACHE_TIMESTAMP (epoch 0) produces an age equal to now, which far exceeds 14 days.
        assertEquals("older_than_14_days", BlobCacheMetrics.resolveBucket(SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP, now));
        // PRE_TIMESTAMP_FIELD_FALLBACK_MILLIS = Instant.parse("2026-01-01T00:00:00Z") is a real positive timestamp; age > 14 days.
        assertEquals(
            "older_than_14_days",
            BlobCacheMetrics.resolveBucket(java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli(), now)
        );
    }

    public void testTimeRangeBucketsMatchThresholds() {
        long now = fakeNowMillis.get();

        // Each boundary is tested: a timestamp exactly at the threshold falls into that bucket.
        assertEquals("15_minutes", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueMinutes(15).getMillis(), now));
        assertEquals("1_hour", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueHours(1).getMillis(), now));
        assertEquals("12_hours", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueHours(12).getMillis(), now));
        assertEquals("1_day", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueDays(1).getMillis(), now));
        assertEquals("3_days", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueDays(3).getMillis(), now));
        assertEquals("7_days", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueDays(7).getMillis(), now));
        assertEquals("14_days", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueDays(14).getMillis(), now));

        // One millisecond older than the 14-day boundary falls into older_than_14_days.
        assertEquals("older_than_14_days", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueDays(14).getMillis() - 1, now));
        // A very old timestamp also lands in the last bucket.
        assertEquals("older_than_14_days", BlobCacheMetrics.resolveBucket(now - TimeValue.timeValueDays(365).getMillis(), now));
    }

    public void testFutureDatedTimestampBucketedAsFifteenMinutes() {
        long now = fakeNowMillis.get();
        // A timestamp in the future produces a negative age, which falls into the smallest bucket.
        assertEquals("15_minutes", BlobCacheMetrics.resolveBucket(now + 1_000, now));
    }

    public void testReadAndMissGaugeEmitOneObservationPerBucket() {
        long now = fakeNowMillis.get();

        // Record one read and one miss in two different buckets.
        metrics.recordRead(SharedBlobCacheService.UNKNOWN_TIMESTAMP);
        metrics.recordRead(now - TimeValue.timeValueHours(2).getMillis());  // → 12_hours bucket
        metrics.recordMiss(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP);

        collectAndReset();

        List<Measurement> readMeasurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.read.total");

        // Only non-zero buckets are emitted: "other" (UNKNOWN_TIMESTAMP) and "12_hours" (2h-old read).
        assertThat(readMeasurements, hasSize(2));
        for (Measurement m : readMeasurements) {
            assertThat(m.attributes().keySet(), contains(REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY));
        }
        assertEquals(1L, sumBucketValue(readMeasurements, "other"));
        assertEquals(1L, sumBucketValue(readMeasurements, "12_hours"));

        List<Measurement> missMeasurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.miss.total");
        // Only "other" was recorded (BACKFILL_IN_PROGRESS_TIMESTAMP).
        assertThat(missMeasurements, hasSize(1));
        // BACKFILL_IN_PROGRESS_TIMESTAMP routes to "other".
        assertEquals(1L, sumBucketValue(missMeasurements, "other"));
    }

    public void testReadAndMissGaugeTotalsMatchAccessors() {
        long now = fakeNowMillis.get();
        int reads = between(1, 20);
        int misses = between(1, reads);

        // Distribute reads and misses across several buckets.
        IntStream.range(0, reads)
            .forEach(
                i -> metrics.recordRead(
                    i % 2 == 0 ? SharedBlobCacheService.UNKNOWN_TIMESTAMP : now - TimeValue.timeValueDays(1).getMillis()
                )
            );
        IntStream.range(0, misses).forEach(i -> metrics.recordMiss(SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP));

        assertEquals(reads, metrics.readCount());
        assertEquals(misses, metrics.missCount());

        collectAndReset();

        long gaugeReads = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.read.total")
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        long gaugeMisses = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.miss.total")
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();

        assertEquals(reads, gaugeReads);
        assertEquals(misses, gaugeMisses);
    }

    public void testBypassReadSamplesNowOnce() {
        // Advance the clock by 1ms on every absoluteTimeInMillis() sample. A timestamp sitting exactly
        // on the 15-minute boundary would land in 15_minutes on the first sample and 1_hour on the second.
        final AtomicLong clock = new AtomicLong(fakeNowMillis.get());
        final TimeProvider advancingClock = new TimeProvider() {
            @Override
            public long relativeTimeInMillis() {
                return clock.get();
            }

            @Override
            public long relativeTimeInNanos() {
                return clock.get() * 1_000_000L;
            }

            @Override
            public long rawRelativeTimeInMillis() {
                return clock.get();
            }

            @Override
            public long absoluteTimeInMillis() {
                return clock.getAndIncrement();
            }
        };
        final RecordingMeterRegistry registry = new RecordingMeterRegistry();
        final BlobCacheMetrics bypassMetrics = new BlobCacheMetrics(registry, advancingClock);
        final long now = clock.get();
        bypassMetrics.recordBypassRead(now - TimeValue.timeValueMinutes(15).getMillis());

        registry.getRecorder().collect();

        List<Measurement> readMeasurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.read.total");
        List<Measurement> missMeasurements = registry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.miss.total");
        assertEquals(1L, sumBucketValue(readMeasurements, "15_minutes"));
        assertEquals(1L, sumBucketValue(missMeasurements, "15_minutes"));
        assertEquals(0L, sumBucketValue(readMeasurements, "1_hour"));
        assertEquals(0L, sumBucketValue(missMeasurements, "1_hour"));
    }

    private static void assertEvictionScanAttributes(
        Measurement measurement,
        BlobCacheMetrics.EvictionScanMode mode,
        BlobCacheMetrics.EvictionScanOutcome outcome
    ) {
        assertThat(measurement.attributes().get(BlobCacheMetrics.EVICTION_SCAN_MODE_ATTRIBUTE_KEY), is(mode.name()));
        assertThat(measurement.attributes().get(BlobCacheMetrics.EVICTION_SCAN_OUTCOME_ATTRIBUTE_KEY), is(outcome.name()));
        assertThat(
            measurement.attributes().keySet(),
            containsInAnyOrder(BlobCacheMetrics.EVICTION_SCAN_MODE_ATTRIBUTE_KEY, BlobCacheMetrics.EVICTION_SCAN_OUTCOME_ATTRIBUTE_KEY)
        );
    }

    private long sumPrefetchMeasurementsFor(BlobCacheMetrics.PrefetchResult result) {
        return recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, BLOB_CACHE_PREFETCH_TOTAL)
            .stream()
            .filter(m -> result.name().equals(m.attributes().get(PREFETCH_RESULT_ATTRIBUTE_KEY)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private void recordMisses(BlobCacheMetrics blobCacheMetrics, int misses) {
        IntStream.range(0, misses).forEach(i -> blobCacheMetrics.recordMiss(SharedBlobCacheService.UNKNOWN_TIMESTAMP));
    }

    /**
     * Resets the recorder, runs a collect, then asserts the totals across all
     * {@link TimeRangeBucket} buckets (plus {@code "other"}) match the expected values.
     *
     * @param reads       expected total read count (sum across all buckets)
     * @param writes      expected total miss count (sum across all buckets)
     * @param readsForRatio expected denominator for the miss ratio (max(reads, 1))
     */
    private void checkReadsAndMisses(int reads, int writes, int readsForRatio) {
        collectAndReset();

        long totalReads = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.read.total")
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertEquals(reads, totalReads);

        long totalMisses = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, "es.blob_cache.miss.total")
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertEquals(writes, totalMisses);

        Measurement missRatio = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_ASYNC_GAUGE, "es.blob_cache.miss.ratio")
            .getLast();
        assertEquals((double) writes / readsForRatio, missRatio.getDouble(), 0.00000001d);
    }

    /** Resets accumulated measurements, then triggers a fresh collect so subsequent assertions start clean. */
    private void collectAndReset() {
        recordingMeterRegistry.getRecorder().resetCalls();
        recordingMeterRegistry.getRecorder().collect();
    }

    private static long sumBucketValue(List<Measurement> measurements, String bucket) {
        return measurements.stream()
            .filter(m -> bucket.equals(m.attributes().get(REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY)))
            .mapToLong(Measurement::getLong)
            .sum();
    }

    private static void assertExpectedAttributesPresent(
        Measurement measurement,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource,
        String fileExtension,
        String threadName
    ) {
        assertThat(measurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_REASON_ATTRIBUTE_KEY), is(cachePopulationReason.name()));
        assertThat(measurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY), is(cachePopulationSource.name()));
        assertThat(measurement.attributes().get(BlobCacheMetrics.LUCENE_FILE_EXTENSION_ATTRIBUTE_KEY), is(fileExtension));
        assertThat(measurement.attributes().get(BlobCacheMetrics.ES_EXECUTOR_ATTRIBUTE_KEY), is(threadName));
    }
}
