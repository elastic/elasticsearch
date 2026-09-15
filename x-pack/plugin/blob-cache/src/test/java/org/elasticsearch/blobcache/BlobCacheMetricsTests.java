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
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_MISS_AGE;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_MISS_TOTAL;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_PREFETCH_TOTAL;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_READ_AGE;
import static org.elasticsearch.blobcache.BlobCacheMetrics.BLOB_CACHE_READ_TOTAL;
import static org.elasticsearch.blobcache.BlobCacheMetrics.LOCK_ACQUIRE_SITE_ATTRIBUTE_KEY;
import static org.elasticsearch.blobcache.BlobCacheMetrics.NON_ES_EXECUTOR_TO_RECORD;
import static org.elasticsearch.blobcache.BlobCacheMetrics.PREFETCH_RESULT_ATTRIBUTE_KEY;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class BlobCacheMetricsTests extends ESTestCase {

    private RecordingMeterRegistry recordingMeterRegistry;
    private BlobCacheMetrics metrics;
    /** Controllable "now" for deterministic age assertions. */
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

    public void testSentinelTimestampsSkipAgeHistogram() {
        metrics.recordRead(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP);
        metrics.recordMiss(SharedBlobCacheService.UNKNOWN_TIMESTAMP);

        assertThat(ageMeasurements(BLOB_CACHE_READ_AGE), empty());
        assertThat(ageMeasurements(BLOB_CACHE_MISS_AGE), empty());
        assertEquals(1L, metrics.readCount());
        assertEquals(1L, metrics.missCount());

        collectAndReset();
        assertEquals(1L, gaugeValue(BLOB_CACHE_READ_TOTAL));
        assertEquals(1L, gaugeValue(BLOB_CACHE_MISS_TOTAL));
    }

    public void testKnownTimestampsRecordAgeHistogram() {
        long now = fakeNowMillis.get();

        metrics.recordRead(SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP);
        metrics.recordRead(now - TimeValue.timeValueHours(2).getMillis());
        metrics.recordMiss(java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli());

        List<Measurement> readAges = ageMeasurements(BLOB_CACHE_READ_AGE);
        assertThat(readAges, hasSize(2));
        assertEquals(now - SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP, readAges.get(0).getLong());
        assertEquals(TimeValue.timeValueHours(2).getMillis(), readAges.get(1).getLong());

        List<Measurement> missAges = ageMeasurements(BLOB_CACHE_MISS_AGE);
        assertThat(missAges, hasSize(1));
        assertEquals(now - java.time.Instant.parse("2026-01-01T00:00:00Z").toEpochMilli(), missAges.getFirst().getLong());
    }

    public void testAgeHistogramRecordsThresholdAges() {
        long now = fakeNowMillis.get();
        for (long boundary : TimeRangeBucket.histogramBoundaries()) {
            metrics.recordRead(now - boundary);
        }
        List<Long> recorded = ageMeasurements(BLOB_CACHE_READ_AGE).stream().map(Measurement::getLong).toList();
        assertEquals(TimeRangeBucket.histogramBoundaries(), recorded);

        metrics.recordRead(now - TimeValue.timeValueDays(14).getMillis() - 1);
        metrics.recordRead(now - TimeValue.timeValueDays(365).getMillis());
        recorded = ageMeasurements(BLOB_CACHE_READ_AGE).stream().map(Measurement::getLong).toList();
        assertEquals(TimeValue.timeValueDays(14).getMillis() + 1, recorded.get(recorded.size() - 2).longValue());
        assertEquals(TimeValue.timeValueDays(365).getMillis(), recorded.getLast().longValue());
    }

    public void testFutureDatedTimestampRecordsNegativeAge() {
        long now = fakeNowMillis.get();
        metrics.recordRead(now + 1_000);
        List<Measurement> readAges = ageMeasurements(BLOB_CACHE_READ_AGE);
        assertThat(readAges, hasSize(1));
        assertEquals(-1_000L, readAges.getFirst().getLong());
    }

    public void testGaugesEmitOneUnattributedObservation() {
        long now = fakeNowMillis.get();

        metrics.recordRead(SharedBlobCacheService.UNKNOWN_TIMESTAMP);
        metrics.recordRead(now - TimeValue.timeValueHours(2).getMillis());
        metrics.recordMiss(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP);

        assertThat(ageMeasurements(BLOB_CACHE_READ_AGE), hasSize(1));
        assertEquals(TimeValue.timeValueHours(2).getMillis(), ageMeasurements(BLOB_CACHE_READ_AGE).getFirst().getLong());
        assertThat(ageMeasurements(BLOB_CACHE_MISS_AGE), empty());

        collectAndReset();

        List<Measurement> readMeasurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, BLOB_CACHE_READ_TOTAL);
        assertThat(readMeasurements, hasSize(1));
        assertThat(readMeasurements.getFirst().attributes().keySet(), empty());
        assertEquals(2L, readMeasurements.getFirst().getLong());

        List<Measurement> missMeasurements = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, BLOB_CACHE_MISS_TOTAL);
        assertThat(missMeasurements, hasSize(1));
        assertThat(missMeasurements.getFirst().attributes().keySet(), empty());
        assertEquals(1L, missMeasurements.getFirst().getLong());
    }

    public void testReadAndMissGaugeTotalsMatchAccessors() {
        long now = fakeNowMillis.get();
        int reads = between(1, 20);
        int misses = between(1, reads);

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
        assertEquals(reads, gaugeValue(BLOB_CACHE_READ_TOTAL));
        assertEquals(misses, gaugeValue(BLOB_CACHE_MISS_TOTAL));
    }

    public void testBypassReadSamplesNowOnce() {
        // Advance the clock by 1ms on every absoluteTimeInMillis() sample. Sampling twice would
        // record read age 15 minutes and miss age 15 minutes + 1ms.
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
        final long expectedAge = TimeValue.timeValueMinutes(15).getMillis();
        bypassMetrics.recordBypassRead(now - expectedAge);

        List<Measurement> readAges = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, BLOB_CACHE_READ_AGE);
        List<Measurement> missAges = registry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, BLOB_CACHE_MISS_AGE);
        assertThat(readAges, hasSize(1));
        assertThat(missAges, hasSize(1));
        assertEquals(expectedAge, readAges.getFirst().getLong());
        assertEquals(expectedAge, missAges.getFirst().getLong());
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
     * Resets the recorder, runs a collect, then asserts the unattributed read/miss totals and miss ratio.
     *
     * @param reads       expected total read count
     * @param writes      expected total miss count
     * @param readsForRatio expected denominator for the miss ratio (max(reads, 1))
     */
    private void checkReadsAndMisses(int reads, int writes, int readsForRatio) {
        collectAndReset();
        assertEquals(reads, gaugeValue(BLOB_CACHE_READ_TOTAL));
        assertEquals(writes, gaugeValue(BLOB_CACHE_MISS_TOTAL));

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

    private long gaugeValue(String metricName) {
        return recordingMeterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_GAUGE, metricName).getLast().getLong();
    }

    private List<Measurement> ageMeasurements(String metricName) {
        return recordingMeterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, metricName);
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
