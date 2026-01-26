/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.elasticsearch.blobcache.BlobCacheMetrics.NON_ES_EXECUTOR_TO_RECORD;
import static org.hamcrest.Matchers.is;

public class BlobCacheMetricsTests extends ESTestCase {

    private RecordingMeterRegistry recordingMeterRegistry;
    private BlobCacheMetrics metrics;

    @Before
    public void createMetrics() {
        recordingMeterRegistry = new RecordingMeterRegistry();
        metrics = new BlobCacheMetrics(recordingMeterRegistry);
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

        // let us check for 0, avoid div by 0.
        checkReadsAndMisses(0, 0, 1);
        int reads = between(1, 100);
        int misses = between(1, reads);
        recordMisses(metrics, misses);
        checkReadsAndMisses(0, misses, misses);
        IntStream.range(0, reads).forEach(i -> metrics.recordRead());
        checkReadsAndMisses(reads, misses, reads);
        recordMisses(metrics, reads);
        checkReadsAndMisses(reads, misses + reads, misses + reads);
    }

    private void recordMisses(BlobCacheMetrics metrics, int misses) {
        IntStream.range(0, misses).forEach(i -> metrics.recordMiss());
    }

    private void checkReadsAndMisses(int reads, int writes, int readsForRatio) {
        recordingMeterRegistry.getRecorder().collect();

        Measurement totalReadsMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, "es.blob_cache.read.total")
            .getLast();
        assertEquals(reads, totalReadsMeasurement.getLong());

        Measurement totalMissesMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_GAUGE, "es.blob_cache.miss.total")
            .getLast();
        assertEquals(writes, totalMissesMeasurement.getLong());

        Measurement missRatio = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, "es.blob_cache.miss.ratio")
            .getLast();
        assertEquals((double) writes / readsForRatio, missRatio.getDouble(), 0.00000001d);
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
