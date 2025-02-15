/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

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
        metrics.recordCachePopulationMetrics(
            Math.toIntExact(ByteSizeValue.ofMb(mebiBytesSent).getBytes()),
            TimeUnit.SECONDS.toNanos(secondsTaken),
            cachePopulationReason,
            cachePopulationSource
        );

        // throughput histogram
        Measurement throughputMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, "es.blob_cache.population.throughput.histogram")
            .get(0);
        assertEquals(throughputMeasurement.getDouble(), (double) mebiBytesSent / secondsTaken, 0.0);
        assertExpectedAttributesPresent(throughputMeasurement, cachePopulationReason, cachePopulationSource);

        // bytes counter
        Measurement totalBytesMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.population.bytes.total")
            .get(0);
        assertEquals(totalBytesMeasurement.getLong(), ByteSizeValue.ofMb(mebiBytesSent).getBytes());
        assertExpectedAttributesPresent(totalBytesMeasurement, cachePopulationReason, cachePopulationSource);

        // time counter
        Measurement totalTimeMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.population.time.total")
            .get(0);
        assertEquals(totalTimeMeasurement.getLong(), TimeUnit.SECONDS.toMillis(secondsTaken));
        assertExpectedAttributesPresent(totalTimeMeasurement, cachePopulationReason, cachePopulationSource);
    }

    private static void assertExpectedAttributesPresent(
        Measurement measurement,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource
    ) {
        assertEquals(measurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_REASON_ATTRIBUTE_KEY), cachePopulationReason.name());
        assertEquals(measurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY), cachePopulationSource.name());
    }
}
