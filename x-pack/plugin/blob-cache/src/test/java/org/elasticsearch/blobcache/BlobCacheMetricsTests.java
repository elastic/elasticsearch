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
        metrics.recordCachePopulationMetrics(
            Math.toIntExact(ByteSizeValue.ofMb(mebiBytesSent).getBytes()),
            TimeUnit.SECONDS.toNanos(secondsTaken),
            "test-index",
            123,
            BlobCacheMetrics.CachePopulationReason.CacheMiss,
            CachePopulationSource.Peer
        );
        Measurement throughputMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_HISTOGRAM, "es.blob_cache.population.throughput.histogram")
            .get(0);
        assertEquals(throughputMeasurement.getDouble(), (double) mebiBytesSent / secondsTaken, 0.0);
        assertEquals(throughputMeasurement.attributes().get(BlobCacheMetrics.SHARD_ID_ATTRIBUTE_KEY), 123);
        assertEquals(throughputMeasurement.attributes().get(BlobCacheMetrics.INDEX_ATTRIBUTE_KEY), "test-index");
        assertEquals(
            throughputMeasurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_REASON_ATTRIBUTE_KEY),
            BlobCacheMetrics.CachePopulationReason.CacheMiss.name()
        );
        assertEquals(
            throughputMeasurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY),
            CachePopulationSource.Peer.name()
        );
    }

    public void testRecordCachePopulationMetricsRecordsTotalBytes() {
        metrics.recordCachePopulationMetrics(
            Math.toIntExact(ByteSizeValue.ofMb(1).getBytes()),
            TimeUnit.SECONDS.toNanos(1),
            "test-index",
            123,
            BlobCacheMetrics.CachePopulationReason.CacheMiss,
            CachePopulationSource.Peer
        );
        Measurement totalBytesMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.population.bytes.total")
            .get(0);
        assertEquals(totalBytesMeasurement.getLong(), ByteSizeValue.ofMb(1).getBytes());
        assertEquals(totalBytesMeasurement.attributes().get(BlobCacheMetrics.SHARD_ID_ATTRIBUTE_KEY), 123);
        assertEquals(totalBytesMeasurement.attributes().get(BlobCacheMetrics.INDEX_ATTRIBUTE_KEY), "test-index");
        assertEquals(
            totalBytesMeasurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_REASON_ATTRIBUTE_KEY),
            BlobCacheMetrics.CachePopulationReason.CacheMiss.name()
        );
        assertEquals(
            totalBytesMeasurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY),
            CachePopulationSource.Peer.name()
        );
    }

    public void testRecordCachePopulationMetricsRecordsTotalTime() {
        metrics.recordCachePopulationMetrics(
            Math.toIntExact(ByteSizeValue.ofMb(1).getBytes()),
            TimeUnit.SECONDS.toNanos(1),
            "test-index",
            123,
            BlobCacheMetrics.CachePopulationReason.CacheMiss,
            CachePopulationSource.Peer
        );
        Measurement totalTimeMeasurement = recordingMeterRegistry.getRecorder()
            .getMeasurements(InstrumentType.LONG_COUNTER, "es.blob_cache.population.time.total")
            .get(0);
        assertEquals(totalTimeMeasurement.getLong(), TimeUnit.SECONDS.toMillis(1));
        assertEquals(totalTimeMeasurement.attributes().get(BlobCacheMetrics.SHARD_ID_ATTRIBUTE_KEY), 123);
        assertEquals(totalTimeMeasurement.attributes().get(BlobCacheMetrics.INDEX_ATTRIBUTE_KEY), "test-index");
        assertEquals(
            totalTimeMeasurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_REASON_ATTRIBUTE_KEY),
            BlobCacheMetrics.CachePopulationReason.CacheMiss.name()
        );
        assertEquals(
            totalTimeMeasurement.attributes().get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY),
            CachePopulationSource.Peer.name()
        );
    }
}
