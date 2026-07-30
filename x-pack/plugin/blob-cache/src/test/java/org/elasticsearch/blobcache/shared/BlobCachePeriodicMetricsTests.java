/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BlobCachePeriodicMetricsTests extends ESTestCase {

    public void testStartRegistersMetricAndPublishesFilledCount() throws IOException {
        final int numRegions = randomIntBetween(4, 10);
        final long regionSize = SharedBytes.PAGE_SIZE * 10L;
        final Settings settings = cacheSettings(numRegions, regionSize);
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final TimeValue interval = TimeValue.timeValueMinutes(1);

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording)
            );
            var metrics = new BlobCachePeriodicMetrics(cacheService, taskQueue.getThreadPool(), recording, interval)
        ) {
            metrics.start();
            assertThat(recording.getLongGauge(BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_CURRENT), notNullValue());
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            final var firstMeasurement = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_CURRENT)
                .getLast();
            assertThat(firstMeasurement.getLong(), equalTo(0L));
            assertThat(firstMeasurement.attributes().isEmpty(), equalTo(true));

            for (int i = 0; i < numRegions; i++) {
                final var cacheKey = new TestCacheKey(new ShardId("index", randomUUID(), 0), "file-" + i);
                SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, regionSize - 1, 0);
            }

            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            final var lastMeasurement = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_CURRENT)
                .getLast();
            assertThat(lastMeasurement.getLong(), equalTo((long) numRegions));
            assertThat(lastMeasurement.attributes().isEmpty(), equalTo(true));
        }
    }

    public void testDisabledIntervalDoesNotRegisterMetric() throws IOException {
        final Settings settings = cacheSettings(4, SharedBytes.PAGE_SIZE * 10L);
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final Settings metricsSettings = Settings.builder()
            .put(settings)
            .put(BlobCachePeriodicMetrics.BLOB_CACHE_METRICS_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
            .build();

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording)
            );
            var metrics = new BlobCachePeriodicMetrics(cacheService, metricsSettings, taskQueue.getThreadPool(), recording)
        ) {
            metrics.start();
            assertThat(recording.getLongGauge(BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_CURRENT), nullValue());
        }
    }

    public void testDefaultIntervalDependsOnStateless() {
        assertThat(BlobCachePeriodicMetrics.BLOB_CACHE_METRICS_INTERVAL_SETTING.get(Settings.EMPTY), equalTo(TimeValue.MINUS_ONE));
        assertThat(
            BlobCachePeriodicMetrics.BLOB_CACHE_METRICS_INTERVAL_SETTING.get(
                Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, false).build()
            ),
            equalTo(TimeValue.MINUS_ONE)
        );
        assertThat(
            BlobCachePeriodicMetrics.BLOB_CACHE_METRICS_INTERVAL_SETTING.get(
                Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build()
            ),
            equalTo(TimeValue.timeValueMinutes(5))
        );
    }

    private static Settings cacheSettings(int numRegions, long regionSize) {
        return Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize * numRegions))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
    }

    private record TestCacheKey(ShardId shardId, String file) implements SharedBlobCacheService.KeyBase {}
}
