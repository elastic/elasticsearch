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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class BlobCachePeriodicMetricsTests extends ESTestCase {

    public void testStartRegistersMetricAndPublishesFilledCount() throws IOException {
        final int numRegions = randomIntBetween(4, 10);
        final long regionSize = SharedBytes.PAGE_SIZE * 10L;
        final TimeValue interval = TimeValue.timeValueMinutes(1);
        final Settings settings = Settings.builder()
            .put(cacheSettings(numRegions, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.getKey(), interval)
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<TestCacheKey>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording)
            );
            var metrics = new BlobCachePeriodicMetrics(cacheService, settings, taskQueue.getThreadPool(), recording)
        ) {
            metrics.start();
            assertThat(recording.getLongGauge(BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED), notNullValue());
            assertThat(recording.getLongGauge(BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL), notNullValue());
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            final var firstFilled = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED)
                .getLast();
            assertThat(firstFilled.getLong(), equalTo(0L));
            assertThat(firstFilled.attributes().isEmpty(), equalTo(true));
            final var firstTotal = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL)
                .getLast();
            assertThat(firstTotal.getLong(), equalTo((long) numRegions));

            for (int i = 0; i < numRegions; i++) {
                final var cacheKey = new TestCacheKey(new ShardId("index", randomUUID(), 0), "file-" + i);
                SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, regionSize - 1, 0);
            }

            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            final var lastFilled = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED)
                .getLast();
            assertThat(lastFilled.getLong(), equalTo((long) numRegions));
            assertThat(lastFilled.attributes().isEmpty(), equalTo(true));
            final var lastTotal = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL)
                .getLast();
            assertThat(lastTotal.getLong(), equalTo((long) numRegions));
        }
    }

    public void testDisabledIntervalDoesNotRegisterMetric() throws IOException {
        final Settings settings = cacheSettings(4, SharedBytes.PAGE_SIZE * 10L);
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final Settings metricsSettings = Settings.builder()
            .put(settings)
            .put(SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
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
            assertThat(recording.getLongGauge(BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED), nullValue());
            assertThat(recording.getLongGauge(BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL), nullValue());
        }
    }

    public void testDefaultIntervalDependsOnStateless() {
        assertThat(SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.get(Settings.EMPTY), equalTo(TimeValue.MINUS_ONE));
        assertThat(
            SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.get(
                Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, false).build()
            ),
            equalTo(TimeValue.MINUS_ONE)
        );
        assertThat(
            SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.get(
                Settings.builder().put(DiscoveryNode.STATELESS_ENABLED_SETTING_NAME, true).build()
            ),
            equalTo(TimeValue.timeValueMinutes(3))
        );
    }

    public void testMetricsIntervalRejectsBelowMinimum() {
        final TimeValue tooSmall = TimeValue.timeValueMillis(
            randomLongBetween(0, SharedBlobCacheService.MIN_SHARED_CACHE_METRICS_INTERVAL.millis() - 1)
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.get(
                Settings.builder().put(SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.getKey(), tooSmall).build()
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "failed to parse value ["
                    + tooSmall.getStringRep()
                    + "] for setting ["
                    + SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.getKey()
                    + "], must be ["
                    + TimeValue.MINUS_ONE.getStringRep()
                    + "] to disable or >= ["
                    + SharedBlobCacheService.MIN_SHARED_CACHE_METRICS_INTERVAL.getStringRep()
                    + "]"
            )
        );
        assertThat(
            SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.get(
                Settings.builder()
                    .put(
                        SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.getKey(),
                        SharedBlobCacheService.MIN_SHARED_CACHE_METRICS_INTERVAL
                    )
                    .build()
            ),
            equalTo(SharedBlobCacheService.MIN_SHARED_CACHE_METRICS_INTERVAL)
        );
        assertThat(
            SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.get(
                Settings.builder().put(SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build()
            ),
            equalTo(TimeValue.MINUS_ONE)
        );
    }

    public void testSampleInvokesEvictionPolicyUpdatePeriodicMetrics() throws IOException {
        final int numRegions = randomIntBetween(4, 10);
        final long regionSize = SharedBytes.PAGE_SIZE * 10L;
        final TimeValue interval = TimeValue.timeValueMinutes(1);
        final Settings settings = Settings.builder()
            .put(cacheSettings(numRegions, regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_METRICS_INTERVAL_SETTING.getKey(), interval)
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final RecordingEvictionPolicy<TestCacheKey> evictionPolicy = new RecordingEvictionPolicy<>();

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                evictionPolicy
            );
            var metrics = new BlobCachePeriodicMetrics(cacheService, settings, taskQueue.getThreadPool(), recording)
        ) {
            final int occupied = randomIntBetween(1, numRegions);
            for (int i = 0; i < occupied; i++) {
                final var cacheKey = new TestCacheKey(new ShardId("index", randomUUID(), 0), "file-" + i);
                SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, regionSize - 1, 0);
            }

            metrics.start();
            assertThat(evictionPolicy.updateCalls.get(), equalTo(0));
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            assertThat(evictionPolicy.updateCalls.get(), equalTo(1));
            assertThat(evictionPolicy.regionsSeen.get(), equalTo(occupied));
            recording.getRecorder().collect();
            final var filled = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, BlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED)
                .getLast();
            assertThat(filled.getLong(), equalTo((long) occupied));
        }
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

    private static final class RecordingEvictionPolicy<KeyType extends SharedBlobCacheService.KeyBase> implements EvictionPolicy<KeyType> {
        private final AtomicInteger updateCalls = new AtomicInteger();
        private final AtomicInteger regionsSeen = new AtomicInteger();

        @Override
        public Predicate<CacheRegion<KeyType>> createPredicate(CacheRegion<KeyType> incoming) {
            return region -> true;
        }

        @Override
        public void onCached(CacheRegion<KeyType> region) {}

        @Override
        public void onEvicted(CacheRegion<KeyType> region) {}

        @Override
        public void updatePeriodicMetrics(Consumer<BiConsumer<CacheRegion<KeyType>, Integer>> regions) {
            updateCalls.incrementAndGet();
            regions.accept((region, freq) -> regionsSeen.incrementAndGet());
        }
    }
}
