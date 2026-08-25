/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.randomRegionTimestampMillis;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCachePeriodicMetrics.METRICS_INTERVAL_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StatelessSharedBlobCachePeriodicMetricsTests extends ESTestCase {

    public void testStartRegistersMetricAndPublishesFilledCount() throws IOException {
        final int numRegions = randomIntBetween(4, 10);
        final long regionSize = SharedBytes.PAGE_SIZE * 10L;
        final TimeValue interval = TimeValue.timeValueMinutes(1);
        final Settings settings = Settings.builder()
            .put(cacheSettings(numRegions, regionSize))
            .put(METRICS_INTERVAL_SETTING.getKey(), interval)
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
            var metrics = new StatelessSharedBlobCachePeriodicMetrics(
                cacheService,
                clusterSettings(settings),
                taskQueue.getThreadPool(),
                recording
            )
        ) {
            metrics.start();
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED), notNullValue());
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL), notNullValue());
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.PROTECTED_METRIC), notNullValue());
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            final var firstFilled = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED)
                .getLast();
            assertThat(firstFilled.getLong(), equalTo(0L));
            assertThat(firstFilled.attributes().isEmpty(), equalTo(true));
            final var firstTotal = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL)
                .getLast();
            assertThat(firstTotal.getLong(), equalTo((long) numRegions));

            for (int i = 0; i < numRegions; i++) {
                final var cacheKey = new TestCacheKey(new ShardId("index", randomUUID(), 0), "file-" + i);
                SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, regionSize - 1, 0, randomRegionTimestampMillis());
            }

            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            final var lastFilled = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED)
                .getLast();
            assertThat(lastFilled.getLong(), equalTo((long) numRegions));
            assertThat(lastFilled.attributes().isEmpty(), equalTo(true));
            final var lastTotal = recording.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL)
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
            .put(METRICS_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
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
            var metrics = new StatelessSharedBlobCachePeriodicMetrics(
                cacheService,
                clusterSettings(metricsSettings),
                taskQueue.getThreadPool(),
                recording
            )
        ) {
            metrics.start();
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED), nullValue());
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL), nullValue());
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.PROTECTED_METRIC), nullValue());
        }
    }

    public void testMetricsIntervalUpdateReschedulesTask() throws IOException {
        final int numRegions = randomIntBetween(4, 10);
        final long regionSize = SharedBytes.PAGE_SIZE * 10L;
        final TimeValue initialInterval = TimeValue.timeValueMinutes(5);
        final Settings settings = Settings.builder()
            .put(cacheSettings(numRegions, regionSize))
            .put(METRICS_INTERVAL_SETTING.getKey(), initialInterval)
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final AtomicInteger sampleCalls = new AtomicInteger();
        final EvictionPolicy<TestCacheKey> countingPolicy = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<TestCacheKey>> createPredicate(CacheRegion<TestCacheKey> incoming) {
                return region -> true;
            }

            @Override
            public void onCached(CacheRegion<TestCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestCacheKey> region) {}

            @Override
            public boolean isProtected(CacheRegion<TestCacheKey> region) {
                sampleCalls.incrementAndGet();
                return false;
            }
        };
        final ClusterSettings clusterSettings = clusterSettings(settings);

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                countingPolicy
            );
            var metrics = new StatelessSharedBlobCachePeriodicMetrics(cacheService, clusterSettings, taskQueue.getThreadPool(), recording)
        ) {
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                new TestCacheKey(new ShardId("index", randomUUID(), 0), "file"),
                regionSize - 1,
                0,
                randomRegionTimestampMillis()
            );

            metrics.start();
            final long t0 = taskQueue.getCurrentTimeMillis();
            // Old 5-minute schedule has not fired yet after 1 minute.
            taskQueue.runTasksUpToTimeInOrder(t0 + TimeValue.timeValueMinutes(1).millis());
            assertThat(sampleCalls.get(), equalTo(0));

            final TimeValue newInterval = TimeValue.timeValueMinutes(1);
            clusterSettings.applySettings(Settings.builder().put(METRICS_INTERVAL_SETTING.getKey(), newInterval).build());

            // First tick of the new schedule.
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + newInterval.millis());
            assertThat(sampleCalls.get(), equalTo(1));

            // Disable sampling before any further ticks, then advance past the original 5-minute deadline.
            clusterSettings.applySettings(Settings.builder().put(METRICS_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
            taskQueue.runTasksUpToTimeInOrder(t0 + initialInterval.millis() + newInterval.millis());
            assertThat(sampleCalls.get(), equalTo(1));
            recording.getRecorder().collect();
            // Publish zeros so a pending unconsumed sample is not retained after sampling is disabled.
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED, 0L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL, 0L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PROTECTED_METRIC, 0L);

            // Re-enable with a different interval.
            final TimeValue reenabledInterval = TimeValue.timeValueSeconds(30);
            clusterSettings.applySettings(Settings.builder().put(METRICS_INTERVAL_SETTING.getKey(), reenabledInterval).build());
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED), notNullValue());
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + reenabledInterval.millis());
            assertThat(sampleCalls.get(), equalTo(2));
        }
    }

    public void testUnsetMetricsIntervalTracksEvictionPolicyDynamically() throws IOException {
        final int numRegions = randomIntBetween(4, 10);
        final long regionSize = SharedBytes.PAGE_SIZE * 10L;
        // Leave metrics_interval unset so its default follows the eviction-policy setting.
        final Settings settings = Settings.builder()
            .put(cacheSettings(numRegions, regionSize))
            .put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.ALWAYS
            )
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final AtomicInteger sampleCalls = new AtomicInteger();
        final EvictionPolicy<TestCacheKey> countingPolicy = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<TestCacheKey>> createPredicate(CacheRegion<TestCacheKey> incoming) {
                return region -> true;
            }

            @Override
            public void onCached(CacheRegion<TestCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestCacheKey> region) {}

            @Override
            public boolean isProtected(CacheRegion<TestCacheKey> region) {
                sampleCalls.incrementAndGet();
                return false;
            }
        };
        final ClusterSettings clusterSettings = clusterSettings(settings);

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                new BlobCacheMetrics(recording),
                countingPolicy
            );
            var metrics = new StatelessSharedBlobCachePeriodicMetrics(cacheService, clusterSettings, taskQueue.getThreadPool(), recording)
        ) {
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                new TestCacheKey(new ShardId("index", randomUUID(), 0), "file"),
                regionSize - 1,
                0,
                randomRegionTimestampMillis()
            );

            metrics.start();
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED), nullValue());
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + TimeValue.timeValueMinutes(5).millis());
            assertThat(sampleCalls.get(), equalTo(0));

            // Switching to PINNED_WINDOW re-evaluates the unset metrics_interval default to 3m and starts sampling.
            clusterSettings.applySettings(
                Settings.builder()
                    .put(
                        StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.PINNED_WINDOW
                    )
                    .build()
            );
            assertThat(recording.getLongGauge(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED), notNullValue());
            final TimeValue pinnedDefault = TimeValue.timeValueMinutes(3);
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + pinnedDefault.millis());
            assertThat(sampleCalls.get(), equalTo(1));
            recording.getRecorder().collect();
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED, 1L);

            // Switching away from PINNED_WINDOW disables sampling again and clears gauges.
            clusterSettings.applySettings(
                Settings.builder()
                    .put(
                        StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.ALWAYS
                    )
                    .build()
            );
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + pinnedDefault.millis());
            assertThat(sampleCalls.get(), equalTo(1));
            recording.getRecorder().collect();
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED, 0L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_TOTAL, 0L);
        }
    }

    public void testDefaultIntervalDependsOnPinnedWindowPolicy() {
        assertThat(METRICS_INTERVAL_SETTING.get(Settings.EMPTY), equalTo(TimeValue.MINUS_ONE));
        assertThat(
            METRICS_INTERVAL_SETTING.get(
                Settings.builder()
                    .put(
                        StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.ALWAYS
                    )
                    .build()
            ),
            equalTo(TimeValue.MINUS_ONE)
        );
        assertThat(
            METRICS_INTERVAL_SETTING.get(
                Settings.builder()
                    .put(
                        StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.INDEX_AGE
                    )
                    .build()
            ),
            equalTo(TimeValue.MINUS_ONE)
        );
        assertThat(
            METRICS_INTERVAL_SETTING.get(
                Settings.builder()
                    .put(
                        StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.PINNED_WINDOW
                    )
                    .build()
            ),
            equalTo(TimeValue.timeValueMinutes(3))
        );
        // Explicit metrics interval wins over the pinned-window-dependent default.
        assertThat(
            METRICS_INTERVAL_SETTING.get(
                Settings.builder()
                    .put(
                        StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.PINNED_WINDOW
                    )
                    .put(METRICS_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                    .build()
            ),
            equalTo(TimeValue.MINUS_ONE)
        );
        assertThat(METRICS_INTERVAL_SETTING.getKey(), equalTo("stateless.cache.metrics_interval"));
        assertTrue(METRICS_INTERVAL_SETTING.isDynamic());
    }

    public void testMetricsIntervalRejectsBelowMinimum() {
        final TimeValue tooSmall = TimeValue.timeValueMillis(
            randomLongBetween(0, StatelessSharedBlobCachePeriodicMetrics.MIN_METRICS_INTERVAL.millis() - 1)
        );
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> METRICS_INTERVAL_SETTING.get(Settings.builder().put(METRICS_INTERVAL_SETTING.getKey(), tooSmall).build())
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "failed to parse value ["
                    + tooSmall.getStringRep()
                    + "] for setting ["
                    + METRICS_INTERVAL_SETTING.getKey()
                    + "], must be ["
                    + TimeValue.MINUS_ONE.getStringRep()
                    + "] to disable or >= ["
                    + StatelessSharedBlobCachePeriodicMetrics.MIN_METRICS_INTERVAL.getStringRep()
                    + "]"
            )
        );
        assertThat(
            METRICS_INTERVAL_SETTING.get(
                Settings.builder()
                    .put(METRICS_INTERVAL_SETTING.getKey(), StatelessSharedBlobCachePeriodicMetrics.MIN_METRICS_INTERVAL)
                    .build()
            ),
            equalTo(StatelessSharedBlobCachePeriodicMetrics.MIN_METRICS_INTERVAL)
        );
        assertThat(
            METRICS_INTERVAL_SETTING.get(Settings.builder().put(METRICS_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build()),
            equalTo(TimeValue.MINUS_ONE)
        );
    }

    public void testSamplePublishesProtectionGaugesViaIsProtected() throws IOException {
        final int numRegions = randomIntBetween(5, 10);
        final long regionSize = SharedBytes.PAGE_SIZE * 10L;
        final TimeValue interval = TimeValue.timeValueMinutes(1);
        final Settings settings = Settings.builder()
            .put(cacheSettings(numRegions, regionSize))
            .put(METRICS_INTERVAL_SETTING.getKey(), interval)
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final RecordingMeterRegistry recording = new RecordingMeterRegistry();
        final AtomicBoolean protectAll = new AtomicBoolean(true);
        final EvictionPolicy<TestCacheKey> evictionPolicy = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<TestCacheKey>> createPredicate(CacheRegion<TestCacheKey> incoming) {
                return region -> true;
            }

            @Override
            public void onCached(CacheRegion<TestCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<TestCacheKey> region) {}

            @Override
            public boolean isProtected(CacheRegion<TestCacheKey> region) {
                return protectAll.get();
            }
        };

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
            var metrics = new StatelessSharedBlobCachePeriodicMetrics(
                cacheService,
                clusterSettings(settings),
                taskQueue.getThreadPool(),
                recording
            )
        ) {
            final ShardId shardId = new ShardId("index", randomUUID(), 0);
            final long now = Math.max(taskQueue.getCurrentTimeMillis(), TimeValue.timeValueDays(1).millis());
            SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, new TestCacheKey(shardId, "inside"), regionSize - 1, 0, now);
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                new TestCacheKey(shardId, "unknown"),
                regionSize - 1,
                0,
                UNKNOWN_TIMESTAMP
            );
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                new TestCacheKey(shardId, "backfill"),
                regionSize - 1,
                0,
                BACKFILL_IN_PROGRESS_TIMESTAMP
            );
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                new TestCacheKey(shardId, "minimal"),
                regionSize - 1,
                0,
                SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP
            );
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                new TestCacheKey(shardId, "pre_timestamp_field"),
                regionSize - 1,
                0,
                SearchDirectory.PRE_TIMESTAMP_FIELD_FALLBACK_MILLIS
            );

            metrics.start();
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED, 5L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PROTECTED_METRIC, 5L);
            // Fresh LFU entries start at frequency 1, so all protected regions land in the positive-freq bucket.
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PROTECTED_FREQ_0_METRIC, 0L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PROTECTED_FREQ_POSITIVE_METRIC, 5L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.UNKNOWN_METRIC, 1L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BACKFILL_METRIC, 1L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.MINIMAL_METRIC, 1L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PRE_TIMESTAMP_FIELD_METRIC, 1L);

            protectAll.set(false);
            taskQueue.runTasksUpToTimeInOrder(taskQueue.getCurrentTimeMillis() + interval.millis());
            recording.getRecorder().collect();
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED, 5L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PROTECTED_METRIC, 0L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PROTECTED_FREQ_0_METRIC, 0L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PROTECTED_FREQ_POSITIVE_METRIC, 0L);
            // Timestamp-special occupancy is independent of protection.
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.UNKNOWN_METRIC, 1L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.BACKFILL_METRIC, 1L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.MINIMAL_METRIC, 1L);
            assertGauge(recording, StatelessSharedBlobCachePeriodicMetrics.PRE_TIMESTAMP_FIELD_METRIC, 1L);
        }
    }

    private static void assertGauge(RecordingMeterRegistry recording, String name, long expected) {
        final var measurement = recording.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, name).getLast();
        assertThat(measurement.getLong(), equalTo(expected));
        assertThat(measurement.attributes().isEmpty(), equalTo(true));
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(
            settings,
            Sets.union(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                Set.of(
                    METRICS_INTERVAL_SETTING,
                    StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING
                )
            )
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
