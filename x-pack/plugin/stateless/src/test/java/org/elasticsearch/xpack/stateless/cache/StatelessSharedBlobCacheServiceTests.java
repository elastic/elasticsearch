/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.freeRegionCountFromCacheService;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.getFromCacheService;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.maybeEvictLeastUsedFromCacheService;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.maybeScheduleDecayAndNewEpochForCacheService;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.TestUtils.newCacheService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class StatelessSharedBlobCacheServiceTests extends ESTestCase {

    public void testDemoteAllSkippedWhenShardLocallyAllocatedAtTaskExecution() throws IOException {
        runDemoteAllTest(true, false);
    }

    public void testDemoteAllWhenShardNotLocallyAllocatedAtTaskExecution() throws IOException {
        runDemoteAllTest(false, true);
    }

    public void testEvictionDegradationTriggersOnExcessiveRejections() throws Exception {
        final int numRegions = randomIntBetween(4, 20);
        final long regionSize = cacheRegionSizeInBytes(1L);
        // The default 95% threshold is always crossed when the never-evict policy rejects all numRegions entries
        final var settingBuilder = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheRegionSizeInBytes(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir());
        // Sometimes configure the defaults explicitly
        if (randomBoolean()) {
            settingBuilder.put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.getKey(),
                "95%"
            );
        }
        if (randomBoolean()) {
            settingBuilder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_PERIOD_SETTING.getKey(), "5m");
        }
        final Settings settings = settingBuilder.build();
        final AtomicInteger policyCallCount = new AtomicInteger(0);
        final var evicted = new AtomicBoolean(false);
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final EvictionPolicy<FileCacheKey> neverEvict = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
                return region -> {
                    policyCallCount.incrementAndGet();
                    return false;
                };
            }

            @Override
            public void onCached(CacheRegion<FileCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<FileCacheKey> region) {
                evicted.set(true);
            }
        };
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                taskQueue.getThreadPool(),
                new BlobCacheMetrics(new RecordingMeterRegistry()),
                neverEvict,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
            // the never-evict policy rejects all regions, crossing the threshold and triggering degradation
            final boolean decayed = fillAndMaybeDecay(cacheService, taskQueue);
            evictRandomly(cacheService, regionSize, decayed);
            // We have up to 20 regions, and its degradation threshold of 95% is 19 regions. So the degradation
            // kicks in on the 20th region, which means the policy is called numRegions times.
            assertThat(policyCallCount.get(), equalTo(numRegions));
            assertTrue(evicted.get());
        }
    }

    public void testEvictionDegradationPeriodLifecycle() throws Exception {
        final int numRegions = randomIntBetween(4, 20);
        final long regionSize = cacheRegionSizeInBytes(1L);
        final long degradationPeriodMillis = TimeUnit.SECONDS.toMillis(10);
        // threshold of 50%: degradation triggers after numRegions/2 rejections
        final int expectedThreshold = (int) (numRegions * 0.5);
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheRegionSizeInBytes(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.getKey(), "50%")
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_PERIOD_SETTING.getKey(), "10s")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

        // policy that always rejects eviction and records how many times its predicate is called
        final AtomicInteger policyCallCount = new AtomicInteger(0);
        final EvictionPolicy<FileCacheKey> countingNeverEvict = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
                return region -> {
                    policyCallCount.incrementAndGet();
                    return false;
                };
            }

            @Override
            public void onCached(CacheRegion<FileCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<FileCacheKey> region) {}
        };
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                taskQueue.getThreadPool(),
                new BlobCacheMetrics(new RecordingMeterRegistry()),
                countingNeverEvict,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {

            boolean decayed = fillAndMaybeDecay(cacheService, taskQueue);

            // scan 1 (time=0): policy consulted until threshold, degradation starts
            evictRandomly(cacheService, regionSize, decayed);
            assertThat(policyCallCount.get(), equalTo(expectedThreshold + 1));
            decayed = fillAndMaybeDecay(cacheService, taskQueue);
            policyCallCount.set(0);

            // scan 2 (time=9999ms): period active, policy bypassed
            taskQueue.runTasksUpToTimeInOrder(degradationPeriodMillis - 1);
            evictRandomly(cacheService, regionSize, decayed);
            assertThat(policyCallCount.get(), equalTo(0));
            decayed = fillAndMaybeDecay(cacheService, taskQueue);
            policyCallCount.set(0);

            // scan 3 (time=10001ms): period expired, policy consulted again
            taskQueue.runTasksUpToTimeInOrder(degradationPeriodMillis + 1);
            evictRandomly(cacheService, regionSize, decayed);
            assertThat(policyCallCount.get(), greaterThan(0));
        }
    }

    public void testEvictionDegradationShortCircuitsSubsequentPolicyChecks() throws Exception {
        // In a single eviction scan, when degradation is triggered, we set degradation startMillis and log a warning message once.
        // Subsequent calls to the policy predicate for other regions in the same scan is skipped.
        // For simplicity, the degradation threshold is set to 0 so that it is triggered immediately on the first rejection.
        // There are two regions, we incref the 1st region to simulate an active reader so that its IO cannot be released which
        // forces the scan to continue on the 2nd region for eviction. This time, the policy predicate is skipped since we are
        // already in degradation mode.
        final int numRegions = 2;
        final long regionSize = cacheRegionSizeInBytes(1L);
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheRegionSizeInBytes(numRegions)))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.getKey(), "0%")
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_PERIOD_SETTING.getKey(), "5m")
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

        final AtomicInteger policyCallCount = new AtomicInteger(0);
        final EvictionPolicy<FileCacheKey> neverEvict = new EvictionPolicy<>() {
            @Override
            public Predicate<CacheRegion<FileCacheKey>> createPredicate(CacheRegion<FileCacheKey> incoming) {
                return region -> {
                    policyCallCount.incrementAndGet();
                    return false;
                };
            }

            @Override
            public void onCached(CacheRegion<FileCacheKey> region) {}

            @Override
            public void onEvicted(CacheRegion<FileCacheKey> region) {}
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                taskQueue.getThreadPool(),
                new BlobCacheMetrics(new RecordingMeterRegistry()),
                neverEvict,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
            getFromCacheService(cacheService, generateFileCacheKey(), regionSize, 0);
            // This is the 1st region since the entry is inserted at the head of freq list.
            final RefCounted firstRegion = getFromCacheService(cacheService, generateFileCacheKey(), regionSize, 0);
            // Decay synchronously (DecayAndNewEpochTask uses DIRECT_EXECUTOR_SERVICE, which runs tasks inline).
            final boolean decayed = randomBoolean();
            if (decayed) {
                maybeScheduleDecayAndNewEpochForCacheService(cacheService);
            }
            taskQueue.runAllRunnableTasks();
            firstRegion.mustIncRef(); // incref to force scan to pick the 2nd region

            // We should observe the warning log only once and policy predicate is also called only once
            final var seenLoggingOnce = new AtomicBoolean(false);
            final var cacheServiceLogger = LogManager.getLogger(StatelessSharedBlobCacheService.class);
            final var mockAppender = new AbstractAppender("mock", null, null, false, Property.EMPTY_ARRAY) {
                @Override
                public void append(LogEvent event) {
                    if (event.getLevel() != Level.WARN
                        || event.getMessage().getFormattedMessage().contains("Eviction policy degraded") == false) {
                        return;
                    }
                    if (seenLoggingOnce.compareAndSet(false, true) == false) {
                        throw new AssertionError("degradation warning logged more than once");
                    }
                }
            };
            mockAppender.start();
            Loggers.addAppender(cacheServiceLogger, mockAppender);

            try {
                evictRandomly(cacheService, regionSize, decayed);
                assertThat(policyCallCount.get(), equalTo(1));
                assertThat(seenLoggingOnce.get(), is(true));
            } finally {
                firstRegion.decRef();
                Loggers.removeAppender(cacheServiceLogger, mockAppender);
                mockAppender.stop();
            }
        }
    }

    private boolean fillAndMaybeDecay(SharedBlobCacheService<FileCacheKey> cacheService, DeterministicTaskQueue taskQueue) {
        final boolean shouldDecay = randomBoolean();
        while (freeRegionCountFromCacheService(cacheService) > 0) {
            getFromCacheService(cacheService, generateFileCacheKey(), cacheRegionSizeInBytes(1), 0);
        }
        if (shouldDecay) {
            maybeScheduleDecayAndNewEpochForCacheService(cacheService);
        }
        taskQueue.runAllRunnableTasks();
        return shouldDecay;
    }

    private void evictRandomly(SharedBlobCacheService<FileCacheKey> cacheService, long regionSize, boolean decayed) {
        if (decayed == false) {
            getFromCacheService(cacheService, generateFileCacheKey(), regionSize, 0);
        } else {
            assertThat(maybeEvictLeastUsedFromCacheService(cacheService, generateFileCacheKey(), regionSize, 0), is(true));
        }
    }

    private FileCacheKey generateFileCacheKey() {
        return new FileCacheKey(new ShardId("index", randomUUID(), between(0, 5)), 1L, randomIdentifier());
    }

    private void runDemoteAllTest(boolean locallyAllocatedAtExecution, boolean expectDemotion) throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(cacheRegionSizeInBytes(500)).getStringRep()
            )
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(cacheRegionSizeInBytes(100)).getStringRep()
            )
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = newCacheService(environment, settings, taskQueue.getThreadPool())
        ) {
            final ShardId shardId = new ShardId("index", randomUUID(), 0);
            final var cacheKey = new FileCacheKey(shardId, 1L, "file");
            SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, cacheRegionSizeInBytes(250), 0);
            SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, cacheRegionSizeInBytes(250), 1);
            assertThat(
                SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheService, key -> key.shardId().equals(shardId)),
                equalTo(Map.of(1, 2))
            );

            cacheService.demoteAllAsync(shardId, id -> locallyAllocatedAtExecution == false);
            taskQueue.runAllRunnableTasks();

            if (expectDemotion) {
                assertThat(
                    SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheService, key -> key.shardId().equals(shardId)),
                    equalTo(Map.of(0, 2))
                );
            } else {
                assertThat(
                    SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheService, key -> key.shardId().equals(shardId)),
                    equalTo(Map.of(1, 2))
                );
            }
        }
    }

    private static long cacheRegionSizeInBytes(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }
}
