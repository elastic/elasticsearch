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
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.EvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Predicate;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.cacheRegion;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.freeRegionCount;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.getEvictionPolicy;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.maybeEvictLeastUsed;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.maybeScheduleDecayAndNewEpoch;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils.randomRegionTimestampMillis;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.TestUtils.newCacheService;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class StatelessSharedBlobCacheServiceTests extends ESTestCase {

    /// The cache maintenance settings, all of which fall back to the cache boost preference setting.
    private static final List<Setting<Boolean>> CACHE_MAINTENANCE_SETTINGS = List.of(
        StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING,
        StatelessSharedBlobCacheService.STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING,
        StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING
    );

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
                STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.getKey(),
                STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.get(Settings.EMPTY).getAsRatio()
            );
        }
        if (randomBoolean()) {
            settingBuilder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_DURATION_SETTING.getKey(), "5m");
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
        final var clusterService = TestUtils.mockClusterService(settings);
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                clusterService.getClusterSettings(),
                taskQueue.getThreadPool(),
                new BlobCacheMetrics(new RecordingMeterRegistry()),
                neverEvict,
                () -> 0L,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
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

    public void testEvictionDegradationDurationLifecycle() throws Exception {
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
            .put(STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.getKey(), "50%")
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_DURATION_SETTING.getKey(), "10s")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();

        // policy that always rejects eviction and records how many times its predicate is called
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
        final var clusterService = TestUtils.mockClusterService(settings);
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                clusterService.getClusterSettings(),
                taskQueue.getThreadPool(),
                new BlobCacheMetrics(new RecordingMeterRegistry()),
                neverEvict,
                () -> 0L,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
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
            .put(STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.getKey(), "0%")
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_DURATION_SETTING.getKey(), "5m")
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

        final var clusterService = TestUtils.mockClusterService(settings);
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                clusterService.getClusterSettings(),
                taskQueue.getThreadPool(),
                new BlobCacheMetrics(new RecordingMeterRegistry()),
                neverEvict,
                () -> 0L,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
            cacheRegion(cacheService, generateFileCacheKey(), regionSize, 0, randomRegionTimestampMillis());
            // This is the 1st region since the entry is inserted at the head of freq list.
            final RefCounted firstRegion = cacheRegion(cacheService, generateFileCacheKey(), regionSize, 0, randomRegionTimestampMillis());
            // Decay synchronously (DecayAndNewEpochTask uses DIRECT_EXECUTOR_SERVICE, which runs tasks inline).
            final boolean decayed = randomBoolean();
            if (decayed) {
                maybeScheduleDecayAndNewEpoch(cacheService);
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
        while (freeRegionCount(cacheService) > 0) {
            cacheRegion(cacheService, generateFileCacheKey(), cacheRegionSizeInBytes(1), 0, randomRegionTimestampMillis());
        }
        if (shouldDecay) {
            maybeScheduleDecayAndNewEpoch(cacheService);
        }
        taskQueue.runAllRunnableTasks();
        return shouldDecay;
    }

    private void evictRandomly(SharedBlobCacheService<FileCacheKey> cacheService, long regionSize, boolean decayed) {
        if (decayed == false) {
            cacheRegion(cacheService, generateFileCacheKey(), regionSize, 0, randomRegionTimestampMillis());
        } else {
            assertThat(maybeEvictLeastUsed(cacheService, generateFileCacheKey(), regionSize, 0), is(true));
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
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                cacheKey,
                cacheRegionSizeInBytes(250),
                0,
                randomRegionTimestampMillis()
            );
            SharedBlobCacheServiceTestUtils.cacheRegion(
                cacheService,
                cacheKey,
                cacheRegionSizeInBytes(250),
                1,
                randomRegionTimestampMillis()
            );
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

    public void testEvictionPolicyOnIndexNodeIsAlwaysDefault() throws IOException {
        final var settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.INDEX_ROLE.roleName())
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheRegionSizeInBytes(1)).getStringRep())
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(cacheRegionSizeInBytes(1)).getStringRep()
            )
            .put("path.home", createTempDir())
            .build();
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterSettings = createClusterSettings(settings);
        final var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings);
        try (
            var environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = newCacheService(environment, settings, taskQueue.getThreadPool(), null, clusterService)
        ) {
            assertThat(getEvictionPolicy(cacheService), instanceOf(DefaultEvictionPolicy.class));

            clusterSettings.applySettings(
                Settings.builder()
                    .put(
                        STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.INDEX_AGE
                    )
                    .build()
            );

            assertThat(getEvictionPolicy(cacheService), instanceOf(DefaultEvictionPolicy.class));
        }
    }

    public void testEvictionPolicyOnSearchNodeCanBeChangedDynamically() throws IOException {
        final var settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.SEARCH_ROLE.roleName())
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheRegionSizeInBytes(1)).getStringRep())
            .put(
                SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(cacheRegionSizeInBytes(1)).getStringRep()
            )
            .put(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(), StatelessCacheEvictionPolicyType.ALWAYS)
            .put("path.home", createTempDir())
            .build();
        final var taskQueue = new DeterministicTaskQueue();
        final var clusterSettings = createClusterSettings(settings);
        final var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings);
        try (
            var environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = newCacheService(environment, settings, taskQueue.getThreadPool(), null, clusterService)
        ) {
            assertThat(getDelegatePolicy(getEvictionPolicy(cacheService)), instanceOf(DefaultEvictionPolicy.class));

            clusterSettings.applySettings(
                Settings.builder()
                    .put(
                        STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                        StatelessCacheEvictionPolicyType.INDEX_AGE
                    )
                    .build()
            );

            assertThat(getDelegatePolicy(getEvictionPolicy(cacheService)), instanceOf(IndexAgeEvictionPolicy.class));
        }
    }

    public void testCacheMaintenanceSettingsFallBackToCacheBoostPreference() {
        for (boolean cacheBoostPreferenceEnabled : new boolean[] { true, false }) {
            final var settings = Settings.builder()
                .put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), cacheBoostPreferenceEnabled)
                .build();
            for (Setting<Boolean> maintenanceSetting : CACHE_MAINTENANCE_SETTINGS) {
                assertThat(maintenanceSetting.get(settings), equalTo(cacheBoostPreferenceEnabled));
            }
        }
        for (Setting<Boolean> maintenanceSetting : CACHE_MAINTENANCE_SETTINGS) {
            assertThat("unset boost preference leaves the maintenance settings off", maintenanceSetting.get(Settings.EMPTY), is(false));
        }
    }

    public void testExplicitCacheMaintenanceSettingOverridesCacheBoostPreference() {
        for (boolean cacheBoostPreferenceEnabled : new boolean[] { true, false }) {
            for (Setting<Boolean> overriddenSetting : CACHE_MAINTENANCE_SETTINGS) {
                final var settings = Settings.builder()
                    .put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), cacheBoostPreferenceEnabled)
                    .put(overriddenSetting.getKey(), cacheBoostPreferenceEnabled == false)
                    .build();
                assertThat(overriddenSetting.get(settings), equalTo(cacheBoostPreferenceEnabled == false));
                for (Setting<Boolean> otherSetting : CACHE_MAINTENANCE_SETTINGS) {
                    if (otherSetting.getKey().equals(overriddenSetting.getKey()) == false) {
                        assertThat(otherSetting.get(settings), equalTo(cacheBoostPreferenceEnabled));
                    }
                }
            }
        }
    }

    /// Removing a cluster-level override must return the setting to the value derived from the node's boost preference, not to `false`.
    public void testCacheMaintenanceSettingResetsToCacheBoostPreferenceDerivedValue() {
        final var nodeSettings = Settings.builder().put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true).build();
        for (Setting<Boolean> maintenanceSetting : CACHE_MAINTENANCE_SETTINGS) {
            final var clusterSettings = createClusterSettings(nodeSettings);
            final var enabled = new AtomicBoolean();
            clusterSettings.initializeAndWatch(maintenanceSetting, enabled::set);
            assertTrue(enabled.get());

            clusterSettings.applySettings(Settings.builder().put(maintenanceSetting.getKey(), false).build());
            assertFalse(enabled.get());

            clusterSettings.applySettings(Settings.EMPTY);
            assertTrue(enabled.get());
        }
    }

    EvictionPolicy<FileCacheKey> getDelegatePolicy(EvictionPolicy<FileCacheKey> evictionPolicy) {
        if (evictionPolicy instanceof SwitchingEvictionPolicy switchingEvictionPolicy) {
            return switchingEvictionPolicy.getDelegate();
        }
        throw new AssertionError("Not a SwitchingEvictionPolicy: " + evictionPolicy);
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        var settingSet = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING);
        settingSet.add(STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING);
        settingSet.add(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING);
        settingSet.add(StatelessSharedBlobCacheService.STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING);
        settingSet.add(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_TIMESTAMP_BACKFILL_ENABLED_SETTING);
        settingSet.add(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING);
        return new ClusterSettings(settings, settingSet);
    }

    private static long cacheRegionSizeInBytes(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }
}
