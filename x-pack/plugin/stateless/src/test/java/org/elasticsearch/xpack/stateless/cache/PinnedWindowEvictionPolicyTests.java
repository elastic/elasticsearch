/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.CacheRegion;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.TestUtils.clusterStateWithShardOnLocalNode;
import static org.elasticsearch.xpack.stateless.TestUtils.clusterStateWithStartedShardsOnLocalNode;
import static org.elasticsearch.xpack.stateless.TestUtils.indexMetadata;
import static org.elasticsearch.xpack.stateless.cache.PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class PinnedWindowEvictionPolicyTests extends ESTestCase {

    private static final String UNKNOWN_TIMESTAMP_FILE_PREFIX = "unknown-file-";
    private static final String OUTSIDE_WINDOW_FILE_PREFIX = "outside-window-file-";
    private static final TimeValue PINNED_WINDOW_DURATION = PINNED_WINDOW_DURATION_SETTING.getDefault(Settings.EMPTY);

    private static final class TestPinnedWindowEvictionPolicy extends PinnedWindowEvictionPolicy {
        private final Set<ShardId> locallyAllocatedShards;
        private final long fixedCurrentTimeMillis;

        TestPinnedWindowEvictionPolicy(Set<ShardId> locallyAllocatedShards, long fixedCurrentTimeMillis, long pinnedWindowDurationMillis) {
            super(TimeValue.timeValueMillis(pinnedWindowDurationMillis));
            this.locallyAllocatedShards = locallyAllocatedShards;
            this.fixedCurrentTimeMillis = fixedCurrentTimeMillis;
        }

        @Override
        protected boolean isShardLocallyAllocated(ShardId shardId) {
            return locallyAllocatedShards.contains(shardId);
        }

        @Override
        protected long currentTimeMillis() {
            return fixedCurrentTimeMillis;
        }
    }

    public void testDurationBelowMinimumRejected() {
        Settings settings = Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "10ms").build();
        expectThrows(IllegalArgumentException.class, () -> PINNED_WINDOW_DURATION_SETTING.get(settings));
    }

    public void testPinnedWindowDurationUpdatesDynamically() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(Settings.EMPTY);
        try (var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings)) {
            final var policy = new PinnedWindowEvictionPolicy(clusterService);
            assertThat(policy.getPinnedWindowDuration(), equalTo(PINNED_WINDOW_DURATION));

            clusterSettings.applySettings(Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "6h").build());
            assertThat(policy.getPinnedWindowDuration(), equalTo(TimeValue.timeValueHours(6)));
        }
    }

    public void testCannotEvictLocallyAllocatedRegionWithinPinnedWindow() {
        final long now = randomLongBetween(TimeValue.timeValueDays(365).millis(), TimeValue.timeValueDays(365 * 50).millis());
        final long pinnedWindowDurationMillis = PINNED_WINDOW_DURATION.millis();
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final long timestampMillis = now - randomLongBetween(0, pinnedWindowDurationMillis - 1);
        final var policy = new TestPinnedWindowEvictionPolicy(Set.of(shardId), now, pinnedWindowDurationMillis);

        assertFalse(policy.canEvict(region(shardId, timestampMillis), region(shardId, timestampMillis + 1)));
    }

    /**
     * Locally allocated regions without a content timestamp must remain protected until their age
     * relative to the pinned window can be evaluated.
     */
    public void testCannotEvictLocallyAllocatedRegionWithUnknownTimestamp() {
        final long now = randomLongBetween(1, Long.MAX_VALUE);
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final var policy = new TestPinnedWindowEvictionPolicy(Set.of(shardId), now, PINNED_WINDOW_DURATION.millis());

        assertFalse(policy.canEvict(region(shardId, UNKNOWN_TIMESTAMP), region(shardId, now)));
    }

    public void testCanEvictLocallyAllocatedRegionOutsidePinnedWindow() {
        final long now = randomLongBetween(TimeValue.timeValueDays(365).millis(), TimeValue.timeValueDays(365 * 50).millis());
        final long pinnedWindowDurationMillis = PINNED_WINDOW_DURATION.millis();
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final long timestampMillis = now - pinnedWindowDurationMillis - randomLongBetween(1, TimeValue.timeValueDays(30).millis());
        final var policy = new TestPinnedWindowEvictionPolicy(Set.of(shardId), now, pinnedWindowDurationMillis);

        assertTrue(policy.canEvict(region(shardId, timestampMillis), region(shardId, now)));
    }

    public void testCanEvictWhenShardNotLocallyAllocated() {
        final long now = randomLongBetween(TimeValue.timeValueDays(365).millis(), TimeValue.timeValueDays(365 * 50).millis());
        final long pinnedWindowDurationMillis = PINNED_WINDOW_DURATION.millis();
        final ShardId localShard = new ShardId("local", randomUUID(), 0);
        final ShardId remoteShard = new ShardId("remote", randomUUID(), 0);
        final long timestampMillis = now - randomLongBetween(0, pinnedWindowDurationMillis - 1);
        final var policy = new TestPinnedWindowEvictionPolicy(Set.of(localShard), now, pinnedWindowDurationMillis);

        assertTrue(policy.canEvict(region(remoteShard, timestampMillis), region(localShard, timestampMillis)));
    }

    public void testPinnedWindowBoundaryIsInclusive() {
        final long pinnedWindowDurationMillis = PINNED_WINDOW_DURATION.millis();
        final long now = pinnedWindowDurationMillis + randomLongBetween(1, Long.MAX_VALUE / 2);
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final var policy = new TestPinnedWindowEvictionPolicy(Set.of(shardId), now, pinnedWindowDurationMillis);

        assertFalse(policy.canEvict(region(shardId, now - pinnedWindowDurationMillis), region(shardId, now)));
        assertTrue(policy.canEvict(region(shardId, now - pinnedWindowDurationMillis - 1), region(shardId, now)));
    }

    public void testShrinkingPinnedWindowMakesRegionEvictable() {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(
            Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), PINNED_WINDOW_DURATION).build()
        );
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final long now = System.currentTimeMillis();
        final long timestampMillis = now - TimeValue.timeValueHours(8).millis();
        try (var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings)) {
            ClusterServiceUtils.setState(
                clusterService,
                clusterStateWithShardOnLocalNode(
                    shardId,
                    indexMetadata(shardId.getIndexName(), shardId.getIndex().getUUID()),
                    ShardRoutingState.STARTED
                )
            );
            final var policy = new PinnedWindowEvictionPolicy(clusterService);
            final CacheRegion<FileCacheKey> region = region(shardId, timestampMillis);
            final CacheRegion<FileCacheKey> incoming = region(shardId, now);

            assertFalse(policy.canEvict(region, incoming));

            clusterSettings.applySettings(Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "6h").build());
            assertTrue(policy.canEvict(region, incoming));
        }
    }

    public void testCannotEvictRegionForStartedShardOnLocalNodeWithinPinnedWindow() {
        assertProtectedForLocalShardRoutingState(ShardRoutingState.STARTED);
    }

    public void testCannotEvictRegionForRelocationTargetShardOnLocalNodeWithinPinnedWindow() {
        assertProtectedForLocalShardRoutingState(ShardRoutingState.INITIALIZING);
    }

    public void testCannotEvictRegionForRelocatingShardOnLocalNodeWithinPinnedWindow() {
        assertProtectedForLocalShardRoutingState(ShardRoutingState.RELOCATING);
    }

    /**
     * Fills the cache with a random mix of unknown-timestamp and outside-window regions, then inserts
     * inside-window regions and verifies that only outside-window regions are evicted to make room.
     */
    public void testPinnedWindowEvictionPolicyEvictsOutsideWindowDataInCache() throws IOException {
        final int numRegions = randomIntBetween(4, 100);
        final int unknownTimestampRegionCount = randomIntBetween(0, numRegions - 1);
        final int outsideWindowRegionCount = numRegions - unknownTimestampRegionCount;
        final long regionSizeInBytes = cacheRegionSizeInBytes(100);
        final TimeValue pinnedWindowDuration = PINNED_WINDOW_DURATION;
        final long now = System.currentTimeMillis();
        final long outsideWindowTimestamp = now - pinnedWindowDuration.millis() - randomLongBetween(
            1,
            TimeValue.timeValueHours(1).millis()
        );
        final long insideWindowTimestamp = now - randomLongBetween(0, pinnedWindowDuration.millis() - 1);

        final String oldIndexName = "old-index";
        final String newIndexName = "new-index";
        final IndexMetadata oldIndex = indexMetadata(oldIndexName, randomUUID());
        final IndexMetadata newIndex = indexMetadata(newIndexName, randomUUID());

        Settings settings = pinnedWindowCacheTestSettings(numRegions, regionSizeInBytes, pinnedWindowDuration);

        final ShardId oldShard = new ShardId(oldIndex.getIndex(), 0);
        final ShardId newShard = new ShardId(newIndex.getIndex(), 0);

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(
                taskQueue.getThreadPool(),
                createClusterSettings(settings)
            );
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                taskQueue.getThreadPool(),
                BlobCacheMetrics.NOOP,
                clusterService,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
            ClusterServiceUtils.setState(clusterService, clusterStateWithStartedShardsOnLocalNode(oldIndex, newIndex));
            assertEquals(numRegions, SharedBlobCacheServiceTestUtils.freeRegionCount(cacheService));

            for (int i = 0; i < unknownTimestampRegionCount; i++) {
                populateCacheRegion(cacheService, oldShard, UNKNOWN_TIMESTAMP_FILE_PREFIX + i, regionSizeInBytes, null);
            }
            for (int i = 0; i < outsideWindowRegionCount; i++) {
                populateCacheRegion(cacheService, oldShard, OUTSIDE_WINDOW_FILE_PREFIX + i, regionSizeInBytes, outsideWindowTimestamp);
            }
            assertEquals(0, SharedBlobCacheServiceTestUtils.freeRegionCount(cacheService));
            assertThat(
                countCachedRegionsWithFilePrefix(cacheService, oldShard, UNKNOWN_TIMESTAMP_FILE_PREFIX),
                equalTo((long) unknownTimestampRegionCount)
            );
            assertThat(
                countCachedRegionsWithFilePrefix(cacheService, oldShard, OUTSIDE_WINDOW_FILE_PREFIX),
                equalTo((long) outsideWindowRegionCount)
            );

            final int oldIndexReinsertions = randomIntBetween(1, outsideWindowRegionCount);
            for (int i = 0; i < oldIndexReinsertions; i++) {
                populateCacheRegion(
                    cacheService,
                    oldShard,
                    OUTSIDE_WINDOW_FILE_PREFIX + randomIntBetween(0, outsideWindowRegionCount - 1),
                    regionSizeInBytes,
                    outsideWindowTimestamp
                );
            }

            final int newEntries = randomIntBetween(1, outsideWindowRegionCount);
            for (int i = 0; i < newEntries; i++) {
                populateCacheRegion(cacheService, newShard, "new-file-" + i, regionSizeInBytes, insideWindowTimestamp);
            }

            assertThat(
                countCachedRegionsWithFilePrefix(cacheService, oldShard, UNKNOWN_TIMESTAMP_FILE_PREFIX),
                equalTo((long) unknownTimestampRegionCount)
            );
            assertThat(
                countCachedRegionsWithFilePrefix(cacheService, oldShard, OUTSIDE_WINDOW_FILE_PREFIX),
                equalTo((long) (outsideWindowRegionCount - newEntries))
            );
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(newShard)), equalTo((long) newEntries));
            assertThat(cacheService.countCachedRegions(key -> true), equalTo((long) numRegions));
        }
    }

    private void assertProtectedForLocalShardRoutingState(ShardRoutingState routingState) {
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = createClusterSettings(
            Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), PINNED_WINDOW_DURATION).build()
        );
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final long now = System.currentTimeMillis();
        final long timestampMillis = now - randomLongBetween(0, PINNED_WINDOW_DURATION.millis() - 1);
        try (var clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings)) {
            ClusterServiceUtils.setState(
                clusterService,
                clusterStateWithShardOnLocalNode(shardId, indexMetadata(shardId.getIndexName(), shardId.getIndex().getUUID()), routingState)
            );
            final var policy = new PinnedWindowEvictionPolicy(clusterService);
            final CacheRegion<FileCacheKey> region = region(shardId, timestampMillis);
            final CacheRegion<FileCacheKey> incoming = region(shardId, now);

            assertFalse(policy.canEvict(region, incoming));
        }
    }

    private static CacheRegion<FileCacheKey> region(ShardId shardId, long timestampMillis) {
        return new CacheRegion<>() {
            @Override
            public FileCacheKey key() {
                return new FileCacheKey(shardId, 1L, "file");
            }

            @Override
            public long timestampMillis() {
                return timestampMillis;
            }
        };
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        Set<Setting<?>> clusterSettings = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        clusterSettings.add(PINNED_WINDOW_DURATION_SETTING);
        return new ClusterSettings(settings, clusterSettings);
    }

    private static long cacheRegionSizeInBytes(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    private Settings pinnedWindowCacheTestSettings(int numRegions, long regionSizeInBytes, TimeValue pinnedWindowDuration) {
        return Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(cacheRegionSizeInBytes(numRegions * 100L))
            )
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes))
            .put(SharedBlobCacheService.SHARED_CACHE_INITIAL_DECAYS_SETTING.getKey(), 0)
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.PINNED_WINDOW
            )
            .put(PINNED_WINDOW_DURATION_SETTING.getKey(), pinnedWindowDuration)
            .put("path.home", createTempDir())
            .build();
    }

    private void populateCacheRegion(
        StatelessSharedBlobCacheService cacheService,
        ShardId shardId,
        String fileName,
        long regionSizeInBytes,
        Long timestampMillis
    ) {
        var key = new FileCacheKey(shardId, 1L, fileName);
        long fileLength = randomLongBetween(1, regionSizeInBytes - 1L);
        if (timestampMillis == null) {
            SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, key, fileLength, 0);
        } else {
            SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, key, fileLength, 0, timestampMillis);
        }
    }

    private static long countCachedRegionsWithFilePrefix(
        StatelessSharedBlobCacheService cacheService,
        ShardId shardId,
        String fileNamePrefix
    ) {
        return cacheService.countCachedRegions(key -> key.shardId().equals(shardId) && key.fileName().startsWith(fileNamePrefix));
    }
}
