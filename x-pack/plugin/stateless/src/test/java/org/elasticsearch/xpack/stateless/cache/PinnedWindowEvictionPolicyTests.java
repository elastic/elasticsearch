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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.cache.PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class PinnedWindowEvictionPolicyTests extends ESTestCase {

    private static final String UNKNOWN_TIMESTAMP_FILE_PREFIX = "unknown-file-";
    private static final String OUTSIDE_WINDOW_FILE_PREFIX = "outside-window-file-";
    private static final TimeValue PINNED_WINDOW_DURATION = PINNED_WINDOW_DURATION_SETTING.getDefault(Settings.EMPTY);

    private DeterministicTaskQueue taskQueue;
    private ClusterService clusterService;
    private ClusterSettings clusterSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        taskQueue = new DeterministicTaskQueue();
        taskQueue.runTasksUpToTimeInOrder(System.currentTimeMillis());
        clusterSettings = createClusterSettings(Settings.EMPTY);
        clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings);
    }

    @After
    public void tearDown() throws Exception {
        clusterService.close();
        super.tearDown();
    }

    public void testDurationBelowMinimumRejected() {
        Settings settings = Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "10ms").build();
        expectThrows(IllegalArgumentException.class, () -> PINNED_WINDOW_DURATION_SETTING.get(settings));
    }

    public void testPinnedWindowDurationUpdatesDynamically() {
        final var policy = new PinnedWindowEvictionPolicy(clusterSettings, clusterService.threadPool(), shardId -> false);
        assertThat(policy.getPinnedWindowDuration(), equalTo(PINNED_WINDOW_DURATION));

        clusterSettings.applySettings(Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "6h").build());
        assertThat(policy.getPinnedWindowDuration(), equalTo(TimeValue.timeValueHours(6)));
    }

    public void testCannotEvictPresentShardRegionWithinPinnedWindow() {
        final long now = randomLongBetween(TimeValue.timeValueDays(365).millis(), TimeValue.timeValueDays(365 * 50).millis());
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final long timestampMillis = now - randomLongBetween(0, PINNED_WINDOW_DURATION.millis() - 1);

        assertFalse(canEvict(fixedTimePolicy(now, PINNED_WINDOW_DURATION, shardId), region(shardId, timestampMillis)));
    }

    public void testCannotEvictPresentShardRegionWithUnknownTimestamp() {
        final long now = randomLongBetween(1, Long.MAX_VALUE);
        final ShardId shardId = new ShardId("index", randomUUID(), 0);

        assertFalse(canEvict(fixedTimePolicy(now, PINNED_WINDOW_DURATION, shardId), region(shardId, UNKNOWN_TIMESTAMP)));
    }

    public void testCannotEvictPresentShardRegionWithBackfillInProgressTimestamp() {
        final long now = randomLongBetween(1, Long.MAX_VALUE);
        final ShardId shardId = new ShardId("index", randomUUID(), 0);

        assertFalse(canEvict(fixedTimePolicy(now, PINNED_WINDOW_DURATION, shardId), region(shardId, BACKFILL_IN_PROGRESS_TIMESTAMP)));
    }

    public void testCanEvictPresentShardRegionOutsidePinnedWindow() {
        final long now = randomLongBetween(TimeValue.timeValueDays(365).millis(), TimeValue.timeValueDays(365 * 50).millis());
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final long timestampMillis = now - PINNED_WINDOW_DURATION.millis() - randomLongBetween(1, TimeValue.timeValueDays(30).millis());

        assertTrue(canEvict(fixedTimePolicy(now, PINNED_WINDOW_DURATION, shardId), region(shardId, timestampMillis)));
    }

    public void testCanEvictWhenShardNotPresent() {
        final long now = randomLongBetween(TimeValue.timeValueDays(365).millis(), TimeValue.timeValueDays(365 * 50).millis());
        final ShardId localShard = new ShardId("local", randomUUID(), 0);
        final ShardId remoteShard = new ShardId("remote", randomUUID(), 0);
        final long timestampMillis = now - randomLongBetween(0, PINNED_WINDOW_DURATION.millis() - 1);

        assertTrue(canEvict(fixedTimePolicy(now, PINNED_WINDOW_DURATION, localShard), region(remoteShard, timestampMillis)));
    }

    public void testPinnedWindowBoundaryIsInclusive() {
        final long now = PINNED_WINDOW_DURATION.millis() + randomLongBetween(1, Long.MAX_VALUE / 2);
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final var policy = fixedTimePolicy(now, PINNED_WINDOW_DURATION, shardId);

        assertFalse(canEvict(policy, region(shardId, now - PINNED_WINDOW_DURATION.millis())));
        assertTrue(canEvict(policy, region(shardId, now - PINNED_WINDOW_DURATION.millis() - 1)));
    }

    public void testShrinkingPinnedWindowMakesRegionEvictable() {
        clusterSettings.applySettings(Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), PINNED_WINDOW_DURATION).build());
        final ShardId shardId = new ShardId("index", randomUUID(), 0);
        final long now = clusterService.threadPool().absoluteTimeInMillis();
        final long timestampMillis = now - TimeValue.timeValueHours(8).millis();
        final var policy = new PinnedWindowEvictionPolicy(
            clusterSettings,
            clusterService.threadPool(),
            shardIdPredicate -> shardIdPredicate.equals(shardId)
        );
        final var region = region(shardId, timestampMillis);

        assertFalse(canEvict(policy, region));

        clusterSettings.applySettings(Settings.builder().put(PINNED_WINDOW_DURATION_SETTING.getKey(), "6h").build());
        assertTrue(canEvict(policy, region));
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
        final long now = taskQueue.getCurrentTimeMillis();
        final long outsideWindowTimestamp = now - PINNED_WINDOW_DURATION.millis() - randomLongBetween(
            1,
            TimeValue.timeValueHours(1).millis()
        );
        final long insideWindowTimestamp = now - randomLongBetween(0, PINNED_WINDOW_DURATION.millis() - 1);

        final IndexMetadata oldIndex = indexMetadata("old-index", randomUUID());
        final IndexMetadata newIndex = indexMetadata("new-index", randomUUID());
        final ShardId oldShard = new ShardId(oldIndex.getIndex(), 0);
        final ShardId newShard = new ShardId(newIndex.getIndex(), 0);
        final Settings settings = pinnedWindowCacheTestSettings(numRegions, regionSizeInBytes);

        try (
            var cacheClusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), createClusterSettings(settings));
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                taskQueue.getThreadPool(),
                BlobCacheMetrics.NOOP,
                cacheClusterService,
                mockIndicesService(cacheClusterService, oldShard, newShard),
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
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

    private PinnedWindowEvictionPolicy fixedTimePolicy(long now, TimeValue pinnedWindowDuration, ShardId... presentShardIds) {
        final Predicate<ShardId> hasShardPredicate = Set.copyOf(Arrays.asList(presentShardIds))::contains;
        return new FixedTimePinnedWindowEvictionPolicy(clusterService.threadPool(), hasShardPredicate, now, pinnedWindowDuration);
    }

    private static boolean canEvict(PinnedWindowEvictionPolicy policy, CacheRegion<FileCacheKey> region) {
        return policy.createPredicate(region).test(region);
    }

    private static IndicesService mockIndicesService(ClusterService clusterService, ShardId... presentShardIds) {
        final Predicate<ShardId> hasShardPredicate = Set.copyOf(Arrays.asList(presentShardIds))::contains;
        return TestUtils.mockIndicesService(clusterService, hasShardPredicate);
    }

    private static final class FixedTimePinnedWindowEvictionPolicy extends PinnedWindowEvictionPolicy {
        private final long fixedCurrentTimeMillis;

        FixedTimePinnedWindowEvictionPolicy(
            ThreadPool threadPool,
            Predicate<ShardId> hasShardPredicate,
            long fixedCurrentTimeMillis,
            TimeValue pinnedWindowDuration
        ) {
            super(threadPool, hasShardPredicate, pinnedWindowDuration);
            this.fixedCurrentTimeMillis = fixedCurrentTimeMillis;
        }

        @Override
        protected long currentTimeMillis() {
            return fixedCurrentTimeMillis;
        }
    }

    private static IndexMetadata indexMetadata(String indexName, String indexUuid) {
        return IndexMetadata.builder(indexName)
            .settings(Settings.builder().put(SETTING_VERSION_CREATED, IndexVersion.current()).put(SETTING_INDEX_UUID, indexUuid))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
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
        Set<Setting<?>> settingsSet = Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.add(PINNED_WINDOW_DURATION_SETTING);
        return new ClusterSettings(settings, settingsSet);
    }

    private static long cacheRegionSizeInBytes(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    private Settings pinnedWindowCacheTestSettings(int numRegions, long regionSizeInBytes) {
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
            .put(PINNED_WINDOW_DURATION_SETTING.getKey(), PINNED_WINDOW_DURATION)
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
