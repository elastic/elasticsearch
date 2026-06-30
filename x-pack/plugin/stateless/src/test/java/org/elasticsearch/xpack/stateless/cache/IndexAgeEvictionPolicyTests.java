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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_CREATION_DATE;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_UUID;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_VERSION_CREATED;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;

public class IndexAgeEvictionPolicyTests extends ESTestCase {

    private static final class TestIndexAgeEvictionPolicy extends IndexAgeEvictionPolicy {
        private final Map<ShardId, Long> creationDates;

        TestIndexAgeEvictionPolicy(Map<ShardId, Long> creationDates) {
            super();
            this.creationDates = creationDates;
        }

        @Override
        protected long indexCreationDateMillis(ShardId shardId) {
            return creationDates.getOrDefault(shardId, Long.MIN_VALUE);
        }
    }

    public void testCanEvictOlderRegionForNewerIncoming() {
        long[] creationDates = randomOlderAndRecentCreationDates();
        ShardId oldShard = new ShardId("old", randomUUID(), 0);
        ShardId recentShard = new ShardId("recent", randomUUID(), 0);
        var policy = new TestIndexAgeEvictionPolicy(Map.of(oldShard, creationDates[0], recentShard, creationDates[1]));

        assertTrue(canEvict(policy, region(oldShard, "f"), region(recentShard, "g")));
    }

    public void testCannotEvictNewerRegionForOlderIncoming() {
        long[] creationDates = randomOlderAndRecentCreationDates();
        ShardId oldShard = new ShardId("old", randomUUID(), 0);
        ShardId recentShard = new ShardId("recent", randomUUID(), 0);
        var policy = new TestIndexAgeEvictionPolicy(Map.of(oldShard, creationDates[0], recentShard, creationDates[1]));

        assertFalse(canEvict(policy, region(recentShard, "f"), region(oldShard, "g")));
    }

    public void testCanEvictWhenCreationDatesEqual() {
        long creationDate = randomLong();
        ShardId shard1 = new ShardId("index", randomUUID(), 0);
        ShardId shard2 = new ShardId("index2", randomUUID(), 0);
        var policy = new TestIndexAgeEvictionPolicy(Map.of(shard1, creationDate, shard2, creationDate));

        assertTrue(canEvict(policy, region(shard1, "f"), region(shard2, "g")));
    }

    public void testMissingShardTreatedAsOldest() {
        long recentDate = randomLong();
        ShardId unknownShard = new ShardId("unknown", randomUUID(), 0);
        ShardId recentShard = new ShardId("recent", randomUUID(), 0);
        var policy = new TestIndexAgeEvictionPolicy(Map.of(recentShard, recentDate));

        assertTrue(canEvict(policy, region(unknownShard, "f"), region(recentShard, "g")));
        assertFalse(canEvict(policy, region(recentShard, "f"), region(unknownShard, "g")));
    }

    /**
     * Fills the cache with regions from an older index, then inserts regions from a newer index and verifies that the
     * index-age eviction policy evicts older-index regions to make room.
     */
    public void testIndexAgeEvictionPolicyEvictsOlderIndexInCache() throws IOException {
        final int numRegions = randomIntBetween(4, 100);
        final long regionSizeInBytes = cacheRegionSizeInBytes(100);
        final long oldCreationDate = randomLongBetween(0, Long.MAX_VALUE - 2);
        final long newCreationDate = oldCreationDate + 1;

        final String oldIndexName = "old-index";
        final String newIndexName = "new-index";
        final IndexMetadata oldIndex = IndexMetadata.builder(oldIndexName)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(SETTING_INDEX_UUID, randomUUID())
                    .put(SETTING_CREATION_DATE, oldCreationDate)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        final IndexMetadata newIndex = IndexMetadata.builder(newIndexName)
            .settings(
                Settings.builder()
                    .put(SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(SETTING_INDEX_UUID, randomUUID())
                    .put(SETTING_CREATION_DATE, newCreationDate)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Settings settings = Settings.builder()
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
                StatelessCacheEvictionPolicyType.INDEX_AGE
            )
            .put("path.home", createTempDir())
            .build();

        final ShardId oldShard = new ShardId(oldIndex.getIndex(), 0);
        final ShardId newShard = new ShardId(newIndex.getIndex(), 0);

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterService clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), ProjectId.DEFAULT);
        final IndicesService indicesService = TestUtils.mockIndicesService(clusterService);
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(ClusterName.DEFAULT)
                .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).put(oldIndex, false).put(newIndex, false).build())
                .build()
        );
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                taskQueue.getThreadPool(),
                BlobCacheMetrics.NOOP,
                clusterService,
                indicesService,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            )
        ) {
            assertEquals(numRegions, SharedBlobCacheServiceTestUtils.freeRegionCount(cacheService));

            for (int i = 0; i < numRegions; i++) {
                var key = new FileCacheKey(oldShard, 1L, "file-" + i);
                SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, key, randomLongBetween(1, regionSizeInBytes - 1L), 0);
            }
            assertEquals(0, SharedBlobCacheServiceTestUtils.freeRegionCount(cacheService));
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(oldShard)), equalTo((long) numRegions));

            final int oldIndexReinsertions = randomIntBetween(1, numRegions);
            for (int i = 0; i < oldIndexReinsertions; i++) {
                var key = new FileCacheKey(oldShard, 1L, "file-" + randomIntBetween(0, numRegions - 1));
                SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, key, randomLongBetween(1, regionSizeInBytes - 1L), 0);
            }

            final int newEntries = randomIntBetween(1, numRegions);
            for (int i = 0; i < newEntries; i++) {
                var key = new FileCacheKey(newShard, 1L, "new-file-" + i);
                SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, key, randomLongBetween(1, regionSizeInBytes - 1L), 0);
            }

            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(newShard)), equalTo((long) newEntries));
            assertThat(cacheService.countCachedRegions(key -> key.shardId().equals(oldShard)), equalTo((long) (numRegions - newEntries)));
            assertThat(cacheService.countCachedRegions(key -> true), equalTo((long) numRegions));
        } finally {
            clusterService.close();
        }
    }

    private static CacheRegion<FileCacheKey> region(ShardId shardId, String file) {
        return new CacheRegion<>() {
            @Override
            public FileCacheKey key() {
                return new FileCacheKey(shardId, 1L, file);
            }

            @Override
            public long timestampMillis() {
                return UNKNOWN_TIMESTAMP;
            }
        };
    }

    private static long[] randomOlderAndRecentCreationDates() {
        long olderDate = randomLongBetween(Long.MIN_VALUE, Long.MAX_VALUE - 1);
        long recentDate = randomLongBetween(olderDate + 1, Long.MAX_VALUE);
        return new long[] { olderDate, recentDate };
    }

    private static long cacheRegionSizeInBytes(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    private static boolean canEvict(IndexAgeEvictionPolicy policy, CacheRegion<FileCacheKey> region, CacheRegion<FileCacheKey> incoming) {
        return policy.createPredicate(incoming).test(region);
    }
}
