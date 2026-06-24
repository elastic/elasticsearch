/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.TestUtils.cacheRegionSizeInBytes;
import static org.elasticsearch.xpack.stateless.TestUtils.clusterStateWithShardOnLocalNode;
import static org.elasticsearch.xpack.stateless.TestUtils.clusterStateWithoutShardOnLocalNode;
import static org.elasticsearch.xpack.stateless.TestUtils.indexMetadata;
import static org.elasticsearch.xpack.stateless.TestUtils.newCacheService;
import static org.hamcrest.Matchers.equalTo;

public class StatelessSharedBlobCacheServiceTests extends ESTestCase {

    public void testResetAccessCountsSkippedWhenCacheBoostPreferenceDisabled() throws IOException {
        runResetAccessCountsTest(false, false, false);
    }

    public void testResetAccessCountsSkippedWhenShardLocallyAllocatedAtTaskExecution() throws IOException {
        runResetAccessCountsTest(true, false, true);
    }

    public void testResetAccessCountsWhenShardNotLocallyAllocatedAtTaskExecution() throws IOException {
        runResetAccessCountsTest(false, true, true);
    }

    private void runResetAccessCountsTest(boolean locallyAllocatedAtExecution, boolean expectReset, boolean cacheBoostEnabled)
        throws IOException {
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
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), cacheBoostEnabled)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ClusterSettings clusterSettings = new ClusterSettings(
            Settings.EMPTY,
            Sets.newHashSet(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(taskQueue.getThreadPool(), clusterSettings);
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = newCacheService(environment, settings, taskQueue.getThreadPool(), null, clusterService)
        ) {
            final ShardId shardId = new ShardId("index", randomUUID(), 0);
            final var index = indexMetadata(shardId.getIndexName(), shardId.getIndex().getUUID());
            final var cacheKey = new FileCacheKey(shardId, 1L, "file");
            SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, cacheRegionSizeInBytes(250), 0);
            SharedBlobCacheServiceTestUtils.cacheRegion(cacheService, cacheKey, cacheRegionSizeInBytes(250), 1);
            assertThat(cacheService.countCachedRegionsByFreq(key -> key.shardId().equals(shardId)), equalTo(Map.of(1, 2)));

            ClusterServiceUtils.setState(
                clusterService,
                locallyAllocatedAtExecution
                    ? clusterStateWithShardOnLocalNode(shardId, index)
                    : clusterStateWithoutShardOnLocalNode(shardId, index)
            );

            if (cacheBoostEnabled) {
                cacheService.asyncResetAccessCounts(shardId, id -> locallyAllocatedAtExecution == false);
            }
            taskQueue.runAllRunnableTasks();

            if (expectReset) {
                assertThat(cacheService.countCachedRegionsByFreq(key -> key.shardId().equals(shardId)), equalTo(Map.of(0, 2)));
            } else {
                assertThat(cacheService.countCachedRegionsByFreq(key -> key.shardId().equals(shardId)), equalTo(Map.of(1, 2)));
            }
        }
    }
}
