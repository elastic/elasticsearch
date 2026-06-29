/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.TestUtils.newCacheService;
import static org.hamcrest.Matchers.equalTo;

public class StatelessSharedBlobCacheServiceTests extends ESTestCase {

    public void testDemoteAllSkippedWhenShardLocallyAllocatedAtTaskExecution() throws IOException {
        runDemoteAllTest(true, false);
    }

    public void testDemoteAllWhenShardNotLocallyAllocatedAtTaskExecution() throws IOException {
        runDemoteAllTest(false, true);
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
