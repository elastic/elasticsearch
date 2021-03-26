/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService.CacheFileRegion;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

public class FrozenCacheServiceTests extends ESTestCase {

    private static long size(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    public void testBasicEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(size(500)).getStringRep())
            .put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), new ByteSizeValue(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            FrozenCacheService cacheService = new FrozenCacheService(environment, settings, taskQueue.getThreadPool())
        ) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, size(250), 0);
            assertEquals(size(100), region0.tracker.getLength());
            assertEquals(4, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, size(250), 1);
            assertEquals(size(100), region1.tracker.getLength());
            assertEquals(3, cacheService.freeRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, size(250), 2);
            assertEquals(size(50), region2.tracker.getLength());
            assertEquals(2, cacheService.freeRegionCount());

            assertTrue(region1.tryEvict());
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(region1.tryEvict());
            assertEquals(3, cacheService.freeRegionCount());
            region0.populateAndRead(
                ByteRange.of(0L, 1L),
                ByteRange.of(0L, 1L),
                (channel, channelPos, relativePos, length) -> 1,
                (channel, channelPos, relativePos, length, progressUpdater) -> progressUpdater.accept(length),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC)
            );
            assertFalse(region0.tryEvict());
            assertEquals(3, cacheService.freeRegionCount());
            taskQueue.runAllRunnableTasks();
            assertTrue(region0.tryEvict());
            assertEquals(4, cacheService.freeRegionCount());
            assertTrue(region2.tryEvict());
            assertEquals(5, cacheService.freeRegionCount());
        }
    }

    public void testAutoEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(size(200)).getStringRep())
            .put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), new ByteSizeValue(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            FrozenCacheService cacheService = new FrozenCacheService(environment, settings, taskQueue.getThreadPool())
        ) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(2, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, size(250), 0);
            assertEquals(size(100), region0.tracker.getLength());
            assertEquals(1, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, size(250), 1);
            assertEquals(size(100), region1.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());

            // acquire region 2, which should evict region 0 (oldest)
            final CacheFileRegion region2 = cacheService.get(cacheKey, size(250), 2);
            assertEquals(size(50), region2.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());

            // explicitly evict region 1
            assertTrue(region1.tryEvict());
            assertEquals(1, cacheService.freeRegionCount());
        }
    }

    public void testForceEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(size(500)).getStringRep())
            .put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), new ByteSizeValue(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            FrozenCacheService cacheService = new FrozenCacheService(environment, settings, taskQueue.getThreadPool())
        ) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            cacheService.removeFromCache(cacheKey1);
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertEquals(4, cacheService.freeRegionCount());
        }
    }

    public void testDecay() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(size(500)).getStringRep())
            .put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), new ByteSizeValue(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            FrozenCacheService cacheService = new FrozenCacheService(environment, settings, taskQueue.getThreadPool())
        ) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(3, cacheService.freeRegionCount());

            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();

            final CacheFileRegion region0Again = cacheService.get(cacheKey1, size(250), 0);
            assertSame(region0Again, region0);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            cacheService.get(cacheKey1, size(250), 0);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey1, size(250), 0);
            assertEquals(2, cacheService.getFreq(region0));

            // advance 2 ticks (decay only starts after 2 ticks)
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));

            // advance another tick
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
        }
    }

    public void testCacheSizeDeprecatedOnNonFrozenNodes() {
        DiscoveryNode.setAdditionalRoles(
            Set.of(DataTier.DATA_HOT_NODE_ROLE, DataTier.DATA_WARM_NODE_ROLE, DataTier.DATA_COLD_NODE_ROLE, DataTier.DATA_FROZEN_NODE_ROLE)
        );
        final Settings settings = Settings.builder()
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(size(500)).getStringRep())
            .put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), new ByteSizeValue(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DataTier.DATA_HOT_NODE_ROLE.roleName())
            .build();
        FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(settings);
        assertWarnings(
            "setting ["
                + FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey()
                + "] to be positive ["
                + new ByteSizeValue(size(500)).getStringRep()
                + "] on node without the data_frozen role is deprecated, roles are [data_hot]"
        );
    }

    private static CacheKey generateCacheKey() {
        return new CacheKey(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomInt(10)),
            randomAlphaOfLength(10)
        );
    }
}
