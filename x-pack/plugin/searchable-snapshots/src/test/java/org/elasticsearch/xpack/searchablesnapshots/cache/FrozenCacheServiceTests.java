/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.CacheFileRegion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

public class FrozenCacheServiceTests extends ESTestCase {

    public void testBasicEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, 250, 0);
            assertEquals(100L, region0.tracker.getLength());
            assertEquals(4, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, 250, 1);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(3, cacheService.freeRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, 250, 2);
            assertEquals(50L, region2.tracker.getLength());
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
            .put(SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "200b")
            .put(SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(2, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, 250, 0);
            assertEquals(100L, region0.tracker.getLength());
            assertEquals(1, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, 250, 1);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());

            // acquire region 2, which should evict region 0 (oldest)
            final CacheFileRegion region2 = cacheService.get(cacheKey, 250, 2);
            assertEquals(50L, region2.tracker.getLength());
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
            .put(SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, 250, 0);
            assertEquals(4, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, 250, 1);
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
            .put(SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, 250, 0);
            assertEquals(4, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, 250, 1);
            assertEquals(3, cacheService.freeRegionCount());

            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();

            final CacheFileRegion region0Again = cacheService.get(cacheKey1, 250, 0);
            assertSame(region0Again, region0);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            cacheService.get(cacheKey1, 250, 0);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey1, 250, 0);
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

    private static CacheKey generateCacheKey() {
        return new CacheKey(
            randomAlphaOfLength(10),
            randomAlphaOfLength(10),
            new ShardId(randomAlphaOfLength(10), randomAlphaOfLength(10), randomInt(10)),
            randomAlphaOfLength(10)
        );
    }
}
