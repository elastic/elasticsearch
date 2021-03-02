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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.CacheFileRegion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SMALL_REGION_SIZE;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_TINY_REGION_SIZE;

public class FrozenCacheServiceTests extends ESTestCase {

    public void testBasicEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_TINY_REGION_SIZE.getKey(), "10b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, 250, 0);
            assertEquals(100L, region0.tracker.getLength());
            assertEquals(3, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, 250, 1);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(2, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, 250, 2);
            assertEquals(50L, region2.tracker.getLength());
            assertEquals(2, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());

            assertTrue(region1.tryEvict());
            assertEquals(3, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            assertFalse(region1.tryEvict());
            assertEquals(3, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            region0.populateAndRead(
                ByteRange.of(0L, 1L),
                ByteRange.of(0L, 1L),
                (channel, channelPos, relativePos, length) -> 1,
                (channel, channelPos, relativePos, length, progressUpdater) -> progressUpdater.accept(length),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC)
            );
            assertFalse(region0.tryEvict());
            assertEquals(3, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            taskQueue.runAllRunnableTasks();
            assertTrue(region0.tryEvict());
            assertEquals(4, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            assertTrue(region2.tryEvict());
            assertEquals(4, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
        }
    }

    public void testAutoEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_TINY_REGION_SIZE.getKey(), "10b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, 460, 0);
            assertEquals(100L, region0.tracker.getLength());
            assertEquals(3, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, 460, 1);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(2, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, 460, 2);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(1, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region3 = cacheService.get(cacheKey, 460, 3);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());

            // acquire region 4, which should evict region 0 (oldest)
            final CacheFileRegion region4 = cacheService.get(cacheKey, 460, 4);
            assertEquals(60L, region4.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertTrue(region0.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());
            assertFalse(region4.isEvicted());

            // explicitly evict region 1
            assertTrue(region1.tryEvict());
            assertEquals(1, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
        }
    }

    public void testAutoEvictionMixedSmallAndLargeRegions() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE.getKey(), "22b")
            .put(SNAPSHOT_CACHE_TINY_REGION_SIZE.getKey(), "10b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeRegionCount());
            assertEquals(4, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, 449, 0);
            assertEquals(10L, region0.tracker.getLength());
            assertEquals(4, cacheService.freeRegionCount());
            assertEquals(4, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, 449, 1);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(3, cacheService.freeRegionCount());
            assertEquals(4, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, 449, 2);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(2, cacheService.freeRegionCount());
            assertEquals(4, cacheService.freeSmallRegionCount());
            final CacheFileRegion region3 = cacheService.get(cacheKey, 449, 3);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(1, cacheService.freeRegionCount());
            assertEquals(4, cacheService.freeSmallRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());

            final CacheFileRegion region4 = cacheService.get(cacheKey, 449, 4);
            assertEquals(100L, region4.tracker.getLength());
            final CacheFileRegion region5 = cacheService.get(cacheKey, 449, 5);
            assertEquals(22L, region5.tracker.getLength());
            final CacheFileRegion region6 = cacheService.get(cacheKey, 449, 6);
            assertEquals(17L, region6.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertEquals(3, cacheService.freeSmallRegionCount());

            final CacheKey otherKey = generateCacheKey();
            final CacheFileRegion otherRegion0 = cacheService.get(otherKey, 49, 0);
            assertEquals(10L, otherRegion0.tracker.getLength());
            final CacheFileRegion otherRegion1 = cacheService.get(otherKey, 49, 1);
            assertEquals(20L, otherRegion1.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());

            // acquire region 2 of the second file, which should evict region 4 (oldest small region)
            final CacheFileRegion otherRegion2 = cacheService.get(otherKey, 49, 2);
            assertEquals(19L, otherRegion2.tracker.getLength());

            assertEquals(0, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());

            assertFalse(region0.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());
            assertTrue(region4.isEvicted());

            // explicitly evict region 1
            assertTrue(region1.tryEvict());
            assertEquals(1, cacheService.freeRegionCount());
            assertEquals(0, cacheService.freeSmallRegionCount());

            // explicitly evict region 1 (small) in the second file
            assertTrue(otherRegion1.tryEvict());
            assertEquals(1, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
        }
    }

    public void testForceEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_TINY_REGION_SIZE.getKey(), "10b")
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
            assertEquals(4, cacheService.freeRegionCount());
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
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_TINY_REGION_SIZE.getKey(), "10b")
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
            final CacheKey cacheKey3 = generateCacheKey();
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(4, cacheService.freeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, 250, 0);
            assertEquals(3, cacheService.freeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, 250, 1);
            assertEquals(2, cacheService.freeRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey3, 250, 2);
            assertEquals(2, cacheService.freeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());

            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();

            final CacheFileRegion region0Again = cacheService.get(cacheKey1, 250, 0);
            assertSame(region0Again, region0);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            final CacheFileRegion region2Again = cacheService.get(cacheKey3, 250, 2);
            assertSame(region2Again, region2);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            cacheService.get(cacheKey1, 250, 0);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey1, 250, 0);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey3, 250, 2);
            assertEquals(2, cacheService.getFreq(region2));
            cacheService.get(cacheKey3, 250, 2);
            assertEquals(2, cacheService.getFreq(region2));

            // advance 2 ticks (decay only starts after 2 ticks)
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            // advance another tick
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            // advance another tick
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));
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
