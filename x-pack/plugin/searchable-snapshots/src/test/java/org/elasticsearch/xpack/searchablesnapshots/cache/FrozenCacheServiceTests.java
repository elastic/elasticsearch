/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.snapshots.SharedCacheConfiguration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.CacheFileRegion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SIZE_SETTING;
import static org.elasticsearch.snapshots.SnapshotsService.SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE;

public class FrozenCacheServiceTests extends ESTestCase {

    public void testBasicEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 0L;
        final long footerCacheRange = 0L;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, 250, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region0.tracker.getLength());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, 250, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, 250, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(50L, region2.tracker.getLength());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());

            assertTrue(region1.tryEvict());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertFalse(region1.tryEvict());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            region0.populateAndRead(
                ByteRange.of(0L, 1L),
                ByteRange.of(0L, 1L),
                (channel, channelPos, relativePos, length) -> 1,
                (channel, channelPos, relativePos, length, progressUpdater) -> progressUpdater.accept(length),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC)
            );
            assertFalse(region0.tryEvict());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            taskQueue.runAllRunnableTasks();
            assertTrue(region0.tryEvict());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertTrue(region2.tryEvict());
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
        }
    }

    public void testHeaderOnlyFile() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 10L;
        final long footerCacheRange = 10L;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(1, cacheService.freeTinyRegionCount());
            final CacheFileRegion region0 = cacheService.get(generateCacheKey(), 8, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(8L, region0.tracker.getLength());
            assertEquals(0, cacheService.freeTinyRegionCount());
            assertFalse(region0.isEvicted());

            final CacheFileRegion region1 = cacheService.get(generateCacheKey(), 10, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(10L, region1.tracker.getLength());
            assertTrue(region0.isEvicted());

            final CacheFileRegion region2 = cacheService.get(generateCacheKey(), 7, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(7L, region2.tracker.getLength());
            assertEquals(0, cacheService.freeTinyRegionCount());
            assertTrue(region0.isEvicted());
            assertTrue(region1.isEvicted());

            final CacheFileRegion region3 = cacheService.get(generateCacheKey(), 11, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(1L, region3.tracker.getLength());
            assertEquals(0, cacheService.freeTinyRegionCount());
            assertTrue(region0.isEvicted());
            assertTrue(region1.isEvicted());
            assertTrue(region2.isEvicted());
        }
    }

    public void testAutoEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 0;
        final long footerCacheRange = 0L;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, 460, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region0.tracker.getLength());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, 460, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, 460, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region3 = cacheService.get(cacheKey, 460, 3, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());

            // acquire region 4, which should evict region 0 (oldest)
            final CacheFileRegion region4 = cacheService.get(cacheKey, 460, 4, cacheHeaderRange, footerCacheRange);
            assertEquals(60L, region4.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertTrue(region0.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());
            assertFalse(region4.isEvicted());

            // explicitly evict region 1
            assertTrue(region1.tryEvict());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
        }
    }

    public void testAutoEvictionMixedSmallAndLargeRegions() throws IOException {
        final ByteSizeValue largeRegionSize = ByteSizeValue.ofBytes(SharedCacheConfiguration.SMALL_REGION_SIZE * 2);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(SharedCacheConfiguration.SMALL_REGION_SIZE * 10))
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), largeRegionSize)
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = SharedCacheConfiguration.TINY_REGION_SIZE;
        final long footerCacheRange = 0L;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final long fileSize = 4 * largeRegionSize.getBytes();
            final CacheFileRegion region0 = cacheService.get(cacheKey, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(cacheHeaderRange, region0.tracker.getLength());
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, fileSize, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region1.tracker.getLength());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, fileSize, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region2.tracker.getLength());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region3 = cacheService.get(cacheKey, fileSize, 3, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region3.tracker.getLength());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());

            final CacheFileRegion region4 = cacheService.get(cacheKey, fileSize, 4, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes() - SharedCacheConfiguration.TINY_REGION_SIZE, region4.tracker.getLength());
            // acquire region 5 which should evict region 1 (oldest large region)
            final CacheFileRegion region5 = cacheService.get(cacheKey, 449, 5, cacheHeaderRange, footerCacheRange);
            assertEquals(27L, region5.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(3, cacheService.freeSmallRegionCount());
            assertTrue(region1.isEvicted());

            final CacheKey otherKey = generateCacheKey();
            final CacheFileRegion otherRegion0 = cacheService.get(otherKey, 49, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(cacheHeaderRange, otherRegion0.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertFalse(region2.isEvicted());

            // acquire region 1 of second file which should evict region 2 (oldest large region)
            final CacheFileRegion otherRegion1 = cacheService.get(otherKey, 49, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(27L, otherRegion1.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());

            assertFalse(region0.isEvicted());
            assertFalse(region3.isEvicted());
            assertTrue(region2.isEvicted());
            assertTrue(region1.isEvicted());

            // explicitly evict region 3
            assertTrue(region3.tryEvict());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());

            // explicitly evict region 0 (small) in the second file
            assertTrue(otherRegion0.tryEvict());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(3, cacheService.freeSmallRegionCount());
        }
    }

    public void testForceEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 0;
        final long footerCacheRange = 0L;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, 250, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(3, cacheService.freeLargeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, 250, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            cacheService.removeFromCache(cacheKey1);
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertEquals(3, cacheService.freeLargeRegionCount());
        }
    }

    public void testDecay() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 0L;
        final long footerCacheRange = 0L;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            final CacheKey cacheKey3 = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, 250, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(3, cacheService.freeLargeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, 250, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.freeLargeRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey3, 250, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());

            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();

            final CacheFileRegion region0Again = cacheService.get(cacheKey1, 250, 0, cacheHeaderRange, footerCacheRange);
            assertSame(region0Again, region0);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            final CacheFileRegion region2Again = cacheService.get(cacheKey3, 250, 2, cacheHeaderRange, footerCacheRange);
            assertSame(region2Again, region2);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            cacheService.get(cacheKey1, 250, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey1, 250, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey3, 250, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.getFreq(region2));
            cacheService.get(cacheKey3, 250, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.getFreq(region2));

            // advance 2 ticks (decay only starts after 2 ticks)
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(2, cacheService.getFreq(region2));

            // advance another tick
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            // advance another tick
            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));
        }
    }

    public void testFooterCachedSeparately() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "2000b")
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 10L;
        final long footerCacheRange = 10L;
        final long fileLength = 250L;
        // should be cached as 10 - 100 - 100 - 100 (30) - 10
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(16, cacheService.freeLargeRegionCount());
            assertEquals(8, cacheService.freeSmallRegionCount());
            assertEquals(2, cacheService.freeTinyRegionCount());

            final CacheFileRegion region0 = cacheService.get(cacheKey, fileLength, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(10L, region0.tracker.getLength());
            assertEquals(16, cacheService.freeLargeRegionCount());
            assertEquals(8, cacheService.freeSmallRegionCount());
            assertEquals(1, cacheService.freeTinyRegionCount());

            final CacheFileRegion region1 = cacheService.get(cacheKey, fileLength, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region1.tracker.getLength());
            assertEquals(15, cacheService.freeLargeRegionCount());
            assertEquals(8, cacheService.freeSmallRegionCount());
            assertEquals(1, cacheService.freeTinyRegionCount());

            final CacheFileRegion region2 = cacheService.get(cacheKey, fileLength, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(100L, region2.tracker.getLength());
            assertEquals(14, cacheService.freeLargeRegionCount());
            assertEquals(8, cacheService.freeSmallRegionCount());
            assertEquals(1, cacheService.freeTinyRegionCount());

            final CacheFileRegion region3 = cacheService.get(cacheKey, fileLength, 3, cacheHeaderRange, footerCacheRange);
            assertEquals(30L, region3.tracker.getLength());
            assertEquals(13, cacheService.freeLargeRegionCount());
            assertEquals(8, cacheService.freeSmallRegionCount());
            assertEquals(1, cacheService.freeTinyRegionCount());

            final CacheFileRegion region4 = cacheService.get(cacheKey, fileLength, 4, cacheHeaderRange, footerCacheRange);
            assertEquals(10L, region4.tracker.getLength());
            assertEquals(13, cacheService.freeLargeRegionCount());
            assertEquals(8, cacheService.freeSmallRegionCount());
            assertEquals(0, cacheService.freeTinyRegionCount());
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
