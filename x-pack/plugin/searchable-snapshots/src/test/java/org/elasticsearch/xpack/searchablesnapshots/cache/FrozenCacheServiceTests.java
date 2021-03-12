/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.cluster.coordination.DeterministicTaskQueue;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.CacheKey;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.CacheFileRegion;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.cache.SharedCacheConfiguration.SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE;
import static org.elasticsearch.xpack.searchablesnapshots.cache.SharedCacheConfiguration.SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE;

public class FrozenCacheServiceTests extends ESTestCase {

    public void testBasicEviction() throws IOException {
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(10 * SharedCacheConfiguration.SMALL_REGION_SIZE);
        final ByteSizeValue largeRegionSize = ByteSizeValue.ofBytes(2 * SharedCacheConfiguration.SMALL_REGION_SIZE);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), largeRegionSize)
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put(SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE.getKey(), 0.001f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 0L;
        final long footerCacheRange = 0L;
        final long fileSize = 5 * largeRegionSize.getBytes() / 2;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region0.tracker.getLength());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, fileSize, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region1.tracker.getLength());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, fileSize, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes() / 2, region2.tracker.getLength());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());

            assertTrue(region1.tryEvict());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertFalse(region1.tryEvict());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            /*
            TODO: rework this
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
            */
        }
    }

    public void testHeaderOnlyFile() throws Exception {
        final ByteSizeValue cacheSize = ByteSizeValue.ofBytes(10 * SharedCacheConfiguration.SMALL_REGION_SIZE);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(2 * SharedCacheConfiguration.SMALL_REGION_SIZE))
            .put(SNAPSHOT_CACHE_SMALL_REGION_SIZE_SHARE.getKey(), 0.2f)
            .put(SNAPSHOT_CACHE_TINY_REGION_SIZE_SHARE.getKey(), 0.001f)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = SharedCacheConfiguration.TINY_REGION_SIZE;
        final long footerCacheRange = SharedCacheConfiguration.TINY_REGION_SIZE;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(1, cacheService.freeTinyRegionCount());
            final long fileLength1 = SharedCacheConfiguration.TINY_REGION_SIZE - 2;
            final CacheFileRegion region0 = cacheService.get(generateCacheKey(), fileLength1, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(fileLength1, region0.tracker.getLength());
            assertEquals(0, cacheService.freeTinyRegionCount());
            assertFalse(region0.isEvicted());

            final CacheFileRegion region1 = cacheService.get(
                generateCacheKey(),
                SharedCacheConfiguration.TINY_REGION_SIZE,
                0,
                cacheHeaderRange,
                footerCacheRange
            );
            assertEquals(SharedCacheConfiguration.TINY_REGION_SIZE, region1.tracker.getLength());
            assertTrue(region0.isEvicted());

            final CacheFileRegion region2 = cacheService.get(generateCacheKey(), 7, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(7L, region2.tracker.getLength());
            assertEquals(0, cacheService.freeTinyRegionCount());
            assertTrue(region0.isEvicted());
            assertTrue(region1.isEvicted());

            final long file3Length = SharedCacheConfiguration.TINY_REGION_SIZE + 1;
            final CacheFileRegion region3 = cacheService.get(generateCacheKey(), file3Length, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(1L, region3.tracker.getLength());
            assertEquals(0, cacheService.freeTinyRegionCount());
            assertTrue(region0.isEvicted());
            assertTrue(region1.isEvicted());
            assertTrue(region2.isEvicted());
        }
    }

    public void testAutoEviction() throws IOException {
        final ByteSizeValue largeRegionSize = ByteSizeValue.ofBytes(SharedCacheConfiguration.SMALL_REGION_SIZE * 2);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(largeRegionSize.getBytes() * 5))
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), largeRegionSize)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 0;
        final long footerCacheRange = 0L;
        final long fileSize = 5 * largeRegionSize.getBytes();
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region0.tracker.getLength());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey, fileSize, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region1.tracker.getLength());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey, fileSize, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region1.tracker.getLength());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            final CacheFileRegion region3 = cacheService.get(cacheKey, fileSize, 3, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region1.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());

            // acquire region 4, which should evict region 0 (oldest)
            final CacheFileRegion region4 = cacheService.get(cacheKey, fileSize, 4, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region4.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
            assertTrue(region0.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());
            assertFalse(region4.isEvicted());

            // explicitly evict region 1
            assertTrue(region1.tryEvict());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(1, cacheService.freeSmallRegionCount());
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
            assertEquals(6, cacheService.freeTinyRegionCount());

            final long fileSize = 5 * largeRegionSize.getBytes();
            final CacheFileRegion region0 = cacheService.get(cacheKey, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(cacheHeaderRange, region0.tracker.getLength());
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            final CacheFileRegion region1 = cacheService.get(cacheKey, fileSize, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region1.tracker.getLength());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            final CacheFileRegion region2 = cacheService.get(cacheKey, fileSize, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region2.tracker.getLength());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            final CacheFileRegion region3 = cacheService.get(cacheKey, fileSize, 3, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region3.tracker.getLength());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());
            assertFalse(region2.isEvicted());
            assertFalse(region3.isEvicted());

            final CacheFileRegion region4 = cacheService.get(cacheKey, fileSize, 4, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region4.tracker.getLength());
            assertEquals(5, cacheService.freeTinyRegionCount());

            // acquire region 5 which should evict region 1 (oldest large region)
            final CacheFileRegion region5 = cacheService.get(cacheKey, fileSize, 5, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes() - SharedCacheConfiguration.TINY_REGION_SIZE, region5.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            assertTrue(region1.isEvicted());

            final CacheKey otherKey = generateCacheKey();
            final CacheFileRegion otherRegion0 = cacheService.get(otherKey, 49, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(49, otherRegion0.tracker.getLength());
            assertEquals(0, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(4, cacheService.freeTinyRegionCount());

            assertFalse(region2.isEvicted());

            // acquire region 1 of second file which should evict region 2 (oldest large region)
            final CacheFileRegion otherRegion1 = cacheService.get(
                otherKey,
                SharedCacheConfiguration.TINY_REGION_SIZE + 49,
                1,
                cacheHeaderRange,
                footerCacheRange
            );
            assertEquals(49L, otherRegion1.tracker.getLength());
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

            // explicitly evict region 0 (tiny) in the second file
            assertTrue(otherRegion0.tryEvict());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());
        }
    }

    public void testForceEviction() throws IOException {
        final ByteSizeValue largeRegionSize = ByteSizeValue.ofBytes(SharedCacheConfiguration.SMALL_REGION_SIZE * 2);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(largeRegionSize.getBytes() * 5))
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), largeRegionSize)
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue(settings, random());
        final Environment environment = TestEnvironment.newEnvironment(settings);
        for (Path path : environment.dataFiles()) {
            Files.createDirectories(path);
        }
        final long cacheHeaderRange = 0;
        final long footerCacheRange = 0L;
        final long fileSize = (largeRegionSize.getBytes() / 2) * 5;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(3, cacheService.freeLargeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, fileSize, 1, cacheHeaderRange, footerCacheRange);
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
        final ByteSizeValue largeRegionSize = ByteSizeValue.ofBytes(SharedCacheConfiguration.SMALL_REGION_SIZE * 2);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SNAPSHOT_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(largeRegionSize.getBytes() * 5))
            .put(SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), largeRegionSize)
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
        final long fileSize = (largeRegionSize.getBytes() / 2) * 5;
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey1 = generateCacheKey();
            final CacheKey cacheKey2 = generateCacheKey();
            final CacheKey cacheKey3 = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            final CacheFileRegion region0 = cacheService.get(cacheKey1, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(3, cacheService.freeLargeRegionCount());
            final CacheFileRegion region1 = cacheService.get(cacheKey2, fileSize, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.freeLargeRegionCount());
            final CacheFileRegion region2 = cacheService.get(cacheKey3, fileSize, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());

            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();

            final CacheFileRegion region0Again = cacheService.get(cacheKey1, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertSame(region0Again, region0);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            final CacheFileRegion region2Again = cacheService.get(cacheKey3, fileSize, 2, cacheHeaderRange, footerCacheRange);
            assertSame(region2Again, region2);
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();
            cacheService.get(cacheKey1, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey1, fileSize, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.getFreq(region0));
            cacheService.get(cacheKey3, fileSize, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(2, cacheService.getFreq(region2));
            cacheService.get(cacheKey3, fileSize, 2, cacheHeaderRange, footerCacheRange);
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
        final long footerCacheRange = SharedCacheConfiguration.TINY_REGION_SIZE;
        final long fileLength = ByteSizeValue.ofKb(330L).getBytes();
        // should be cached as 1k - 128k - 128k - 128k (72k) - 1k
        try (FrozenCacheService cacheService = new FrozenCacheService(environment, taskQueue.getThreadPool())) {
            final CacheKey cacheKey = generateCacheKey();
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(6, cacheService.freeTinyRegionCount());

            final CacheFileRegion region0 = cacheService.get(cacheKey, fileLength, 0, cacheHeaderRange, footerCacheRange);
            assertEquals(SharedCacheConfiguration.TINY_REGION_SIZE, region0.tracker.getLength());
            assertEquals(4, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            final CacheFileRegion region1 = cacheService.get(cacheKey, fileLength, 1, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region1.tracker.getLength());
            assertEquals(3, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            final CacheFileRegion region2 = cacheService.get(cacheKey, fileLength, 2, cacheHeaderRange, footerCacheRange);
            assertEquals(largeRegionSize.getBytes(), region2.tracker.getLength());
            assertEquals(2, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            final CacheFileRegion region3 = cacheService.get(cacheKey, fileLength, 3, cacheHeaderRange, footerCacheRange);
            assertEquals(
                fileLength - largeRegionSize.getBytes() * 2 - SharedCacheConfiguration.TINY_REGION_SIZE * 2,
                region3.tracker.getLength()
            );
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(5, cacheService.freeTinyRegionCount());

            final CacheFileRegion region4 = cacheService.get(cacheKey, fileLength, 4, cacheHeaderRange, footerCacheRange);
            assertEquals(SharedCacheConfiguration.TINY_REGION_SIZE, region4.tracker.getLength());
            assertEquals(1, cacheService.freeLargeRegionCount());
            assertEquals(2, cacheService.freeSmallRegionCount());
            assertEquals(4, cacheService.freeTinyRegionCount());
        }
    }

    public void testCacheSizeDeprecatedOnNonFrozenNodes() {
        DiscoveryNode.setAdditionalRoles(
            Set.of(DataTier.DATA_HOT_NODE_ROLE, DataTier.DATA_WARM_NODE_ROLE, DataTier.DATA_COLD_NODE_ROLE, DataTier.DATA_FROZEN_NODE_ROLE)
        );
        final Settings settings = Settings.builder()
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), "500b")
            .put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), "100b")
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DataTier.DATA_HOT_NODE_ROLE.roleName())
            .build();
        FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(settings);
        assertWarnings(
            "setting ["
                + FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey()
                + "] to be positive [500b] on node without the data_frozen role is deprecated, roles are [data_hot]"
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
