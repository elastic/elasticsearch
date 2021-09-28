/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.shared;

import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.common.CacheKey;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService.CacheFileRegion;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
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
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
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
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
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
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
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

    public void testCacheSizeRejectedOnNonFrozenNodes() {
        String cacheSize = randomBoolean() ? new ByteSizeValue(size(500)).getStringRep() : new RatioValue(between(1, 100)).toString();
        final Settings settings = Settings.builder()
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(FrozenCacheService.SNAPSHOT_CACHE_REGION_SIZE_SETTING.getKey(), new ByteSizeValue(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_HOT_NODE_ROLE.roleName())
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "setting ["
                    + FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey()
                    + "] to be positive ["
                    + cacheSize
                    + "] is only permitted on nodes with the data_frozen role, roles are [data_hot]"
            )
        );
    }

    public void testDedicateFrozenCacheSizeDefaults() {
        final Settings settings = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();

        RelativeByteSizeValue relativeCacheSize = FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isAbsolute(), is(false));
        assertThat(relativeCacheSize.isNonZeroSize(), is(true));
        assertThat(relativeCacheSize.calculateValue(ByteSizeValue.ofBytes(10000), null), equalTo(ByteSizeValue.ofBytes(9000)));
        assertThat(FrozenCacheService.SNAPSHOT_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofGb(100)));
    }

    public void testNotDedicatedFrozenCacheSizeDefaults() {
        final Settings settings = Settings.builder()
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                Sets.union(
                    Set.of(
                        randomFrom(
                            DiscoveryNodeRole.DATA_HOT_NODE_ROLE,
                            DiscoveryNodeRole.DATA_COLD_NODE_ROLE,
                            DiscoveryNodeRole.DATA_WARM_NODE_ROLE,
                            DiscoveryNodeRole.DATA_CONTENT_NODE_ROLE
                        )
                    ),
                    new HashSet<>(
                        randomSubsetOf(
                            between(0, 3),
                            DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE,
                            DiscoveryNodeRole.INGEST_ROLE,
                            DiscoveryNodeRole.MASTER_ROLE
                        )
                    )
                ).stream().map(DiscoveryNodeRole::roleName).collect(Collectors.toList())
            )
            .build();

        RelativeByteSizeValue relativeCacheSize = FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isNonZeroSize(), is(false));
        assertThat(relativeCacheSize.isAbsolute(), is(true));
        assertThat(relativeCacheSize.getAbsolute(), equalTo(ByteSizeValue.ZERO));
        assertThat(FrozenCacheService.SNAPSHOT_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofBytes(-1)));
    }

    public void testMaxHeadroomRejectedForAbsoluteCacheSize() {
        String cacheSize = new ByteSizeValue(size(500)).getStringRep();
        final Settings settings = Settings.builder()
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(FrozenCacheService.SNAPSHOT_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey(), new ByteSizeValue(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> FrozenCacheService.SNAPSHOT_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "setting ["
                    + FrozenCacheService.SNAPSHOT_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey()
                    + "] cannot be specified for absolute ["
                    + FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey()
                    + "="
                    + cacheSize
                    + "]"
            )
        );
    }

    public void testCalculateCacheSize() {
        long smallSize = 10000;
        long largeSize = ByteSizeValue.ofTb(10).getBytes();
        assertThat(FrozenCacheService.calculateCacheSize(Settings.EMPTY, smallSize), equalTo(0L));
        final Settings settings = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();
        assertThat(FrozenCacheService.calculateCacheSize(settings, smallSize), equalTo(9000L));
        assertThat(FrozenCacheService.calculateCacheSize(settings, largeSize), equalTo(largeSize - ByteSizeValue.ofGb(100).getBytes()));
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
