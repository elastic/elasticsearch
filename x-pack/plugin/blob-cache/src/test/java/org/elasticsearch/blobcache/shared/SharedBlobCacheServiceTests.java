/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SharedBlobCacheServiceTests extends ESTestCase {

    private static long size(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    public void testBasicEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(environment, settings, taskQueue.getThreadPool(), ThreadPool.Names.GENERIC)
        ) {
            final var cacheKey = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey, size(250), 0);
            assertEquals(size(100), region0.tracker.getLength());
            assertEquals(4, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey, size(250), 1);
            assertEquals(size(100), region1.tracker.getLength());
            assertEquals(3, cacheService.freeRegionCount());
            final var region2 = cacheService.get(cacheKey, size(250), 2);
            assertEquals(size(50), region2.tracker.getLength());
            assertEquals(2, cacheService.freeRegionCount());

            synchronized (cacheService) {
                assertTrue(region1.tryEvict());
            }
            assertEquals(3, cacheService.freeRegionCount());
            synchronized (cacheService) {
                assertFalse(region1.tryEvict());
            }
            assertEquals(3, cacheService.freeRegionCount());
            final var bytesReadFuture = new PlainActionFuture<Integer>();
            region0.populateAndRead(
                ByteRange.of(0L, 1L),
                ByteRange.of(0L, 1L),
                (channel, channelPos, relativePos, length) -> 1,
                (channel, channelPos, relativePos, length, progressUpdater) -> progressUpdater.accept(length),
                bytesReadFuture
            );
            synchronized (cacheService) {
                assertFalse(region0.tryEvict());
            }
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(bytesReadFuture.isDone());
            taskQueue.runAllRunnableTasks();
            synchronized (cacheService) {
                assertTrue(region0.tryEvict());
            }
            assertEquals(4, cacheService.freeRegionCount());
            synchronized (cacheService) {
                assertTrue(region2.tryEvict());
            }
            assertEquals(5, cacheService.freeRegionCount());
            assertTrue(bytesReadFuture.isDone());
            assertEquals(Integer.valueOf(1), bytesReadFuture.actionGet());
        }
    }

    public void testAutoEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(200)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(environment, settings, taskQueue.getThreadPool(), ThreadPool.Names.GENERIC)
        ) {
            final var cacheKey = generateCacheKey();
            assertEquals(2, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey, size(250), 0);
            assertEquals(size(100), region0.tracker.getLength());
            assertEquals(1, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey, size(250), 1);
            assertEquals(size(100), region1.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertFalse(region0.isEvicted());
            assertFalse(region1.isEvicted());

            // acquire region 2, which should evict region 0 (oldest)
            final var region2 = cacheService.get(cacheKey, size(250), 2);
            assertEquals(size(50), region2.tracker.getLength());
            assertEquals(0, cacheService.freeRegionCount());
            assertTrue(region0.isEvicted());
            assertFalse(region1.isEvicted());

            // explicitly evict region 1
            synchronized (cacheService) {
                assertTrue(region1.tryEvict());
            }
            assertEquals(1, cacheService.freeRegionCount());
        }
    }

    public void testForceEviction() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(environment, settings, taskQueue.getThreadPool(), ThreadPool.Names.GENERIC)
        ) {
            final var cacheKey1 = generateCacheKey();
            final var cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey2, size(250), 1);
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
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(environment, settings, taskQueue.getThreadPool(), ThreadPool.Names.GENERIC)
        ) {
            final var cacheKey1 = generateCacheKey();
            final var cacheKey2 = generateCacheKey();
            assertEquals(5, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(3, cacheService.freeRegionCount());

            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));

            taskQueue.advanceTime();
            taskQueue.runAllRunnableTasks();

            final var region0Again = cacheService.get(cacheKey1, size(250), 0);
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

    /**
     * Exercise SharedBlobCacheService#get in multiple threads to trigger any assertion errors.
     * @throws IOException
     */
    public void testGetMultiThreaded() throws IOException {
        int threads = between(2, 10);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(
                SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                ByteSizeValue.ofBytes(size(between(1, 20) * 100L)).getStringRep()
            )
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MIN_TIME_DELTA_SETTING.getKey(), randomFrom("0", "1ms", "10s"))
            .put("path.home", createTempDir())
            .build();
        long fileLength = size(500);
        ThreadPool threadPool = new TestThreadPool("testGetMultiThreaded");
        Set<String> files = randomSet(1, 10, () -> randomAlphaOfLength(5));
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<String>(environment, settings, threadPool, ThreadPool.Names.GENERIC)
        ) {
            CyclicBarrier ready = new CyclicBarrier(threads);
            List<Thread> threadList = IntStream.range(0, threads).mapToObj(no -> {
                int iterations = between(100, 500);
                String[] cacheKeys = IntStream.range(0, iterations).mapToObj(ignore -> randomFrom(files)).toArray(String[]::new);
                int[] regions = IntStream.range(0, iterations).map(ignore -> between(0, 4)).toArray();
                int[] yield = IntStream.range(0, iterations).map(ignore -> between(0, 9)).toArray();
                int[] evict = IntStream.range(0, iterations).map(ignore -> between(0, 99)).toArray();
                return new Thread(() -> {
                    try {
                        ready.await();
                        for (int i = 0; i < iterations; ++i) {
                            try {
                                SharedBlobCacheService<String>.CacheFileRegion cacheFileRegion = cacheService.get(
                                    cacheKeys[i],
                                    fileLength,
                                    regions[i]
                                );
                                if (cacheFileRegion.tryIncRef()) {
                                    if (yield[i] == 0) {
                                        Thread.yield();
                                    }
                                    cacheFileRegion.decRef();
                                }
                                if (evict[i] == 0) {
                                    cacheService.forceEvict(x -> true);
                                }
                            } catch (AlreadyClosedException e) {
                                // ignore
                            }
                        }
                    } catch (InterruptedException | BrokenBarrierException e) {
                        assert false;
                        throw new RuntimeException(e);
                    }
                });
            }).toList();
            threadList.forEach(Thread::start);
            threadList.forEach(thread -> {
                try {
                    thread.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        } finally {
            threadPool.shutdownNow();
        }
    }

    public void testCacheSizeRejectedOnNonFrozenNodes() {
        String cacheSize = randomBoolean()
            ? ByteSizeValue.ofBytes(size(500)).getStringRep()
            : (new RatioValue(between(1, 100))).formatNoTrailingZerosPercent();
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_HOT_NODE_ROLE.roleName())
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "Setting ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
                    + "] to be positive ["
                    + cacheSize
                    + "] is only permitted on nodes with the data_frozen, search, or indexing role. Roles are [data_hot]"
            )
        );
    }

    public void testMultipleDataPathsRejectedOnFrozenNodes() {
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .putList(Environment.PATH_DATA_SETTING.getKey(), List.of("a", "b"))
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "setting ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
                    + "="
                    + ByteSizeValue.ofBytes(size(500)).getStringRep()
                    + "] is not permitted on nodes with multiple data paths [a,b]"
            )
        );
    }

    public void testDedicateFrozenCacheSizeDefaults() {
        final Settings settings = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();

        RelativeByteSizeValue relativeCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isAbsolute(), is(false));
        assertThat(relativeCacheSize.isNonZeroSize(), is(true));
        assertThat(relativeCacheSize.calculateValue(ByteSizeValue.ofBytes(10000), null), equalTo(ByteSizeValue.ofBytes(9000)));
        assertThat(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofGb(100)));
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

        RelativeByteSizeValue relativeCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isNonZeroSize(), is(false));
        assertThat(relativeCacheSize.isAbsolute(), is(true));
        assertThat(relativeCacheSize.getAbsolute(), equalTo(ByteSizeValue.ZERO));
        assertThat(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofBytes(-1)));
    }

    public void testSearchOrIndexNodeCacheSizeDefaults() {
        final Settings settings = Settings.builder()
            .putList(
                NodeRoleSettings.NODE_ROLES_SETTING.getKey(),
                randomFrom(DiscoveryNodeRole.SEARCH_ROLE, DiscoveryNodeRole.INDEX_ROLE).roleName()
            )
            .build();

        RelativeByteSizeValue relativeCacheSize = SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.get(settings);
        assertThat(relativeCacheSize.isAbsolute(), is(false));
        assertThat(relativeCacheSize.isNonZeroSize(), is(true));
        assertThat(relativeCacheSize.calculateValue(ByteSizeValue.ofBytes(10000), null), equalTo(ByteSizeValue.ofBytes(9000)));
        assertThat(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings), equalTo(ByteSizeValue.ofGb(100)));
    }

    public void testMaxHeadroomRejectedForAbsoluteCacheSize() {
        String cacheSize = ByteSizeValue.ofBytes(size(500)).getStringRep();
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), cacheSize)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.get(settings)
        );
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(
            e.getCause().getMessage(),
            is(
                "setting ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_MAX_HEADROOM_SETTING.getKey()
                    + "] cannot be specified for absolute ["
                    + SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey()
                    + "="
                    + cacheSize
                    + "]"
            )
        );
    }

    public void testCalculateCacheSize() {
        long smallSize = 10000;
        long largeSize = ByteSizeValue.ofTb(10).getBytes();
        assertThat(SharedBlobCacheService.calculateCacheSize(Settings.EMPTY, smallSize), equalTo(0L));
        final Settings settings = Settings.builder()
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .build();
        assertThat(SharedBlobCacheService.calculateCacheSize(settings, smallSize), equalTo(9000L));
        assertThat(SharedBlobCacheService.calculateCacheSize(settings, largeSize), equalTo(largeSize - ByteSizeValue.ofGb(100).getBytes()));
    }

    private static Object generateCacheKey() {
        return new Object();
    }

    public void testCacheSizeChanges() throws IOException {
        ByteSizeValue val1 = new ByteSizeValue(randomIntBetween(1, 5), ByteSizeUnit.MB);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), val1.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            SharedBlobCacheService<?> cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ThreadPool.Names.GENERIC
            )
        ) {
            assertEquals(val1.getBytes(), cacheService.getStats().size());
        }

        ByteSizeValue val2 = new ByteSizeValue(randomIntBetween(1, 5), ByteSizeUnit.MB);
        settings = Settings.builder()
            .put(settings)
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), val2.getStringRep())
            .build();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            SharedBlobCacheService<?> cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                ThreadPool.Names.GENERIC
            )
        ) {
            assertEquals(val2.getBytes(), cacheService.getStats().size());
        }
    }

    private void assertThatNonPositiveRecoveryRangeSizeRejected(Setting<ByteSizeValue> setting) {
        final String value = randomFrom(ByteSizeValue.MINUS_ONE, ByteSizeValue.ZERO).getStringRep();
        final Settings settings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
            .put(setting.getKey(), value)
            .build();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> setting.get(settings));
        assertThat(e.getCause(), notNullValue());
        assertThat(e.getCause(), instanceOf(SettingsException.class));
        assertThat(e.getCause().getMessage(), is("setting [" + setting.getKey() + "] must be greater than zero"));
    }

    public void testNonPositiveRegionSizeRejected() {
        assertThatNonPositiveRecoveryRangeSizeRejected(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING);
    }

    public void testNonPositiveRangeSizeRejected() {
        assertThatNonPositiveRecoveryRangeSizeRejected(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING);
    }

    public void testNonPositiveRecoveryRangeSizeRejected() {
        assertThatNonPositiveRecoveryRangeSizeRejected(SharedBlobCacheService.SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING);
    }

}
