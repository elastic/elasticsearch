/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobcache.shared;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.common.SparseFileTracker;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.RangeMissingHandler;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService.SourceInputStreamFactory;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RatioValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.StoppableExecutorServiceWrapper;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SharedBlobCacheServiceTests extends ESTestCase {

    private static long size(long numPages) {
        return numPages * SharedBytes.PAGE_SIZE;
    }

    private static <E extends Exception> void completeWith(ActionListener<Void> listener, CheckedRunnable<E> runnable) {
        ActionListener.completeWith(listener, () -> {
            runnable.run();
            return null;
        });
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
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
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
                assertTrue(tryEvict(region1));
            }
            assertEquals(3, cacheService.freeRegionCount());
            synchronized (cacheService) {
                assertFalse(tryEvict(region1));
            }
            assertEquals(3, cacheService.freeRegionCount());
            final var bytesReadFuture = new PlainActionFuture<Integer>();
            region0.populateAndRead(
                ByteRange.of(0L, 1L),
                ByteRange.of(0L, 1L),
                (channel, channelPos, relativePos, length) -> 1,
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> progressUpdater.accept(length)
                ),
                taskQueue.getThreadPool().generic(),
                bytesReadFuture
            );
            synchronized (cacheService) {
                assertFalse(tryEvict(region0));
            }
            assertEquals(3, cacheService.freeRegionCount());
            assertFalse(bytesReadFuture.isDone());
            taskQueue.runAllRunnableTasks();
            synchronized (cacheService) {
                assertTrue(tryEvict(region0));
            }
            assertEquals(4, cacheService.freeRegionCount());
            synchronized (cacheService) {
                assertTrue(tryEvict(region2));
            }
            assertEquals(5, cacheService.freeRegionCount());
            assertTrue(bytesReadFuture.isDone());
            assertEquals(Integer.valueOf(1), bytesReadFuture.actionGet());
        }
    }

    private static boolean tryEvict(SharedBlobCacheService.CacheFileRegion<Object> region1) {
        if (randomBoolean()) {
            return region1.tryEvict();
        } else {
            boolean result = region1.tryEvictNoDecRef();
            if (result) {
                region1.decRef();
            }
            return result;
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
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
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
                assertTrue(tryEvict(region1));
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
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
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

    public void testForceEvictResponse() throws IOException {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
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

            assertEquals(1, cacheService.forceEvict(cK -> cK == cacheKey1));
            assertEquals(1, cacheService.forceEvict(e -> true));
        }
    }

    public void testDecay() throws IOException {
        // we have 8 regions
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(400)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            assertEquals(4, cacheService.freeRegionCount());

            final var cacheKey1 = generateCacheKey();
            final var cacheKey2 = generateCacheKey();
            final var cacheKey3 = generateCacheKey();
            // add a region that we can evict when provoking first decay
            cacheService.get("evictkey", size(250), 0);
            assertEquals(3, cacheService.freeRegionCount());
            final var region0 = cacheService.get(cacheKey1, size(250), 0);
            assertEquals(2, cacheService.freeRegionCount());
            final var region1 = cacheService.get(cacheKey2, size(250), 1);
            assertEquals(1, cacheService.freeRegionCount());
            final var region2 = cacheService.get(cacheKey3, size(250), 1);
            assertEquals(0, cacheService.freeRegionCount());

            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));
            AtomicLong expectedEpoch = new AtomicLong();
            Runnable triggerDecay = () -> {
                assertThat(taskQueue.hasRunnableTasks(), is(false));
                cacheService.get(expectedEpoch.toString(), size(250), 0);
                assertThat(taskQueue.hasRunnableTasks(), is(true));
                taskQueue.runAllRunnableTasks();
                assertThat(cacheService.epoch(), equalTo(expectedEpoch.incrementAndGet()));
            };

            triggerDecay.run();

            cacheService.get(cacheKey1, size(250), 0);
            cacheService.get(cacheKey2, size(250), 1);
            cacheService.get(cacheKey3, size(250), 1);

            triggerDecay.run();

            final var region0Again = cacheService.get(cacheKey1, size(250), 0);
            assertSame(region0Again, region0);
            assertEquals(3, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            triggerDecay.run();

            cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.getFreq(region0));
            cacheService.get(cacheKey1, size(250), 0);
            assertEquals(4, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            // ensure no freq=0 entries
            cacheService.get(cacheKey2, size(250), 1);
            cacheService.get(cacheKey3, size(250), 1);
            assertEquals(2, cacheService.getFreq(region1));
            assertEquals(2, cacheService.getFreq(region2));

            triggerDecay.run();

            assertEquals(3, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            triggerDecay.run();
            assertEquals(2, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));

            // ensure no freq=0 entries
            cacheService.get(cacheKey2, size(250), 1);
            cacheService.get(cacheKey3, size(250), 1);
            assertEquals(2, cacheService.getFreq(region1));
            assertEquals(2, cacheService.getFreq(region2));

            triggerDecay.run();
            assertEquals(1, cacheService.getFreq(region0));
            assertEquals(1, cacheService.getFreq(region1));
            assertEquals(1, cacheService.getFreq(region2));

            triggerDecay.run();
            assertEquals(0, cacheService.getFreq(region0));
            assertEquals(0, cacheService.getFreq(region1));
            assertEquals(0, cacheService.getFreq(region2));
        }
    }

    /**
     * Test when many objects need to decay, in particular useful to measure how long the decay task takes.
     * For 1M objects (with no assertions) it took 26ms locally.
     */
    public void testMassiveDecay() throws IOException {
        int regions = 1024; // to measure decay time, increase to 1024*1024 and disable assertions.
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(regions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(1)).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            Runnable decay = () -> {
                assertThat(taskQueue.hasRunnableTasks(), is(true));
                long before = System.currentTimeMillis();
                taskQueue.runAllRunnableTasks();
                long after = System.currentTimeMillis();
                logger.debug("took {} ms", (after - before));
            };
            long fileLength = size(regions + 100);
            Object cacheKey = new Object();
            for (int i = 0; i < regions; ++i) {
                cacheService.get(cacheKey, fileLength, i);
                if (Integer.bitCount(i) == 1) {
                    logger.debug("did {} gets", i);
                }
            }
            assertThat(taskQueue.hasRunnableTasks(), is(false));
            cacheService.get(cacheKey, fileLength, regions);
            decay.run();
            int maxRounds = 5;
            for (int round = 2; round <= maxRounds; ++round) {
                for (int i = round; i < regions + round; ++i) {
                    cacheService.get(cacheKey, fileLength, i);
                    if (Integer.bitCount(i) == 1) {
                        logger.debug("did {} gets", i);
                    }
                }
                decay.run();
            }

            Map<Integer, Integer> freqs = new HashMap<>();
            for (int i = maxRounds; i < regions + maxRounds; ++i) {
                int freq = cacheService.getFreq(cacheService.get(cacheKey, fileLength, i)) - 2;
                freqs.compute(freq, (k, v) -> v == null ? 1 : v + 1);
                if (Integer.bitCount(i) == 1) {
                    logger.debug("did {} gets", i);
                }
            }
            assertThat(freqs.get(4), equalTo(regions - maxRounds + 1));
        }
    }

    /**
     * Exercise SharedBlobCacheService#get in multiple threads to trigger any assertion errors.
     * @throws IOException
     */
    public void testGetMultiThreaded() throws IOException {
        final int threads = between(2, 10);
        final int regionCount = between(1, 20);
        final boolean incRef = randomBoolean();
        // if we have enough regions, a get should always have a result (except for explicit evict interference)
        // if we incRef, we risk the eviction racing against that, leading to no available region, so allow
        // the already closed exception in that case.
        final boolean allowAlreadyClosed = regionCount < threads || incRef;

        logger.info("{} {} {}", threads, regionCount, allowAlreadyClosed);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(regionCount * 100L)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_MIN_TIME_DELTA_SETTING.getKey(), randomFrom("0", "1ms", "10s"))
            .put("path.home", createTempDir())
            .build();
        long fileLength = size(500);
        ThreadPool threadPool = new TestThreadPool("testGetMultiThreaded");
        Set<String> files = randomSet(1, 10, () -> randomAlphaOfLength(5));
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<String>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
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
                                SharedBlobCacheService.CacheFileRegion<String> cacheFileRegion;
                                try {
                                    cacheFileRegion = cacheService.get(cacheKeys[i], fileLength, regions[i]);
                                } catch (AlreadyClosedException e) {
                                    assert allowAlreadyClosed || e.getMessage().equals("evicted during free region allocation") : e;
                                    throw e;
                                }
                                assertTrue(cacheFileRegion.testOnlyNonVolatileIO() != null || cacheFileRegion.isEvicted());
                                if (incRef && cacheFileRegion.tryIncRef()) {
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

    public void testFetchFullCacheEntry() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();

        final var bulkTaskCount = new AtomicInteger(0);
        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic()) {
            @Override
            public void execute(Runnable command) {
                super.execute(command);
                bulkTaskCount.incrementAndGet();
            }
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                final var cacheKey = generateCacheKey();
                assertEquals(5, cacheService.freeRegionCount());
                final long size = size(250);
                AtomicLong bytesRead = new AtomicLong(size);
                final PlainActionFuture<Void> future = new PlainActionFuture<>();
                cacheService.maybeFetchFullEntry(
                    cacheKey,
                    size,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assert streamFactory == null : streamFactory;
                            bytesRead.addAndGet(-length);
                            progressUpdater.accept(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );

                future.get(10, TimeUnit.SECONDS);
                assertEquals(0L, bytesRead.get());
                assertEquals(2, cacheService.freeRegionCount());
                assertEquals(3, bulkTaskCount.get());
            }
            {
                // a download that would use up all regions should not run
                final var cacheKey = generateCacheKey();
                assertEquals(2, cacheService.freeRegionCount());
                var configured = cacheService.maybeFetchFullEntry(
                    cacheKey,
                    size(500),
                    (ch, chPos, streamFactory, relPos, len, update, completionListener) -> completeWith(completionListener, () -> {
                        throw new AssertionError("Should never reach here");
                    }),
                    bulkExecutor,
                    ActionListener.noop()
                );
                assertFalse(configured);
                assertEquals(2, cacheService.freeRegionCount());
            }
        }

        threadPool.shutdown();
    }

    public void testFetchFullCacheEntryConcurrently() throws Exception {
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(500)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put("path.home", createTempDir())
            .build();

        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic());

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {

            final long size = size(randomIntBetween(1, 100));
            final Thread[] threads = new Thread[10];
            for (int i = 0; i < threads.length; i++) {
                threads[i] = new Thread(() -> {
                    for (int j = 0; j < 1000; j++) {
                        final var cacheKey = generateCacheKey();
                        safeAwait(
                            (ActionListener<Void> listener) -> cacheService.maybeFetchFullEntry(
                                cacheKey,
                                size,
                                (
                                    channel,
                                    channelPos,
                                    streamFactory,
                                    relativePos,
                                    length,
                                    progressUpdater,
                                    completionListener) -> completeWith(completionListener, () -> progressUpdater.accept(length)),
                                bulkExecutor,
                                listener
                            )
                        );
                    }
                });
            }
            for (Thread thread : threads) {
                thread.start();
            }
            for (Thread thread : threads) {
                thread.join();
            }
        } finally {
            assertTrue(ThreadPool.terminate(threadPool, 10L, TimeUnit.SECONDS));
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
        ByteSizeValue val1 = ByteSizeValue.of(randomIntBetween(1, 5), ByteSizeUnit.MB);
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
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            assertEquals(val1.getBytes(), cacheService.getStats().size());
        }

        ByteSizeValue val2 = ByteSizeValue.of(randomIntBetween(1, 5), ByteSizeUnit.MB);
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
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            assertEquals(val2.getBytes(), cacheService.getStats().size());
        }
    }

    public void testMaybeEvictLeastUsed() throws Exception {
        final int numRegions = 10;
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(numRegions)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<Object>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final Map<Object, SharedBlobCacheService.CacheFileRegion<Object>> cacheEntries = new HashMap<>();

            assertThat("All regions are free", cacheService.freeRegionCount(), equalTo(numRegions));
            assertThat("Cache has no entries", cacheService.maybeEvictLeastUsed(), is(false));

            // use all regions in cache
            for (int i = 0; i < numRegions; i++) {
                final var cacheKey = generateCacheKey();
                var entry = cacheService.get(cacheKey, regionSize, 0);
                entry.populate(
                    ByteRange.of(0L, regionSize),
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> progressUpdater.accept(length)
                    ),
                    taskQueue.getThreadPool().generic(),
                    ActionListener.noop()
                );
                assertThat(cacheService.getFreq(entry), equalTo(1));
                cacheEntries.put(cacheKey, entry);
            }

            assertThat("All regions are used", cacheService.freeRegionCount(), equalTo(0));
            assertThat("Cache entries are not old enough to be evicted", cacheService.maybeEvictLeastUsed(), is(false));

            taskQueue.runAllRunnableTasks();

            assertThat("All regions are used", cacheService.freeRegionCount(), equalTo(0));
            assertThat("Cache entries are not old enough to be evicted", cacheService.maybeEvictLeastUsed(), is(false));

            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            cacheEntries.keySet().forEach(key -> cacheService.get(key, regionSize, 0));
            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            // touch some random cache entries
            var usedCacheKeys = Set.copyOf(randomSubsetOf(cacheEntries.keySet()));
            usedCacheKeys.forEach(key -> cacheService.get(key, regionSize, 0));

            cacheEntries.forEach(
                (key, entry) -> assertThat(cacheService.getFreq(entry), usedCacheKeys.contains(key) ? equalTo(3) : equalTo(1))
            );

            assertThat("All regions are used", cacheService.freeRegionCount(), equalTo(0));
            assertThat("Cache entries are not old enough to be evicted", cacheService.maybeEvictLeastUsed(), is(false));

            cacheService.maybeScheduleDecayAndNewEpoch();
            taskQueue.runAllRunnableTasks();

            assertThat("All regions are used", cacheService.freeRegionCount(), equalTo(0));
            cacheEntries.forEach(
                (key, entry) -> assertThat(cacheService.getFreq(entry), usedCacheKeys.contains(key) ? equalTo(2) : equalTo(0))
            );

            var zeroFrequencyCacheEntries = cacheEntries.size() - usedCacheKeys.size();
            for (int i = 0; i < zeroFrequencyCacheEntries; i++) {
                assertThat(cacheService.freeRegionCount(), equalTo(i));
                assertThat("Cache entry is old enough to be evicted", cacheService.maybeEvictLeastUsed(), is(true));
                assertThat(cacheService.freeRegionCount(), equalTo(i + 1));
            }

            assertThat("No more cache entries old enough to be evicted", cacheService.maybeEvictLeastUsed(), is(false));
            assertThat(cacheService.freeRegionCount(), equalTo(zeroFrequencyCacheEntries));
        }
    }

    public void testMaybeFetchRegion() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put("path.home", createTempDir())
            .build();

        final var bulkTaskCount = new AtomicInteger(0);
        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic()) {
            @Override
            public void execute(Runnable command) {
                super.execute(command);
                bulkTaskCount.incrementAndGet();
            }
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                // fetch a single region
                final var cacheKey = generateCacheKey();
                assertEquals(5, cacheService.freeRegionCount());
                final long blobLength = size(250); // 3 regions
                AtomicLong bytesRead = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRegion(
                    cacheKey,
                    0,
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assert streamFactory == null : streamFactory;
                            bytesRead.addAndGet(length);
                            progressUpdater.accept(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(regionSize, bytesRead.get());
                assertEquals(4, cacheService.freeRegionCount());
                assertEquals(1, bulkTaskCount.get());
            }
            {
                // fetch multiple regions to used all the cache
                final int remainingFreeRegions = cacheService.freeRegionCount();
                assertEquals(4, cacheService.freeRegionCount());

                final var cacheKey = generateCacheKey();
                final long blobLength = regionSize * remainingFreeRegions;
                AtomicLong bytesRead = new AtomicLong(0L);

                final PlainActionFuture<Collection<Boolean>> future = new PlainActionFuture<>();
                final var listener = new GroupedActionListener<>(remainingFreeRegions, future);
                for (int region = 0; region < remainingFreeRegions; region++) {
                    cacheService.maybeFetchRegion(
                        cacheKey,
                        region,
                        blobLength,
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                            completionListener,
                            () -> {
                                assert streamFactory == null : streamFactory;
                                bytesRead.addAndGet(length);
                                progressUpdater.accept(length);
                            }
                        ),
                        bulkExecutor,
                        listener
                    );
                }

                var results = future.get(10, TimeUnit.SECONDS);
                assertThat(results.stream().allMatch(result -> result), is(true));
                assertEquals(blobLength, bytesRead.get());
                assertEquals(0, cacheService.freeRegionCount());
                assertEquals(1 + remainingFreeRegions, bulkTaskCount.get());
            }
            {
                // cache fully used, no entry old enough to be evicted
                assertEquals(0, cacheService.freeRegionCount());
                final var cacheKey = generateCacheKey();
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRegion(
                    cacheKey,
                    randomIntBetween(0, 10),
                    randomLongBetween(1L, regionSize),
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            throw new AssertionError("should not be executed");
                        }
                    ),
                    bulkExecutor,
                    future
                );
                assertThat("Listener is immediately completed", future.isDone(), is(true));
                assertThat("Region already exists in cache", future.get(), is(false));
            }
            {
                cacheService.computeDecay();

                // fetch one more region should evict an old cache entry
                final var cacheKey = generateCacheKey();
                assertEquals(0, cacheService.freeRegionCount());
                long blobLength = randomLongBetween(1L, regionSize);
                AtomicLong bytesRead = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRegion(
                    cacheKey,
                    0,
                    blobLength,
                    (channel, channelPos, ignore, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assert ignore == null : ignore;
                            bytesRead.addAndGet(length);
                            progressUpdater.accept(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(blobLength, bytesRead.get());
                assertEquals(0, cacheService.freeRegionCount());
            }
        }

        threadPool.shutdown();
    }

    public void testMaybeFetchRange() throws Exception {
        final long cacheSize = size(500L);
        final long regionSize = size(100L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put("path.home", createTempDir())
            .build();

        final var bulkTaskCount = new AtomicInteger(0);
        final var threadPool = new TestThreadPool("test");
        final var bulkExecutor = new StoppableExecutorServiceWrapper(threadPool.generic()) {
            @Override
            public void execute(Runnable command) {
                super.execute(command);
                bulkTaskCount.incrementAndGet();
            }
        };

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            {
                // fetch a random range in a random region of the blob
                final var cacheKey = generateCacheKey();
                assertEquals(5, cacheService.freeRegionCount());

                // blobLength is 1024000 bytes and requires 3 regions
                final long blobLength = size(250);
                final var regions = List.of(
                    // region 0: 0-409600
                    ByteRange.of(cacheService.getRegionStart(0), cacheService.getRegionEnd(0)),
                    // region 1: 409600-819200
                    ByteRange.of(cacheService.getRegionStart(1), cacheService.getRegionEnd(1)),
                    // region 2: 819200-1228800
                    ByteRange.of(cacheService.getRegionStart(2), cacheService.getRegionEnd(2))
                );

                long pos = randomLongBetween(0, blobLength - 1L);
                long len = randomLongBetween(1, blobLength - pos);
                var range = ByteRange.of(pos, pos + len);
                var region = between(0, regions.size() - 1);
                var regionRange = cacheService.mapSubRangeToRegion(range, region);

                var bytesCopied = new AtomicLong(0L);
                var future = new PlainActionFuture<Boolean>();
                cacheService.maybeFetchRange(
                    cacheKey,
                    region,
                    range,
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            assertThat(range.start() + relativePos, equalTo(cacheService.getRegionStart(region) + regionRange.start()));
                            assertThat(channelPos, equalTo(Math.toIntExact(regionRange.start())));
                            assertThat(length, equalTo(Math.toIntExact(regionRange.length())));
                            bytesCopied.addAndGet(length);
                        }
                    ),
                    bulkExecutor,
                    future
                );
                var fetched = future.get(10, TimeUnit.SECONDS);

                assertThat(regionRange.length(), equalTo(bytesCopied.get()));
                if (regionRange.isEmpty()) {
                    assertThat(fetched, is(false));
                    assertEquals(5, cacheService.freeRegionCount());
                    assertEquals(0, bulkTaskCount.get());
                } else {
                    assertThat(fetched, is(true));
                    assertEquals(4, cacheService.freeRegionCount());
                    assertEquals(1, bulkTaskCount.get());
                }
            }
            {
                // fetch multiple ranges to use all the cache
                final int remainingFreeRegions = cacheService.freeRegionCount();
                assertThat(remainingFreeRegions, greaterThanOrEqualTo(4));
                bulkTaskCount.set(0);

                final var cacheKey = generateCacheKey();
                final long blobLength = regionSize * remainingFreeRegions;
                AtomicLong bytesCopied = new AtomicLong(0L);

                final PlainActionFuture<Collection<Boolean>> future = new PlainActionFuture<>();
                final var listener = new GroupedActionListener<>(remainingFreeRegions, future);
                for (int region = 0; region < remainingFreeRegions; region++) {
                    cacheService.maybeFetchRange(
                        cacheKey,
                        region,
                        ByteRange.of(0L, blobLength),
                        blobLength,
                        (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                            completionListener,
                            () -> bytesCopied.addAndGet(length)
                        ),
                        bulkExecutor,
                        listener
                    );
                }

                var results = future.get(10, TimeUnit.SECONDS);
                assertThat(results.stream().allMatch(result -> result), is(true));
                assertEquals(blobLength, bytesCopied.get());
                assertEquals(0, cacheService.freeRegionCount());
                assertEquals(remainingFreeRegions, bulkTaskCount.get());
            }
            {
                // cache fully used, no entry old enough to be evicted
                assertEquals(0, cacheService.freeRegionCount());
                final var cacheKey = generateCacheKey();
                final var blobLength = randomLongBetween(1L, regionSize);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRange(
                    cacheKey,
                    randomIntBetween(0, 10),
                    ByteRange.of(0L, blobLength),
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> {
                            throw new AssertionError("should not be executed");
                        }
                    ),
                    bulkExecutor,
                    future
                );
                assertThat("Listener is immediately completed", future.isDone(), is(true));
                assertThat("Region already exists in cache", future.get(), is(false));
            }
            {
                cacheService.computeDecay();

                // fetch one more range should evict an old cache entry
                final var cacheKey = generateCacheKey();
                assertEquals(0, cacheService.freeRegionCount());
                long blobLength = randomLongBetween(1L, regionSize);
                AtomicLong bytesCopied = new AtomicLong(0L);
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                cacheService.maybeFetchRange(
                    cacheKey,
                    0,
                    ByteRange.of(0L, blobLength),
                    blobLength,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> bytesCopied.addAndGet(length)
                    ),
                    bulkExecutor,
                    future
                );

                var fetched = future.get(10, TimeUnit.SECONDS);
                assertThat("Region has been fetched", fetched, is(true));
                assertEquals(blobLength, bytesCopied.get());
                assertEquals(0, cacheService.freeRegionCount());
            }
        }
        threadPool.shutdown();
    }

    public void testPopulate() throws Exception {
        final long regionSize = size(1L);
        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(100)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put("path.home", createTempDir())
            .build();

        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            final var blobLength = size(12L);

            // start populating the first region
            var entry = cacheService.get(cacheKey, blobLength, 0);
            AtomicLong bytesWritten = new AtomicLong(0L);
            final PlainActionFuture<Boolean> future1 = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize - 1),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> {
                        bytesWritten.addAndGet(length);
                        progressUpdater.accept(length);
                    }
                ),
                taskQueue.getThreadPool().generic(),
                future1
            );

            assertThat(future1.isDone(), is(false));
            assertThat(taskQueue.hasRunnableTasks(), is(true));

            // start populating the second region
            entry = cacheService.get(cacheKey, blobLength, 1);
            final PlainActionFuture<Boolean> future2 = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize - 1),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> {
                        bytesWritten.addAndGet(length);
                        progressUpdater.accept(length);
                    }
                ),
                taskQueue.getThreadPool().generic(),
                future2
            );

            // start populating again the first region, listener should be called immediately
            entry = cacheService.get(cacheKey, blobLength, 0);
            final PlainActionFuture<Boolean> future3 = new PlainActionFuture<>();
            entry.populate(
                ByteRange.of(0, regionSize - 1),
                (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                    completionListener,
                    () -> {
                        bytesWritten.addAndGet(length);
                        progressUpdater.accept(length);
                    }
                ),
                taskQueue.getThreadPool().generic(),
                future3
            );

            assertThat(future3.isDone(), is(true));
            var written = future3.get(10L, TimeUnit.SECONDS);
            assertThat(written, is(false));

            taskQueue.runAllRunnableTasks();

            written = future1.get(10L, TimeUnit.SECONDS);
            assertThat(future1.isDone(), is(true));
            assertThat(written, is(true));
            written = future2.get(10L, TimeUnit.SECONDS);
            assertThat(future2.isDone(), is(true));
            assertThat(written, is(true));
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

    public void testUseFullRegionSize() throws IOException {
        final long regionSize = size(randomIntBetween(1, 100));
        final long cacheSize = regionSize * randomIntBetween(1, 10);

        Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSize).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(cacheSize).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                taskQueue.getThreadPool(),
                taskQueue.getThreadPool().executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            ) {
                @Override
                protected int computeCacheFileRegionSize(long fileLength, int region) {
                    // use full region
                    return super.getRegionSize();
                }
            }
        ) {
            final var cacheKey = generateCacheKey();
            final var blobLength = randomLongBetween(1L, cacheSize);

            int regions = Math.toIntExact(blobLength / regionSize);
            regions += (blobLength % regionSize == 0 ? 0 : 1);
            assertThat(
                cacheService.computeCacheFileRegionSize(blobLength, randomFrom(regions)),
                equalTo(BlobCacheUtils.toIntBytes(regionSize))
            );
            for (int region = 0; region < regions; region++) {
                var cacheFileRegion = cacheService.get(cacheKey, blobLength, region);
                assertThat(cacheFileRegion.tracker.getLength(), equalTo(regionSize));
            }
        }
    }

    public void testUsageSharedSourceInputStreamFactoryInCachePopulation() throws Exception {
        final long regionSizeInBytes = size(100);
        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(size(200)).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes(regionSizeInBytes).getStringRep())
            .put("path.home", createTempDir())
            .build();
        final ThreadPool threadPool = new TestThreadPool("test");
        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            var cacheService = new SharedBlobCacheService<>(
                environment,
                settings,
                threadPool,
                threadPool.executor(ThreadPool.Names.GENERIC),
                BlobCacheMetrics.NOOP
            )
        ) {
            final var cacheKey = generateCacheKey();
            assertEquals(2, cacheService.freeRegionCount());
            final var region = cacheService.get(cacheKey, size(250), 0);
            assertEquals(regionSizeInBytes, region.tracker.getLength());

            // Read disjoint ranges to create holes in the region
            final long interval = regionSizeInBytes / between(5, 20);
            for (var start = interval; start < regionSizeInBytes - 2 * SharedBytes.PAGE_SIZE; start += interval) {
                final var range = ByteRange.of(start, start + SharedBytes.PAGE_SIZE);
                final PlainActionFuture<Integer> future = new PlainActionFuture<>();
                region.populateAndRead(
                    range,
                    range,
                    (channel, channelPos, relativePos, length) -> length,
                    (channel, channelPos, streamFactory, relativePos, length, progressUpdater, completionListener) -> completeWith(
                        completionListener,
                        () -> progressUpdater.accept(length)
                    ),
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    future
                );
                safeGet(future);
            }

            // Read the entire region with a shared source input stream and we want to ensure the following behaviours
            // 1. fillCacheRange is invoked as many times as the number of holes/gaps
            // 2. fillCacheRange is invoked single threaded with the gap order
            // 3. The shared streamFactory is passed to each invocation
            // 4. The factory is closed at the end
            final int numberGaps = region.tracker.getCompletedRanges().size() + 1;
            final var invocationCounter = new AtomicInteger();
            final var factoryClosed = new AtomicBoolean(false);
            final var dummyStreamFactory = new SourceInputStreamFactory() {
                @Override
                public void create(int relativePos, ActionListener<InputStream> listener) {
                    listener.onResponse(null);
                }

                @Override
                public void close() {
                    factoryClosed.set(true);
                }
            };

            final var rangeMissingHandler = new RangeMissingHandler() {
                final AtomicReference<Thread> invocationThread = new AtomicReference<>();
                final AtomicInteger position = new AtomicInteger(-1);

                @Override
                public SourceInputStreamFactory sharedInputStreamFactory(List<SparseFileTracker.Gap> gaps) {
                    return dummyStreamFactory;
                }

                @Override
                public void fillCacheRange(
                    SharedBytes.IO channel,
                    int channelPos,
                    SourceInputStreamFactory streamFactory,
                    int relativePos,
                    int length,
                    IntConsumer progressUpdater,
                    ActionListener<Void> completion
                ) throws IOException {
                    completeWith(completion, () -> {
                        if (invocationCounter.incrementAndGet() == 1) {
                            final Thread witness = invocationThread.compareAndExchange(null, Thread.currentThread());
                            assertThat(witness, nullValue());
                        } else {
                            assertThat(invocationThread.get(), sameInstance(Thread.currentThread()));
                        }
                        assertThat(streamFactory, sameInstance(dummyStreamFactory));
                        assertThat(position.getAndSet(relativePos), lessThan(relativePos));
                        progressUpdater.accept(length);
                    });
                }
            };

            final var range = ByteRange.of(0, regionSizeInBytes);
            if (randomBoolean()) {
                final PlainActionFuture<Integer> future = new PlainActionFuture<>();
                region.populateAndRead(
                    range,
                    range,
                    (channel, channelPos, relativePos, length) -> length,
                    rangeMissingHandler,
                    threadPool.generic(),
                    future
                );
                assertThat(safeGet(future).longValue(), equalTo(regionSizeInBytes));
            } else {
                final PlainActionFuture<Boolean> future = new PlainActionFuture<>();
                region.populate(range, rangeMissingHandler, threadPool.generic(), future);
                assertThat(safeGet(future), equalTo(true));
            }
            assertThat(invocationCounter.get(), equalTo(numberGaps));
            assertThat(region.tracker.checkAvailable(regionSizeInBytes), is(true));
            assertBusy(() -> assertThat(factoryClosed.get(), is(true)));
        } finally {
            threadPool.shutdown();
        }
    }
}
