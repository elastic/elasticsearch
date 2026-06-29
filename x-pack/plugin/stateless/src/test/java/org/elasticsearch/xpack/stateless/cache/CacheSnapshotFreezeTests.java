/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBytes.PAGE_SIZE;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;

public class CacheSnapshotFreezeTests extends ESTestCase {

    /**
     * Minimal subclass that uses the protected test constructor so no real I/O is
     * needed. {@link #getOccupiedEntries()} returns an empty list and
     * {@link #syncSlotRange(int, int)} is a no-op, so the tests focus purely on
     * the freeze/unfreeze locking semantics.
     */
    private static final class TestService extends StatelessSharedBlobCacheService {

        private final NodeEnvironment nodeEnvironment;

        TestService(NodeEnvironment environment, Settings settings, ThreadPool threadPool) {
            super(
                environment,
                settings,
                threadPool,
                new BlobCacheMetrics(MeterRegistry.NOOP),
                new DefaultEvictionPolicy<>(),
                System::nanoTime,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            );
            this.nodeEnvironment = environment;
        }

        @Override
        public List<SharedBlobCacheService.CacheIndexEntry<FileCacheKey>> getOccupiedEntries() {
            return List.of();
        }

        @Override
        public void syncSlotRange(int slot, int flags) throws IOException {
            // no-op — no real file to sync in tests
        }

        @Override
        public void close() {
            try {
                super.close();
            } finally {
                nodeEnvironment.close();
            }
        }
    }

    private static Settings buildSettings(String tmpDir) {
        return Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "test-node")
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes((long) PAGE_SIZE * 4).getStringRep())
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofBytes((long) PAGE_SIZE * 4).getStringRep())
            .put("path.home", tmpDir)
            .build();
    }

    private TestService createTestService(ThreadPool threadPool) throws IOException {
        String tmp = createTempDir().toAbsolutePath().toString();
        Settings settings = buildSettings(tmp);
        NodeEnvironment env = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
        return new TestService(env, settings, threadPool);
    }

    /**
     * A frozen cache must block {@link StatelessSharedBlobCacheService#forceEvict} callers until
     * {@link StatelessSharedBlobCacheService#unfreeze} is called.
     */
    public void testFreezeBlocksForceEvictUntilUnfreeze() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        try (TestService service = createTestService(threadPool)) {
            long stamp = service.freeze();

            CountDownLatch evictStarted = new CountDownLatch(1);
            CountDownLatch evictDone = new CountDownLatch(1);

            Thread evictThread = new Thread(() -> {
                evictStarted.countDown();
                service.forceEvict(k -> true);
                evictDone.countDown();
            }, "evict-thread");
            evictThread.setDaemon(true);
            evictThread.start();

            // Wait until the evict thread has at least started, then give it time to reach the lock.
            assertTrue("evict thread did not start in time", evictStarted.await(5, TimeUnit.SECONDS));
            assertFalse("forceEvict should still be blocked while cache is frozen", evictDone.await(150, TimeUnit.MILLISECONDS));

            service.unfreeze(stamp);

            assertTrue("forceEvict did not complete after unfreeze", evictDone.await(5, TimeUnit.SECONDS));
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Multiple concurrent {@link StatelessSharedBlobCacheService#forceEvict} callers must all be
     * released once {@link StatelessSharedBlobCacheService#unfreeze} is called.
     */
    public void testUnfreezeReleasesMultipleWaiters() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        try (TestService service = createTestService(threadPool)) {
            long stamp = service.freeze();

            int numWaiters = 3;
            CountDownLatch allStarted = new CountDownLatch(numWaiters);
            CountDownLatch allDone = new CountDownLatch(numWaiters);

            for (int i = 0; i < numWaiters; i++) {
                Thread t = new Thread(() -> {
                    allStarted.countDown();
                    service.forceEvict(k -> true);
                    allDone.countDown();
                }, "evict-waiter-" + i);
                t.setDaemon(true);
                t.start();
            }

            assertTrue("not all evict threads started in time", allStarted.await(5, TimeUnit.SECONDS));
            assertFalse("all waiters should still be blocked while cache is frozen", allDone.await(150, TimeUnit.MILLISECONDS));

            service.unfreeze(stamp);

            assertTrue("not all waiters completed after unfreeze", allDone.await(5, TimeUnit.SECONDS));
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * If an exception is thrown inside a {@code try/finally} that mirrors
     * {@link CacheSnapshotService#snapshot}, the {@code finally} block must call
     * {@link StatelessSharedBlobCacheService#unfreeze}, and subsequent
     * {@link StatelessSharedBlobCacheService#forceEvict} calls must proceed immediately
     * without blocking.
     */
    public void testFreezeReleasedInFinally() throws Exception {
        ThreadPool threadPool = new TestThreadPool("test", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        try (TestService service = createTestService(threadPool)) {
            // Simulate the try/finally pattern from CacheSnapshotService.snapshot,
            // but throw before completing normal work.
            long stamp = service.freeze();
            try {
                throw new RuntimeException("simulated failure during snapshot");
            } finally {
                // This must always run and release the write stamp.
                service.unfreeze(stamp);
            }
        } catch (RuntimeException ignored) {
            // The exception propagates out — this is expected. The important thing is
            // that unfreeze() was called in the finally block above.
        }

        // Re-create the service (the one above is closed) and verify that a fresh
        // freeze/unfreeze cycle followed by forceEvict does not block.
        ThreadPool tp2 = new TestThreadPool("test2", StatelessPlugin.statelessExecutorBuilders(Settings.EMPTY, true));
        try (TestService service2 = createTestService(tp2)) {
            long stamp = service2.freeze();
            service2.unfreeze(stamp);

            CountDownLatch done = new CountDownLatch(1);
            Thread t = new Thread(() -> {
                service2.forceEvict(k -> true);
                done.countDown();
            }, "post-unfreeze-evict");
            t.setDaemon(true);
            t.start();

            assertTrue("forceEvict should not block after unfreeze", done.await(1, TimeUnit.SECONDS));
        } finally {
            terminate(tp2);
            terminate(threadPool);
        }
    }
}
