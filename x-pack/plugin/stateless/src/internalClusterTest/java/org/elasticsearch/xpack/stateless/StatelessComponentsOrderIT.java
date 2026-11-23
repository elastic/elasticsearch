/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.hamcrest.Matchers.nullValue;

public class StatelessComponentsOrderIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    public void testClosingNodeShouldWaitForOngoingMerge() throws Exception {
        startMasterOnlyNode();
        var nodeSettings = Settings.builder()
            .put(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .build();
        final String indexNode = startIndexNode(nodeSettings);
        ensureStableCluster(2);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        final TestStatelessPlugin plugin = findPlugin(indexNode, TestStatelessPlugin.class);
        final var indicesService = internalCluster().getInstance(IndicesService.class, indexNode);
        final var indexShard = findIndexShard(indexName);
        final Thread shuttingDownThread;

        try {
            logger.info("--> indexing and flush docs to trigger background merge");
            for (int i = 0; i < 11; i++) {
                indexDocs(indexName, 10);
                logger.info("--> indexed {} docs", (i + 1) * 10);
                flush(indexName);
            }

            // Wait for merge to trigger and evict cache so that merge will attempt to fill the cache
            safeAwait(plugin.mergeReadStartedLatch);
            logger.info("--> evict cache after merge read started");
            final var blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
            getCacheService(blobStoreCacheDirectory).forceEvict((key) -> true);

            logger.info("--> deleting index to remove the shard from IndicesService");
            safeGet(indicesAdmin().prepareDelete(indexName).execute());
            assertNull(indicesService.indexService(indexShard.shardId().getIndex()));

            logger.info("--> shutting down the index node");
            shuttingDownThread = new Thread(() -> {
                try {
                    internalCluster().stopNode(indexNode);
                } catch (IOException e) {
                    fail(e);
                }
            });
            shuttingDownThread.start();

            safeAwait(plugin.statelessCloseCalledLatch);
        } finally {
            // Let merge continue, and it should not run into exceptions such as ClosedChannelException or EsRejectedExecutionException
            logger.info("--> resume the merge thread");
            plugin.cacheEvictedLatch.countDown();
        }

        shuttingDownThread.join(30000);
        assertFalse(shuttingDownThread.isAlive());
        assertThat(plugin.mergeBlockFailure.get(), nullValue());
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        private static final Logger logger = LogManager.getLogger(TestStatelessPlugin.class);

        private final CountDownLatch mergeReadStartedLatch = new CountDownLatch(1);
        private final CountDownLatch cacheEvictedLatch = new CountDownLatch(1);
        private final CountDownLatch statelessCloseCalledLatch = new CountDownLatch(1);
        private final AtomicReference<String> mergeBlockFailure = new AtomicReference<>();

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexBlobStoreCacheDirectory createIndexBlobStoreCacheDirectory(
            StatelessSharedBlobCacheService cacheService,
            ShardId shardId
        ) {
            return new IndexBlobStoreCacheDirectory(cacheService, shardId) {
                @Override
                protected IndexInput doOpenInput(String name, IOContext context, BlobFileRanges blobFileRanges) {
                    // Only park the merge thread where it holds no IndexWriter monitor. Lucene opens the readers of the segments to merge
                    // under that monitor (see IndexWriter#mergeMiddle calling ReadersAndUpdates#getReaderForMerge), and parking there
                    // would block every flush and refresh of the shard, including the ones the test still has to complete before it can
                    // resume the merge. Those opens are let through, the merge is blocked at a later read made outside the monitor.
                    if (ThreadPool.Names.MERGE.equals(EsExecutors.executorName(Thread.currentThread()))
                        && holdsIndexWriterMonitor() == false) {
                        mergeReadStartedLatch.countDown();
                        logger.info("--> merge thread blocked");
                        awaitCacheEvicted();
                    }
                    return super.doOpenInput(name, context, blobFileRanges);
                }
            };
        }

        /**
         * Parks the merge thread until the test resumes it. A timeout is recorded in {@link #mergeBlockFailure} for the test thread to
         * report, rather than thrown from here: an {@link AssertionError} thrown into Lucene is a tragic event that fails the engine
         * and leaves the test waiting for a merge that never completes instead of failing with the actual cause.
         */
        private void awaitCacheEvicted() {
            try {
                if (cacheEvictedLatch.await(SAFE_AWAIT_TIMEOUT.millis(), TimeUnit.MILLISECONDS)) {
                    return;
                }
                final var threads = dumpRelevantThreads();
                logger.info("--> merge thread timed out waiting to be unblocked, relevant threads:\n{}", threads);
                mergeBlockFailure.compareAndSet(null, "merge thread timed out waiting to be unblocked, relevant threads:\n" + threads);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                mergeBlockFailure.compareAndSet(null, "merge thread interrupted while waiting to be unblocked");
            }
            // let this and any later merge read proceed, the test fails on the recorded failure
            cacheEvictedLatch.countDown();
        }

        /**
         * @return whether the current thread holds the monitor of an {@code IndexWriter} (matched by class name so that the engine's
         * own subclasses of it are covered too)
         */
        private static boolean holdsIndexWriterMonitor() {
            final var threadInfos = ManagementFactory.getThreadMXBean()
                .getThreadInfo(new long[] { Thread.currentThread().threadId() }, true, false);
            for (MonitorInfo monitor : threadInfos[0].getLockedMonitors()) {
                if (monitor.getClassName().contains("IndexWriter")) {
                    return true;
                }
            }
            return false;
        }

        /**
         * Dumps the test, flush and merge threads including the monitor each is blocked on and its owner, so that a failure can show what
         * (if anything) the blocked merge thread is holding up, e.g. the flush waiting on the IndexWriter monitor.
         */
        private static String dumpRelevantThreads() {
            final var sb = new StringBuilder();
            for (ThreadInfo info : ManagementFactory.getThreadMXBean().dumpAllThreads(true, true)) {
                final String name = info.getThreadName();
                if (name.startsWith("TEST-") == false && name.contains("[flush]") == false && name.contains("[merge]") == false) {
                    continue;
                }
                sb.append('"').append(name).append("\" ").append(info.getThreadState());
                if (info.getLockInfo() != null) {
                    sb.append(" on ").append(info.getLockInfo());
                }
                if (info.getLockOwnerName() != null) {
                    sb.append(" owned by \"").append(info.getLockOwnerName()).append('"');
                }
                sb.append('\n');
                for (StackTraceElement frame : info.getStackTrace()) {
                    sb.append("\tat ").append(frame).append('\n');
                }
                for (MonitorInfo monitor : info.getLockedMonitors()) {
                    sb.append("\t- locked ").append(monitor).append(" at depth ").append(monitor.getLockedStackDepth()).append('\n');
                }
            }
            return sb.toString();
        }

        @Override
        public void close() throws IOException {
            statelessCloseCalledLatch.countDown();
            super.close(); // this closes the SharedBlobCacheService
            // Randomly delay for one of the two possible exceptions
            // * ClosedChannelException if the delay is longer
            // * EsRejectedExecutionException if the delay is short
            safeSleep(randomLongBetween(0, 500));
        }
    }
}
