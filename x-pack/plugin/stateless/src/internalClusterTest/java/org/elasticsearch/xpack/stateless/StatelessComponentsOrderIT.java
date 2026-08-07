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
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.IndexBlobStoreCacheDirectory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.UnaryOperator;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;

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

        try (ExecutorService executorService = Executors.newCachedThreadPool()) {
            final Future<?> shuttingDownFuture;
            final Future<?> indexingFuture;
            try {
                logger.info("--> indexing and flush docs to trigger background merge");
                indexingFuture = executorService.submit(() -> {
                    try {
                        for (int i = 0; i < 11; i++) {
                            if (plugin.mergeReadStartedLatch.getCount() == 0) {
                                logger.info("--> merge read started, stopping indexing loop");
                                break;
                            }
                            indexDocs(indexName, 10, UnaryOperator.identity(), null, null, false);
                            flush(indexName);
                        }
                    } catch (Exception e) {
                        logger.info("--> indexing loop threw, this is fine", e);
                    }
                });

                // Wait for merge to trigger and evict cache so that merge will attempt to fill the cache
                safeAwait(plugin.mergeReadStartedLatch);
                logger.info("--> evict cache after merge read started");
                final var blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
                getCacheService(blobStoreCacheDirectory).forceEvict((key) -> true);

                logger.info("--> deleting index to remove the shard from IndicesService");
                safeGet(indicesAdmin().prepareDelete(indexName).execute());
                assertNull(indicesService.indexService(indexShard.shardId().getIndex()));

                logger.info("--> shutting down the index node");
                shuttingDownFuture = executorService.submit(() -> {
                    try {
                        internalCluster().stopNode(indexNode);
                    } catch (IOException e) {
                        fail(e);
                    }
                });

                safeAwait(plugin.statelessCloseCalledLatch);
            } finally {
                // Let merge continue, and it should not run into exceptions such as ClosedChannelException or EsRejectedExecutionException
                // Do this even if the test is failing, so we don't interfere with the teardown
                logger.info("--> resume the merge thread");
                plugin.cacheEvictedLatch.countDown();
            }

            safeGet(indexingFuture);
            safeGet(shuttingDownFuture);
        }
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        private final CountDownLatch mergeReadStartedLatch = new CountDownLatch(1);
        private final CountDownLatch cacheEvictedLatch = new CountDownLatch(1);
        private final CountDownLatch statelessCloseCalledLatch = new CountDownLatch(1);

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
                    if (ThreadPool.Names.MERGE.equals(EsExecutors.executorName(Thread.currentThread()))) {
                        mergeReadStartedLatch.countDown();
                        safeAwait(cacheEvictedLatch);
                    }
                    return super.doOpenInput(name, context, blobFileRanges);
                }
            };
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
