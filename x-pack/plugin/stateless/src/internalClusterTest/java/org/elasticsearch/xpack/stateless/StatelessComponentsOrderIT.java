/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
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

        logger.info("--> indexing and flush docs to trigger background merge");
        for (int i = 0; i < 11; i++) {
            indexDocs(indexName, 10);
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
        final Thread shuttingDownThread = new Thread(() -> {
            try {
                internalCluster().stopNode(indexNode);
            } catch (IOException e) {
                fail(e);
            }
        });
        shuttingDownThread.start();

        safeAwait(plugin.statelessCloseCalledLatch);
        // Let merge continue, and it should not run into exceptions such as ClosedChannelException or EsRejectedExecutionException
        logger.info("--> resume the merge thread");
        plugin.cacheEvictedLatch.countDown();

        shuttingDownThread.join(30000);
        assertFalse(shuttingDownThread.isAlive());
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
