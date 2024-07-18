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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogRecoveryMetrics;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static org.elasticsearch.index.IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class StatelessConcurrentRefreshIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(StatelessCommitService.STATELESS_UPLOAD_DELAYED.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 5)
            // tests in this suite expect a precise number of commits
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return plugins;
    }

    public static class TestStateless extends Stateless {

        public final AtomicReference<CyclicBarrier> commitIndexWriterBarrierReference = new AtomicReference<>();
        public final AtomicReference<CyclicBarrier> indexBarrierReference = new AtomicReference<>();
        public final AtomicInteger indexCounter = new AtomicInteger();

        public TestStateless(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            RefreshThrottler.Factory refreshThrottlerFactory,
            DocumentParsingProvider documentParsingProvider,
            TranslogRecoveryMetrics translogRecoveryMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                refreshThrottlerFactory,
                statelessCommitService.getIndexEngineLocalReaderListenerForShard(engineConfig.getShardId()),
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                translogRecoveryMetrics
            ) {
                @Override
                protected void commitIndexWriter(IndexWriter writer, Translog translog) throws IOException {
                    final CyclicBarrier barrier = commitIndexWriterBarrierReference.get();
                    if (barrier != null) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                    }
                    super.commitIndexWriter(writer, translog);
                }

                @Override
                public IndexResult index(Index index) throws IOException {
                    final CyclicBarrier barrier = indexBarrierReference.get();
                    if (barrier != null) {
                        safeAwait(barrier);
                        safeAwait(barrier);
                    }
                    final IndexResult indexResult = super.index(index);
                    indexCounter.incrementAndGet();
                    return indexResult;
                }
            };
        }
    }

    /**
     * This test is to demonstrate that the system can progress successfully with concurrent indexing shard relocation
     * and bulk indexing request. Successful progress means both relocation and bulk indexing can completed within
     * expected timeframe. For bulk indexing with wait_for refresh policy, this can be up to 5 seconds.
     * During indexing shard relocation, the source primary needs to acquire all index operation permits. In the meantime,
     * the bulk indexing request with wait_for refresh policy can hold an operation permit and not release it until the
     * scheduled refresh succeeds. In #109603, we fixed a bug where the bulk indexing request is not completed due to
     * stale translog location and the system makes no progress. This test is to ensure the bug is now fixed.
     * Note that separately, we want to consider not holding operation permit while waiting for refresh, see also ES-8732.
     */
    public void testConcurrentBulkIndexingWithWaitUntilAndRelocation() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        var indexName = "index";
        createIndex(indexName, indexSettings(1, 1).put(INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING.getKey(), "1h").build());
        ensureGreen(indexName);

        bulkIndexDocsWithRefresh(indexName, 20, WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkIndexDocsWithRefresh(indexName, 20, WriteRequest.RefreshPolicy.IMMEDIATE);

        final String newIndexNode = startIndexNode();
        ensureStableCluster(4);

        final var testStateless = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestStateless.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError("plugin not found"));

        final var indexBarrier = new CyclicBarrier(2);
        testStateless.indexBarrierReference.set(indexBarrier);

        logger.info("--> starting the bulk indexing thread");
        final var indexingThread = new Thread(() -> bulkIndexDocsWithRefresh(indexName, 20, WriteRequest.RefreshPolicy.WAIT_UNTIL));
        indexingThread.start();

        logger.info("--> allow one new doc to be indexed for flush");
        safeAwait(indexBarrier);
        safeAwait(indexBarrier);

        logger.info("--> block bulk indexing thread, it holds one operation permit");
        safeAwait(indexBarrier);
        testStateless.indexBarrierReference.set(null); // unblock future indexing

        final var commitIndexWriterBarrier = new CyclicBarrier(2);
        testStateless.commitIndexWriterBarrierReference.set(commitIndexWriterBarrier);

        logger.info("--> relocating index shard into {}", newIndexNode);
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", newIndexNode), indexName);

        logger.info("--> wait for relocation to block at pre-flush");
        safeAwait(commitIndexWriterBarrier);
        testStateless.commitIndexWriterBarrierReference.set(null); // unblock future commits

        logger.info("--> continue bulk indexing");
        safeAwait(indexBarrier);
        assertBusy(() -> assertThat(testStateless.indexCounter.get(), equalTo(60))); // ensure all docs are indexed

        logger.info("--> continue relocation");
        safeAwait(commitIndexWriterBarrier); // let relocation continue

        logger.info("--> wait for bulk indexing to complete");
        indexingThread.join(10_000);
        assertThat(indexingThread.isAlive(), is(false));

        ensureGreen(indexName);
    }

    /**
     * This test is similar to {@link #testConcurrentBulkIndexingWithWaitUntilAndRelocation} but replaces relocation with a
     * concurrent flush which used to experience the same issue fixed by #109603. See also ES-8733.
     */
    public void testConcurrentBulkIndexingWithWaitUntilAndFlush() throws Exception {
        startMasterOnlyNode();
        final String indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        var indexName = "index";
        createIndex(indexName, indexSettings(1, 1).put(INDEX_TRANSLOG_FLUSH_THRESHOLD_AGE_SETTING.getKey(), "1h").build());
        ensureGreen(indexName);

        bulkIndexDocsWithRefresh(indexName, 20, WriteRequest.RefreshPolicy.IMMEDIATE);
        bulkIndexDocsWithRefresh(indexName, 20, WriteRequest.RefreshPolicy.IMMEDIATE);

        final var testStateless = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestStateless.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError("plugin not found"));

        final var indexBarrier = new CyclicBarrier(2);
        testStateless.indexBarrierReference.set(indexBarrier);

        logger.info("--> starting the bulk indexing thread");
        final var indexingThread = new Thread(() -> bulkIndexDocsWithRefresh(indexName, 20, WriteRequest.RefreshPolicy.WAIT_UNTIL));
        indexingThread.start();

        logger.info("--> allow one new doc to be indexed for flush");
        safeAwait(indexBarrier);
        safeAwait(indexBarrier);

        logger.info("--> block bulk indexing thread, it holds one operation permit");
        safeAwait(indexBarrier);
        testStateless.indexBarrierReference.set(null); // unblock future indexing

        final var commitIndexWriterBarrier = new CyclicBarrier(2);
        testStateless.commitIndexWriterBarrierReference.set(commitIndexWriterBarrier);

        logger.info("--> flush and wait for it to block");
        final Thread flushThread = new Thread(() -> flush(indexName));
        flushThread.start();
        safeAwait(commitIndexWriterBarrier);
        testStateless.commitIndexWriterBarrierReference.set(null); // unblock future commits

        logger.info("--> continue bulk indexing");
        safeAwait(indexBarrier);
        assertBusy(() -> assertThat(testStateless.indexCounter.get(), equalTo(60))); // ensure all docs are indexed

        logger.info("--> continue flush");
        safeAwait(commitIndexWriterBarrier);

        logger.info("--> wait for flush to complete");
        flushThread.join(10_000);
        assertThat(flushThread.isAlive(), is(false));

        logger.info("--> wait for bulk indexing to complete");
        indexingThread.join(10_000);
        assertThat(indexingThread.isAlive(), is(false));
    }

    private void bulkIndexDocsWithRefresh(String indexName, int numDocs, WriteRequest.RefreshPolicy refreshPolicy) {
        var bulkRequest = client().prepareBulk();
        for (int i = 0; i < numDocs; i++) {
            bulkRequest.add(prepareIndex(indexName).setSource("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
        }
        bulkRequest.setRefreshPolicy(refreshPolicy);
        assertNoFailures(bulkRequest.get(TEST_REQUEST_TIMEOUT));
    }
}
