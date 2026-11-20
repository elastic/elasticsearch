/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * One coordinating charge is enough for a batch that this node also writes as primary. A second update by query waits
 * while that charge is still held, and a reindex whose coordinating node is not the primary waits while replica bytes
 * sit near the limit. Slices of one reindex share that budget. A page that fills it makes the other slice wait, and
 * the task finishes with nothing still charged.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ReindexIndexingPressureIT extends ESIntegTestCase {

    /**
     * One batch is many modest documents. Each primary index operation stays well under the limit, and the bulk's
     * coordinating charge is about the stored sources, so two of those charges do not fit.
     */
    private static final int DOC_CHARS = 4_000;
    private static final int DOCS_PER_BATCH = 40;
    private static final long ONE_BATCH_BYTES = (long) DOC_CHARS * DOCS_PER_BATCH;
    private static final long TWO_BATCH_LIMIT = ONE_BATCH_BYTES + ONE_BATCH_BYTES / 2;
    private static final long ROOMY_LIMIT = 2L * 1024L * 1024L;
    private static final long DEFAULT_REINDEX_ESTIMATE = IndexingPressure.REINDEX_BYTES_ESTIMATE_PER_DOCUMENT.getDefault(Settings.EMPTY)
        .getBytes();

    private final AtomicLong memoryLimit = new AtomicLong(TWO_BATCH_LIMIT);
    private final AtomicLong reindexEstimatePerDocument = new AtomicLong(DEFAULT_REINDEX_ESTIMATE);
    private final AtomicReference<CountDownLatch> releaseWrite = new AtomicReference<>();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), ReindexPlugin.class),
            BlockingIndexPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(IndexingPressure.MAX_INDEXING_BYTES.getKey(), memoryLimit.get() + "b")
            .put(IndexingPressure.REINDEX_BYTES_ESTIMATE_PER_DOCUMENT.getKey(), reindexEstimatePerDocument.get() + "b")
            .put("thread_pool.write.size", 1)
            .put("thread_pool.write.queue_size", 1)
            .build();
    }

    @After
    public void releaseBlockedWrites() {
        CountDownLatch release = releaseWrite.getAndSet(null);
        if (release != null) {
            release.countDown();
        }
        BlockingIndexPlugin.PRE_INDEX.set((shardId, index) -> {});
        reindexEstimatePerDocument.set(DEFAULT_REINDEX_ESTIMATE);
    }

    public void testSecondUpdateByQueryWaitsWhileOneBatchIsHeld() throws Exception {
        // A search reserves the setting's per-document guess for each document in the page. Leave a little room beside
        // that estimate for the index request already sitting in the write pool, and no room for a second page.
        long perDocument = IndexingPressure.REINDEX_BYTES_ESTIMATE_PER_DOCUMENT.getDefault(Settings.EMPTY).getBytes();
        long estimate = (long) DOCS_PER_BATCH * perDocument;
        memoryLimit.set(estimate + 32 * 1024);
        String node = internalCluster().startNode();
        ensureStableCluster(1);
        createIndex("docs-a");
        createIndex("docs-b");
        createIndex("blocker");
        indexBatch("docs-a");
        indexBatch("docs-b");
        refresh("docs-a", "docs-b");

        IndexingPressure pressure = internalCluster().getInstance(IndexingPressure.class, node);
        long baseline = pressure.stats().getCurrentCoordinatingBytes();
        CountDownLatch inWrite = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        releaseWrite.set(release);
        BlockingIndexPlugin.PRE_INDEX.set((shardId, index) -> {
            if (shardId.getIndexName().equals("blocker")) {
                inWrite.countDown();
                safeAwait(release);
            }
        });

        ActionFuture<DocWriteResponse> blockingWrite = client().index(new IndexRequest("blocker").id("1").source("f", "v"));
        safeAwait(inWrite);
        ActionFuture<DocWriteResponse> queuedWrite = client().index(new IndexRequest("blocker").id("2").source("f", "v"));

        try {
            ActionFuture<BulkByPaginatedSearchResponse> admitted = client().execute(
                UpdateByQueryAction.INSTANCE,
                heldRetry(batchRequest("docs-a"))
            );
            // Wait until the fetched page is what is held. The reservation starts at the estimate and then shrinks to
            // the documents actually found, and either of those is far larger than the blocked index request.
            assertBusy(() -> {
                long heldBytes = pressure.stats().getCurrentCoordinatingBytes() - baseline;
                assertThat(heldBytes, greaterThan(perDocument));
            });
            assertFalse(admitted.isDone());

            long rejectionsBefore = pressure.stats().getCoordinatingRejections();
            ActionFuture<BulkByPaginatedSearchResponse> waiting = client().execute(UpdateByQueryAction.INSTANCE, batchRequest("docs-b"));
            assertBusy(() -> {
                assertFalse(waiting.isDone());
                assertThat(pressure.stats().getCoordinatingRejections(), greaterThan(rejectionsBefore));
            });

            long held = pressure.stats().getCurrentCoordinatingBytes() - baseline;
            assertThat(held, greaterThan(perDocument));
            assertFalse(admitted.isDone());
            assertFalse(waiting.isDone());

            release.countDown();
            BulkByPaginatedSearchResponse admittedResponse = admitted.actionGet(TimeValue.timeValueSeconds(30));
            BulkByPaginatedSearchResponse waitingResponse = waiting.actionGet(TimeValue.timeValueSeconds(30));
            assertThat(admittedResponse.getUpdated(), equalTo((long) DOCS_PER_BATCH));
            assertThat(waitingResponse.getUpdated(), equalTo((long) DOCS_PER_BATCH));
            blockingWrite.actionGet(TimeValue.timeValueSeconds(10));
            queuedWrite.actionGet(TimeValue.timeValueSeconds(10));
            assertBusy(() -> assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(baseline)));
        } finally {
            release.countDown();
            BlockingIndexPlugin.PRE_INDEX.set((shardId, index) -> {});
        }
    }

    /**
     * Two slices of one reindex share the coordinating budget. The first page fills it, so the other slice waits with
     * no page of its own, and every document is written with the budget back to where it started.
     */
    public void testSlicedReindexWaitsWhileOnePageHoldsTheBudget() throws Exception {
        // One page is a single document. The guess for that page is well above the document, and the budget fits that
        // guess once, so the other slice cannot reserve while the page is held. Indexing one document stays under the
        // same budget, which also has to admit the source writes.
        int slices = 2;
        int totalDocs = 16;
        int docChars = 8_000;
        long perDocument = 64L * 1024L;
        reindexEstimatePerDocument.set(perDocument);
        memoryLimit.set(perDocument + 4L * 1024L);
        String node = internalCluster().startNode();
        ensureStableCluster(1);
        createIndex("source");
        createIndex("dest");
        indexDocuments("source", totalDocs, docChars);
        refresh("source");

        IndexingPressure pressure = internalCluster().getInstance(IndexingPressure.class, node);
        long baseline = pressure.stats().getCurrentCoordinatingBytes();
        long rejectionsBefore = pressure.stats().getCoordinatingRejections();
        CountDownLatch inWrite = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        releaseWrite.set(release);
        BlockingIndexPlugin.PRE_INDEX.set((shardId, index) -> {
            if (shardId.getIndexName().equals("dest")) {
                inWrite.countDown();
                safeAwait(release);
            }
        });

        ReindexRequestBuilder request = new ReindexRequestBuilder(client()).source("source").destination("dest").setSlices(slices);
        request.source().setSize(1);
        try {
            ActionFuture<BulkByPaginatedSearchResponse> running = request.execute();
            safeAwait(inWrite);
            assertBusy(() -> {
                assertFalse(running.isDone());
                assertThat(pressure.stats().getCoordinatingRejections(), greaterThan(rejectionsBefore));
                assertThat(pressure.stats().getCurrentCoordinatingBytes() - baseline, greaterThan((long) docChars));
            });

            release.countDown();
            BulkByPaginatedSearchResponse response = running.actionGet(TimeValue.timeValueSeconds(60));
            assertThat(response.getCreated(), equalTo((long) totalDocs));
            assertBusy(() -> assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(baseline)));
        } finally {
            release.countDown();
            BlockingIndexPlugin.PRE_INDEX.set((shardId, index) -> {});
        }
    }

    public void testLocalPrimaryDoesNotChargeTheBulkTwice() throws Exception {
        memoryLimit.set(ROOMY_LIMIT);
        String node = internalCluster().startNode();
        ensureStableCluster(1);
        createIndex("docs");
        indexBatch("docs");
        refresh("docs");

        IndexingPressure pressure = internalCluster().getInstance(IndexingPressure.class, node);
        long baselineCoordinating = pressure.stats().getCurrentCoordinatingBytes();
        long baselineCombined = pressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes();
        CountDownLatch inPrimary = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        releaseWrite.set(release);
        BlockingIndexPlugin.PRE_INDEX.set((shardId, index) -> {
            if (shardId.getIndexName().equals("docs")) {
                inPrimary.countDown();
                safeAwait(release);
            }
        });

        try {
            ActionFuture<BulkByPaginatedSearchResponse> admitted = client().execute(UpdateByQueryAction.INSTANCE, batchRequest("docs"));
            safeAwait(inPrimary);

            long coordinating = pressure.stats().getCurrentCoordinatingBytes() - baselineCoordinating;
            long combined = pressure.stats().getCurrentCombinedCoordinatingAndPrimaryBytes() - baselineCombined;
            assertThat(coordinating, greaterThan(ONE_BATCH_BYTES / 2));
            assertThat(combined, lessThan(coordinating + coordinating / 2));

            release.countDown();
            BulkByPaginatedSearchResponse response = admitted.actionGet(TimeValue.timeValueSeconds(30));
            assertThat(response.getUpdated(), equalTo((long) DOCS_PER_BATCH));
            assertBusy(() -> assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(baselineCoordinating)));
        } finally {
            release.countDown();
            BlockingIndexPlugin.PRE_INDEX.set((shardId, index) -> {});
        }
    }

    public void testReplicaBytesNearTheLimitHoldAReindexUntilTheyDrop() throws Exception {
        memoryLimit.set(ROOMY_LIMIT);
        internalCluster().startNode();
        internalCluster().startNode();
        ensureStableCluster(2);
        String index = "source";
        createIndex(index, indexSettings(1, 1).build());
        ensureGreen(index);
        client().index(new IndexRequest(index).id("1").source("t", "v")).actionGet();
        awaitWritePoolIdle();
        refresh(index);

        ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        ShardRouting replica = state.routingTable().index(index).shard(0).replicaShards().get(0);
        String replicaName = state.nodes().get(replica.currentNodeId()).getName();
        String primaryName = state.nodes().get(state.routingTable().index(index).shard(0).primaryShard().currentNodeId()).getName();
        assertThat(replicaName, not(equalTo(primaryName)));

        IndexingPressure pressure = internalCluster().getInstance(IndexingPressure.class, replicaName);
        long baseline = pressure.stats().getCurrentCoordinatingBytes();
        long rejectionsBefore = pressure.stats().getCoordinatingRejections();
        long fill = pressure.stats().getMemoryLimit() - 128;
        Releasable pinned = pressure.markReplicaOperationStarted(1, fill, false);
        try {
            ActionFuture<BulkByPaginatedSearchResponse> running = new ReindexRequestBuilder(internalCluster().client(replicaName)).source(
                index
            ).destination("dest").execute();
            assertBusy(() -> {
                assertFalse(running.isDone());
                assertThat(pressure.stats().getCoordinatingRejections(), greaterThan(rejectionsBefore));
            });
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(baseline));

            pinned.close();
            pinned = null;
            BulkByPaginatedSearchResponse response = running.actionGet(TimeValue.timeValueSeconds(30));
            assertThat(response.getCreated(), equalTo(1L));
            assertBusy(() -> assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(baseline)));
        } finally {
            if (pinned != null) {
                pinned.close();
            }
        }
        assertThat(pressure.stats().getCurrentReplicaBytes(), equalTo(0L));
    }

    private void createIndex(String name) {
        createIndex(name, indexSettings(1, 0).build());
    }

    private void indexBatch(String index) throws Exception {
        indexDocuments(index, DOCS_PER_BATCH, DOC_CHARS);
    }

    /**
     * Each primary queues a global-checkpoint sync on the write pool before it returns. That pool has one queue slot,
     * so the next write waits until the sync has finished. A bulk that would not fit the coordinating limit is split.
     */
    private void indexDocuments(String index, int count, int chars) throws Exception {
        String source = "{\"t\":\"" + "x".repeat(chars) + "\"}";
        // Half the limit leaves room for request context and the bytes the shard request adds on top of the bulk.
        long room = memoryLimit.get() / 2;
        long contextBytes = internalCluster().getInstance(ThreadPool.class).getThreadContext().estimatedRequestContextBytes();
        BulkRequest bulk = new BulkRequest();
        for (int i = 0; i < count; i++) {
            IndexRequest request = new IndexRequest(index).id(Integer.toString(i)).source(source, XContentType.JSON);
            if (bulk.numberOfActions() > 0 && bulk.ramBytesUsed() + request.ramBytesUsed() + contextBytes > room) {
                sendBulk(bulk);
                bulk = new BulkRequest();
            }
            bulk.add(request);
        }
        sendBulk(bulk);
    }

    private void sendBulk(BulkRequest bulk) throws Exception {
        awaitWritePoolIdle();
        BulkResponse response = client().bulk(bulk).actionGet();
        assertFalse(response.buildFailureMessage(), response.hasFailures());
        awaitWritePoolIdle();
    }

    private void awaitWritePoolIdle() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                ThreadPoolExecutor executor = (ThreadPoolExecutor) internalCluster().getInstance(ThreadPool.class, node)
                    .executor(ThreadPool.Names.WRITE);
                assertThat(executor.getActiveCount(), equalTo(0));
                assertThat(executor.getQueue().size(), equalTo(0));
            }
        });
    }

    private static UpdateByQueryRequest batchRequest(String index) {
        UpdateByQueryRequest request = new UpdateByQueryRequest(index);
        request.getSearchRequest().source().size(DOCS_PER_BATCH);
        return request;
    }

    private static UpdateByQueryRequest heldRetry(UpdateByQueryRequest request) {
        // The write pool is one thread and a queue of one, so the admitted bulk is rejected and has to retry.
        request.setRetryBackoffInitialTime(TimeValue.timeValueSeconds(5));
        request.setMaxRetries(10);
        return request;
    }

    public static class BlockingIndexPlugin extends Plugin {
        static final AtomicReference<CheckedBiConsumer<ShardId, Engine.Index, Exception>> PRE_INDEX = new AtomicReference<>(
            (shardId, index) -> {}
        );

        @Override
        public void onIndexModule(org.elasticsearch.index.IndexModule indexModule) {
            indexModule.addIndexOperationListener(new IndexingOperationListener() {
                @Override
                public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
                    try {
                        PRE_INDEX.get().accept(shardId, operation);
                    } catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                    return operation;
                }
            });
        }
    }
}
