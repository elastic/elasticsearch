/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;

import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

/**
 * One coordinating charge is enough for a batch that this node also writes as primary. A second update by query is rejected
 * while that charge is still held, and replica bytes already near the limit reject a reindex that has no primary here.
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

    private final AtomicLong memoryLimit = new AtomicLong(TWO_BATCH_LIMIT);
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
    }

    public void testSecondUpdateByQueryIsRejectedWhileOneBatchIsHeld() throws Exception {
        memoryLimit.set(TWO_BATCH_LIMIT);
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
            assertBusy(() -> assertThat(pressure.stats().getCurrentCoordinatingBytes() - baseline, greaterThan(ONE_BATCH_BYTES / 2)));
            assertFalse(admitted.isDone());

            long rejectionsBefore = pressure.stats().getCoordinatingRejections();
            ActionFuture<BulkByPaginatedSearchResponse> rejected = client().execute(UpdateByQueryAction.INSTANCE, batchRequest("docs-b"));
            Exception failure = expectThrows(Exception.class, () -> rejected.actionGet(TimeValue.timeValueSeconds(30)));
            EsRejectedExecutionException coordinatingRejection = (EsRejectedExecutionException) ExceptionsHelper.unwrap(
                failure,
                EsRejectedExecutionException.class
            );
            assertNotNull(failure.toString(), coordinatingRejection);
            assertThat(ExceptionsHelper.status(failure), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(coordinatingRejection.getMessage(), containsString("max_coordinating_bytes"));
            assertThat(pressure.stats().getCoordinatingRejections(), greaterThan(rejectionsBefore));

            long held = pressure.stats().getCurrentCoordinatingBytes() - baseline;
            assertThat(held, greaterThan(ONE_BATCH_BYTES / 2));
            assertThat(held, lessThan(TWO_BATCH_LIMIT));
            assertFalse(admitted.isDone());

            release.countDown();
            BulkByPaginatedSearchResponse admittedResponse = admitted.actionGet(TimeValue.timeValueSeconds(30));
            assertThat(admittedResponse.getUpdated(), equalTo((long) DOCS_PER_BATCH));
            blockingWrite.actionGet(TimeValue.timeValueSeconds(10));
            queuedWrite.actionGet(TimeValue.timeValueSeconds(10));
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

    public void testReplicaBytesNearTheLimitRejectAReindexOnANonPrimary() throws Exception {
        memoryLimit.set(ROOMY_LIMIT);
        internalCluster().startNode();
        internalCluster().startNode();
        ensureStableCluster(2);
        String index = "source";
        createIndex(index, indexSettings(1, 1).build());
        ensureGreen(index);
        client().index(new IndexRequest(index).id("1").source("t", "v")).actionGet();
        refresh(index);

        ClusterState state = client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        ShardRouting replica = state.routingTable().index(index).shard(0).replicaShards().get(0);
        String replicaName = state.nodes().get(replica.currentNodeId()).getName();
        String primaryName = state.nodes().get(state.routingTable().index(index).shard(0).primaryShard().currentNodeId()).getName();
        assertThat(replicaName, not(equalTo(primaryName)));

        IndexingPressure pressure = internalCluster().getInstance(IndexingPressure.class, replicaName);
        long baseline = pressure.stats().getCurrentCoordinatingBytes();
        long fill = pressure.stats().getMemoryLimit() - 128;
        try (Releasable ignored = pressure.markReplicaOperationStarted(1, fill, false)) {
            Exception failure = expectThrows(
                Exception.class,
                () -> new ReindexRequestBuilder(internalCluster().client(replicaName)).source(index).destination("dest").get()
            );
            EsRejectedExecutionException rejected = (EsRejectedExecutionException) ExceptionsHelper.unwrap(
                failure,
                EsRejectedExecutionException.class
            );
            assertNotNull(failure.toString(), rejected);
            assertThat(ExceptionsHelper.status(failure), equalTo(RestStatus.TOO_MANY_REQUESTS));
            assertThat(rejected.getMessage(), containsString("replica_bytes"));
            assertThat(pressure.stats().getCurrentCoordinatingBytes(), equalTo(baseline));
        }
        assertThat(pressure.stats().getCurrentReplicaBytes(), equalTo(0L));
    }

    private void createIndex(String name) {
        createIndex(name, indexSettings(1, 0).build());
    }

    private static void indexBatch(String index) {
        String source = "{\"t\":\"" + "x".repeat(DOC_CHARS) + "\"}";
        for (int i = 0; i < DOCS_PER_BATCH; i++) {
            client().index(new IndexRequest(index).id(Integer.toString(i)).source(source, XContentType.JSON)).actionGet();
        }
    }

    private static UpdateByQueryRequest batchRequest(String index) {
        UpdateByQueryRequest request = new UpdateByQueryRequest(index);
        request.getSearchRequest().source().size(DOCS_PER_BATCH);
        return request;
    }

    private static UpdateByQueryRequest heldRetry(UpdateByQueryRequest request) {
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
