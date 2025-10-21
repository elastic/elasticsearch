/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexingOperationListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2, numClientNodes = 1)
public class IndexingPressureIT extends ESIntegTestCase {

    public static final String INDEX_NAME = "test";

    private static final Settings unboundedWriteQueue = Settings.builder().put("thread_pool.write.queue_size", -1).build();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put(unboundedWriteQueue).build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class, PreIndexListenerInstallerPlugin.class);
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    public void testWriteIndexingPressureMetricsAreIncremented() throws Exception {
        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames();
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final CountDownLatch replicationSendPointReached = new CountDownLatch(1);
        final CountDownLatch latchBlockingReplicationSend = new CountDownLatch(1);

        TransportService primaryService = internalCluster().getInstance(TransportService.class, primaryName);
        final MockTransportService primaryTransportService = (MockTransportService) primaryService;
        TransportService replicaService = internalCluster().getInstance(TransportService.class, replicaName);
        final MockTransportService replicaTransportService = (MockTransportService) replicaService;

        primaryTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportShardBulkAction.ACTION_NAME + "[r]")) {
                try {
                    replicationSendPointReached.countDown();
                    latchBlockingReplicationSend.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        final ThreadPool replicaThreadPool = replicaTransportService.getThreadPool();
        final Releasable replicaRelease = blockWriteThreadPool(replicaThreadPool);

        final BulkRequest bulkRequest = new BulkRequest();
        long totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        final long bulkRequestSize = bulkRequest.ramBytesUsed();
        final long bulkShardRequestSize = totalRequestSize;
        final long bulkOps = bulkRequest.numberOfActions();

        try {
            final ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);
            replicationSendPointReached.await();

            IndexingPressure primaryWriteLimits = internalCluster().getInstance(IndexingPressure.class, primaryName);
            IndexingPressure replicaWriteLimits = internalCluster().getInstance(IndexingPressure.class, replicaName);
            IndexingPressure coordinatingWriteLimits = internalCluster().getInstance(IndexingPressure.class, coordinatingOnlyNode);

            assertThat(primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), greaterThan(bulkShardRequestSize));
            assertThat(primaryWriteLimits.stats().getCurrentPrimaryBytes(), greaterThan(bulkShardRequestSize));
            assertThat(primaryWriteLimits.stats().getCurrentPrimaryOps(), greaterThanOrEqualTo(bulkOps));
            assertEquals(0, primaryWriteLimits.stats().getCurrentCoordinatingBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentCoordinatingOps());
            assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaOps());

            assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentCoordinatingBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentCoordinatingOps());
            assertEquals(0, replicaWriteLimits.stats().getCurrentPrimaryBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentPrimaryOps());
            assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaOps());

            assertEquals(bulkRequestSize, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(bulkRequestSize, coordinatingWriteLimits.stats().getCurrentCoordinatingBytes());
            assertEquals(bulkOps, coordinatingWriteLimits.stats().getCurrentCoordinatingOps());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentPrimaryBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentPrimaryOps());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaOps());

            latchBlockingReplicationSend.countDown();

            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            final BulkRequest secondBulkRequest = new BulkRequest();
            secondBulkRequest.add(request);

            /*
             * Use the primary as the coordinating node this time.
             * We never use the replica as the coordinating node because
             * we try to go async immediately and the replica's thread
             * pool is stuffed.
             */
            final ActionFuture<BulkResponse> secondFuture = client(primaryName).bulk(secondBulkRequest);

            final long secondBulkRequestSize = secondBulkRequest.ramBytesUsed();
            final long secondBulkShardRequestSize = request.ramBytesUsed();
            final long secondBulkOps = secondBulkRequest.numberOfActions();

            assertBusy(() -> {
                assertThat(
                    primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(),
                    greaterThan(bulkShardRequestSize + secondBulkRequestSize)
                );
                assertEquals(secondBulkRequestSize, primaryWriteLimits.stats().getCurrentCoordinatingBytes());
                assertEquals(secondBulkOps, primaryWriteLimits.stats().getCurrentCoordinatingOps());
                assertThat(primaryWriteLimits.stats().getCurrentPrimaryBytes(), greaterThan(bulkShardRequestSize + secondBulkRequestSize));
                assertThat(primaryWriteLimits.stats().getCurrentPrimaryOps(), equalTo(bulkOps + secondBulkOps));

                assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, replicaWriteLimits.stats().getCurrentCoordinatingBytes());
                assertEquals(0, replicaWriteLimits.stats().getCurrentCoordinatingOps());
                assertEquals(0, replicaWriteLimits.stats().getCurrentPrimaryBytes());
                assertEquals(0, replicaWriteLimits.stats().getCurrentPrimaryOps());
            });
            assertEquals(bulkRequestSize, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertBusy(
                () -> assertThat(
                    replicaWriteLimits.stats().getCurrentReplicaBytes(),
                    greaterThan(bulkShardRequestSize + secondBulkShardRequestSize)
                )
            );
            assertBusy(() -> assertThat(replicaWriteLimits.stats().getCurrentReplicaOps(), equalTo(bulkOps + secondBulkOps)));

            replicaRelease.close();

            successFuture.actionGet();
            secondFuture.actionGet();

            assertEquals(0, primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentCoordinatingBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentCoordinatingOps());
            assertEquals(0, primaryWriteLimits.stats().getCurrentPrimaryBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentPrimaryOps());
            assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaOps());

            assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentCoordinatingBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentCoordinatingOps());
            assertEquals(0, replicaWriteLimits.stats().getCurrentPrimaryBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentPrimaryOps());
            assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaOps());

            assertEquals(0, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentCoordinatingBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentCoordinatingOps());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentPrimaryBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentPrimaryOps());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaOps());
        } finally {
            if (replicationSendPointReached.getCount() > 0) {
                replicationSendPointReached.countDown();
            }
            replicaRelease.close();
            if (latchBlockingReplicationSend.getCount() > 0) {
                latchBlockingReplicationSend.countDown();
            }
            replicaRelease.close();
            primaryTransportService.clearAllRules();
        }
    }

    public void testWriteCanBeRejectedAtCoordinatingLevel() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        long totalRequestSize = 0;
        for (int i = 0; i < 80; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        final long bulkRequestSize = bulkRequest.ramBytesUsed();
        final long bulkShardRequestSize = totalRequestSize;
        restartNodesWithSettings(
            Settings.builder().put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), (long) (bulkShardRequestSize * 1.5) + "B").build()
        );

        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames();
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (Releasable replicaRelease = blockWriteThreadPool(replicaThreadPool)) {
            final ActionFuture<BulkResponse> successFuture = client(coordinatingOnlyNode).bulk(bulkRequest);

            IndexingPressure primaryWriteLimits = internalCluster().getInstance(IndexingPressure.class, primaryName);
            IndexingPressure replicaWriteLimits = internalCluster().getInstance(IndexingPressure.class, replicaName);
            IndexingPressure coordinatingWriteLimits = internalCluster().getInstance(IndexingPressure.class, coordinatingOnlyNode);

            assertBusy(() -> {
                assertThat(primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), greaterThan(bulkShardRequestSize));
                assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
                assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertThat(replicaWriteLimits.stats().getCurrentReplicaBytes(), greaterThan(bulkShardRequestSize));
                assertEquals(bulkRequestSize, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
            });

            expectThrows(EsRejectedExecutionException.class, () -> {
                if (randomBoolean()) {
                    client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
                } else if (randomBoolean()) {
                    client(primaryName).bulk(bulkRequest).actionGet();
                } else {
                    client(replicaName).bulk(bulkRequest).actionGet();
                }
            });

            replicaRelease.close();

            successFuture.actionGet();

            assertEquals(0, primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
        }
    }

    public void testWriteCanBeRejectedAtPrimaryLevel() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        long totalRequestSize = 0;
        int numberOfIndexRequests = randomIntBetween(50, 100);
        for (int i = 0; i < numberOfIndexRequests; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            totalRequestSize += request.ramBytesUsed();
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }
        final long bulkShardRequestSize = totalRequestSize;
        restartNodesWithSettings(
            Settings.builder().put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), (long) (bulkShardRequestSize * 1.5) + "B").build()
        );

        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames();
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (Releasable replicaRelease = blockWriteThreadPool(replicaThreadPool)) {
            final ActionFuture<BulkResponse> successFuture = client(primaryName).bulk(bulkRequest);

            IndexingPressure primaryWriteLimits = internalCluster().getInstance(IndexingPressure.class, primaryName);
            IndexingPressure replicaWriteLimits = internalCluster().getInstance(IndexingPressure.class, replicaName);
            IndexingPressure coordinatingWriteLimits = internalCluster().getInstance(IndexingPressure.class, coordinatingOnlyNode);

            assertBusy(() -> {
                assertThat(primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(), greaterThan(bulkShardRequestSize));
                assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
                assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertThat(replicaWriteLimits.stats().getCurrentReplicaBytes(), greaterThan(bulkShardRequestSize));
                assertEquals(0, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
                assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
            });

            BulkResponse responses = client(coordinatingOnlyNode).bulk(bulkRequest).actionGet();
            assertTrue(responses.hasFailures());
            assertThat(responses.getItems()[0].getFailure().getCause().getCause(), instanceOf(EsRejectedExecutionException.class));

            replicaRelease.close();

            successFuture.actionGet();

            assertEquals(0, primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
        }
    }

    public void testWritesWillSucceedIfBelowThreshold() throws Exception {
        restartNodesWithSettings(
            Settings.builder()
                .put(IndexingPressure.MAX_COORDINATING_BYTES.getKey(), "1MB")
                .put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), "1MB")
                .build()
        );
        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (Releasable replicaRelease = blockWriteThreadPool(replicaThreadPool)) {
            // The write limits is set to 1MB. We will send up to 800KB to stay below that threshold.
            int thresholdToStopSending = 800 * 1024;

            ArrayList<ActionFuture<DocWriteResponse>> responses = new ArrayList<>();
            long totalRequestSize = 0;
            while (totalRequestSize < thresholdToStopSending) {
                IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                    .source(Collections.singletonMap("key", randomAlphaOfLength(500)));
                totalRequestSize += request.ramBytesUsed();
                responses.add(client(coordinatingOnlyNode).index(request));
            }

            replicaRelease.close();

            // Would throw exception if one of the operations was rejected
            responses.forEach(ActionFuture::actionGet);
        }
    }

    public void testWriteCanRejectOnPrimaryBasedOnMaxOperationSize() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        long firstInFlightRequestSizeInBytes = 0;
        long firstInFlightRequestLargestDocumentSize = 0;
        int numberOfIndexRequests = randomIntBetween(50, 100);
        for (int i = 0; i < numberOfIndexRequests; i++) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(randomIntBetween(50, 100))));
            firstInFlightRequestSizeInBytes += request.ramBytesUsed();
            firstInFlightRequestLargestDocumentSize = Math.max(firstInFlightRequestLargestDocumentSize, request.source().length());
            assertTrue(request.ramBytesUsed() > request.source().length());
            bulkRequest.add(request);
        }

        long maxPrimaryBytes = (long) (firstInFlightRequestSizeInBytes * 1.5);
        restartNodesWithSettings(Settings.builder().put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), maxPrimaryBytes + "B").build());

        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames();
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        var primaryIndexOperationBlockedLatch = new CountDownLatch(1);
        var primaryIndexOperationDispatchedLatch = new CountDownLatch(1);
        PreIndexListenerInstallerPlugin.installPreIndexListener(((shardId, index) -> {
            if (index.origin().equals(Engine.Operation.Origin.PRIMARY)) {
                primaryIndexOperationDispatchedLatch.countDown();
                safeAwait(primaryIndexOperationBlockedLatch);
            }
        }));

        final ThreadPool replicaThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        final ThreadPool primaryThreadPool = internalCluster().getInstance(ThreadPool.class, replicaName);
        try (
            Releasable blockedPrimaryWriteThreadsRelease = blockWriteThreadPool(primaryThreadPool);
            Releasable blockedReplicaWriteThreadsRelease = blockWriteThreadPool(replicaThreadPool)
        ) {
            final ActionFuture<BulkResponse> successFuture = client(primaryName).bulk(bulkRequest);

            IndexingPressure primaryWriteLimits = internalCluster().getInstance(IndexingPressure.class, primaryName);
            IndexingPressure replicaWriteLimits = internalCluster().getInstance(IndexingPressure.class, replicaName);
            IndexingPressure coordinatingWriteLimits = internalCluster().getInstance(IndexingPressure.class, coordinatingOnlyNode);

            long inFlightPrimaryBytesBeforeDispatch = primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes();
            assertThat(inFlightPrimaryBytesBeforeDispatch, greaterThan(firstInFlightRequestSizeInBytes));

            blockedPrimaryWriteThreadsRelease.close();
            safeAwait(primaryIndexOperationDispatchedLatch);
            assertThat(
                primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes(),
                equalTo(inFlightPrimaryBytesBeforeDispatch + firstInFlightRequestLargestDocumentSize * 4)
            );
            primaryIndexOperationBlockedLatch.countDown();
            PreIndexListenerInstallerPlugin.resetPreIndexListener();

            var roomBeforePrimaryRejectionsInBytes = maxPrimaryBytes - primaryWriteLimits.stats()
                .getCurrentCombinedCoordinatingAndPrimaryBytes();
            var bulkRequestWithLargeExpandedOperation = new BulkRequest();
            var indexRequest = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength((int) roomBeforePrimaryRejectionsInBytes / 2)));
            // We need to ensure that the request would be able to be dispatched but once the document is expanded it will be rejected
            assertThat(indexRequest.ramBytesUsed(), is(lessThan(roomBeforePrimaryRejectionsInBytes)));
            bulkRequestWithLargeExpandedOperation.add(indexRequest);

            BulkResponse responses = client(coordinatingOnlyNode).bulk(bulkRequestWithLargeExpandedOperation).actionGet();
            assertTrue(responses.hasFailures());
            BulkItemResponse.Failure failure = responses.getItems()[0].getFailure();

            // The indexing memory pressure failing for this request is happening once the primary operation is dispatched into the write
            // thread pool, hence the triple exception nesting:
            // 1. TransportBulkAction (coordinator)
            // 2. TransportShardBulkAction (coordinator)
            // 3. TransportShardBulkAction (primary)
            // 4. TransportShardBulkAction[p] (primary)
            assertThat(failure.getCause().getCause().getCause(), instanceOf(EsRejectedExecutionException.class));

            blockedReplicaWriteThreadsRelease.close();

            successFuture.actionGet();

            assertEquals(0, primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
            assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
        }
    }

    public void testWriteCanRejectOnReplicaBasedOnMaxDocumentSize() throws Exception {
        final BulkRequest bulkRequest = new BulkRequest();
        long totalRequestSize = 0;
        int numberOfIndexRequests = randomIntBetween(50, 100);
        for (int i = 0; i < numberOfIndexRequests; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID()).source(Requests.INDEX_CONTENT_TYPE);
            totalRequestSize += request.ramBytesUsed();
            bulkRequest.add(request);
        }

        // The request meets the primary limits, but the replica limits are set lower, preventing it from proceeding
        IndexRequest largeIndexRequest = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
            .source(Collections.singletonMap("key", randomAlphaOfLength((int) totalRequestSize)));
        bulkRequest.add(largeIndexRequest);
        totalRequestSize += largeIndexRequest.ramBytesUsed();

        final long bulkShardRequestSize = totalRequestSize;
        restartNodesWithSettings(
            Settings.builder()
                .put(IndexingPressure.MAX_PRIMARY_BYTES.getKey(), bulkShardRequestSize * 5 + "B")
                .put(IndexingPressure.MAX_REPLICA_BYTES.getKey(), (long) (bulkShardRequestSize * 1.5) + "B")
                // Ensure that the replica request fails straight away after the first rejection
                .put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), 0)
                .build()
        );

        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames();
        String primaryName = primaryReplicaNodeNames.v1();
        String replicaName = primaryReplicaNodeNames.v2();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final ActionFuture<BulkResponse> successFuture = client(primaryName).bulk(bulkRequest);

        IndexingPressure primaryWriteLimits = internalCluster().getInstance(IndexingPressure.class, primaryName);
        IndexingPressure replicaWriteLimits = internalCluster().getInstance(IndexingPressure.class, replicaName);
        IndexingPressure coordinatingWriteLimits = internalCluster().getInstance(IndexingPressure.class, coordinatingOnlyNode);

        BulkResponse responses = successFuture.actionGet();
        assertFalse(responses.hasFailures());

        assertEquals(0, primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, primaryWriteLimits.stats().getCurrentReplicaBytes());
        assertEquals(0, primaryWriteLimits.stats().getPrimaryRejections());

        assertEquals(0, replicaWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, replicaWriteLimits.stats().getCurrentReplicaBytes());
        assertEquals(1L, replicaWriteLimits.stats().getReplicaRejections());

        assertEquals(0, coordinatingWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, coordinatingWriteLimits.stats().getCurrentReplicaBytes());
    }

    public void testDocumentsBeyondMaxSizeAreRejected() throws Exception {
        restartNodesWithSettings(Settings.builder().put(IndexingPressure.MAX_OPERATION_SIZE.getKey(), "10B").build());

        assertAcked(prepareCreate(INDEX_NAME, indexSettings(1, 1)));
        ensureGreen(INDEX_NAME);

        Tuple<String, String> primaryReplicaNodeNames = getPrimaryReplicaNodeNames();
        String primaryName = primaryReplicaNodeNames.v1();
        String coordinatingOnlyNode = getCoordinatingOnlyNode();

        final BulkRequest bulkRequest = new BulkRequest();
        int numberOfIndexRequests = randomIntBetween(50, 100);
        for (int i = 0; i < numberOfIndexRequests; ++i) {
            IndexRequest request = new IndexRequest(INDEX_NAME).id(UUIDs.base64UUID())
                .source(Collections.singletonMap("key", randomAlphaOfLength(50)));
            bulkRequest.add(request);
        }
        final ActionFuture<BulkResponse> successFuture = client(randomBoolean() ? primaryName : coordinatingOnlyNode).bulk(bulkRequest);

        IndexingPressure primaryWriteLimits = internalCluster().getInstance(IndexingPressure.class, primaryName);

        BulkResponse responses = successFuture.actionGet();
        assertTrue(responses.hasFailures());

        assertEquals(0, primaryWriteLimits.stats().getCurrentCombinedCoordinatingAndPrimaryBytes());
        assertEquals(0, primaryWriteLimits.stats().getCurrentPrimaryOps());
        assertEquals(0, primaryWriteLimits.stats().getCurrentPrimaryBytes());

        assertEquals(0, primaryWriteLimits.stats().getTotalPrimaryOps());
        assertEquals(0, primaryWriteLimits.stats().getTotalPrimaryBytes());

        assertEquals(1L, primaryWriteLimits.stats().getPrimaryRejections());
        assertEquals(1L, primaryWriteLimits.stats().getLargeOpsRejections());
        assertThat(primaryWriteLimits.stats().getTotalLargeRejectedOpsBytes(), is(greaterThanOrEqualTo(50L)));
    }

    private void restartNodesWithSettings(Settings settings) throws Exception {
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                return Settings.builder().put(unboundedWriteQueue).put(settings).build();
            }
        });
    }

    private String getCoordinatingOnlyNode() {
        return clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .nodes()
            .getCoordinatingOnlyNodes()
            .values()
            .iterator()
            .next()
            .getName();
    }

    private Tuple<String, String> getPrimaryReplicaNodeNames() {
        IndicesStatsResponse response = indicesAdmin().prepareStats(INDEX_NAME).get();
        String primaryId = Stream.of(response.getShards())
            .map(ShardStats::getShardRouting)
            .filter(ShardRouting::primary)
            .findAny()
            .get()
            .currentNodeId();
        String replicaId = Stream.of(response.getShards())
            .map(ShardStats::getShardRouting)
            .filter(sr -> sr.primary() == false)
            .findAny()
            .get()
            .currentNodeId();
        DiscoveryNodes nodes = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState().nodes();
        String primaryName = nodes.get(primaryId).getName();
        String replicaName = nodes.get(replicaId).getName();
        return new Tuple<>(primaryName, replicaName);
    }

    private Releasable blockWriteThreadPool(ThreadPool threadPool) {
        final CountDownLatch blockReplication = new CountDownLatch(1);
        final int threads = threadPool.info(ThreadPool.Names.WRITE).getMax();
        final CountDownLatch pointReached = new CountDownLatch(threads);
        for (int i = 0; i < threads; ++i) {
            threadPool.executor(ThreadPool.Names.WRITE).execute(() -> {
                try {
                    pointReached.countDown();
                    blockReplication.await();
                } catch (InterruptedException e) {
                    throw new IllegalStateException(e);
                }
            });
        }

        return () -> {
            if (blockReplication.getCount() > 0) {
                blockReplication.countDown();
            }
        };
    }

    public static class PreIndexListenerInstallerPlugin extends Plugin {
        public static AtomicReference<CheckedBiConsumer<ShardId, Engine.Index, Exception>> PRE_INDEX_CHECK_REF = new AtomicReference<>(
            ((shardId, index) -> {})
        );

        public PreIndexListenerInstallerPlugin() {}

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addIndexOperationListener(new InjectablePreIndexOperationListener(PRE_INDEX_CHECK_REF));
        }

        public static void installPreIndexListener(CheckedBiConsumer<ShardId, Engine.Index, Exception> preIndexCheck) {
            PRE_INDEX_CHECK_REF.set(preIndexCheck);
        }

        public static void resetPreIndexListener() {
            PRE_INDEX_CHECK_REF.set((shardId, index) -> {});
        }
    }

    static class InjectablePreIndexOperationListener implements IndexingOperationListener {
        private final AtomicReference<CheckedBiConsumer<ShardId, Engine.Index, Exception>> preIndexCheckRef;

        InjectablePreIndexOperationListener(AtomicReference<CheckedBiConsumer<ShardId, Engine.Index, Exception>> preIndexCheckRef) {
            this.preIndexCheckRef = preIndexCheckRef;
        }

        @Override
        public Engine.Index preIndex(ShardId shardId, Engine.Index index) {
            try {
                preIndexCheckRef.get().accept(shardId, index);
            } catch (Exception e) {
                throw new AssertionError("unexpected error", e);
            }
            return index;
        }
    }
}
