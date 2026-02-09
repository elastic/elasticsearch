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

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.indices.flush.TransportShardFlushAction;
import org.elasticsearch.action.admin.indices.refresh.TransportShardRefreshAction;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.routing.IndexRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexRequest;
import org.elasticsearch.xpack.stateless.reshard.SplitStateRequest;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardAction;
import org.elasticsearch.xpack.stateless.reshard.TransportReshardSplitAction;
import org.elasticsearch.xpack.stateless.reshard.TransportUpdateSplitTargetShardStateAction;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

// Test that requests (index, flush, refresh) handled by a stale coordinating are forwarded correctly by the primary node
// -- When the coordinator that is coordinating a replication request has stale cluster state (w.r.t resharding state),
// -- the replication request has to be forwarded to the target shard(s) at the primary node. This test is for
// -- testing such scenarios for various replication requests such as indexing, refresh and flush.

// During resharding HANDOFF step, test that the primary node does not release permits
// (and hence does not process queued request) until it has observed HANDOFF on the target shard.
public class StatelessIntermediateReshardRoutingIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        // Randomly the test framework sets this to 0, but we need retries in case the primary is not ready during target delegation.
        return super.nodeSettings().put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s");
    }

    // Test forwarding of indexing requests (by the primary shard) when the coordinator is so stale that
    // it is not even aware of ongoing resharding.
    // This test also validates that realtime GET requests behave correctly when the coordinator
    // is completely unaware of the reshard.
    public void testIndexRoutingWhenCoordinatorStale() throws Exception {
        // Create master and index nodes
        String masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        // Create an index with one shard
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        // Index some initial documents
        int initialDocs = randomIntBetween(10, 20);
        indexDocs(indexName, initialDocs);
        flush(indexName);

        // Get index metadata to calculate post-split routing
        final Index index = resolveIndex(indexName);
        final IndexMetadata currentIndexMetadata = indexMetadata(internalCluster().clusterService(indexNode).state(), index);
        final IndexMetadata indexMetadataPostSplit = IndexMetadata.builder(currentIndexMetadata).reshardAddShards(2).build();
        final IndexRouting indexRoutingPostSplit = IndexRouting.fromIndexMetadata(indexMetadataPostSplit);

        // Generate document IDs that will route to specific shards after split
        final String shard0docId = makeRoutingValueForShard(indexRoutingPostSplit, 0);
        final String shard1docId = makeRoutingValueForShard(indexRoutingPostSplit, 1);

        // Create a search node to act as the isolated coordinator
        // Search nodes cannot hold index shards, so target shard will be on indexNode
        String isolatedSearchNode = startSearchNode();
        ensureStableCluster(4);

        // Isolate the search node from getting cluster state updates by blocking publication
        MockTransportService isolatedTransportService = MockTransportService.getInstance(isolatedSearchNode);
        isolatedTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> {
                // Block cluster state updates by sending an error response
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                logger.info("Blocking cluster state publication on isolated search node");
                channel.sendResponse(new IllegalStateException("cluster state updates blocked"));
            }
        );

        CyclicBarrier handoffTransitionBlock = new CyclicBarrier(2);
        CountDownLatch blockSplit = new CountDownLatch(1);

        // Intercept on the index node where target shard will send the state transition request
        // Since search nodes can't hold shards, the target shard will be on indexNode
        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        holdAtHandoff(indexTransportService, handoffTransitionBlock, blockSplit);

        int docsToIndex = randomIntBetween(50, 100);

        // Reshard the index to two shards - send to master node (starts async)
        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        try {
            // Wait for all target shards to arrive at HANDOFF point (blocked)
            handoffTransitionBlock.await();

            // Index documents with specific routing values to the isolated search node (which doesn't know about the reshard)
            // Index one document that will go to shard 0 (source shard)
            client(isolatedSearchNode).prepareIndex(indexName)
                .setId(shard0docId)
                .setRouting(shard0docId)
                .setSource("field", "value_shard0")
                .get();

            // Index one document that will go to shard 1 (target shard)
            client(isolatedSearchNode).prepareIndex(indexName)
                .setId(shard1docId)
                .setRouting(shard1docId)
                .setSource("field", "value_shard1")
                .get();

            // Index additional random documents
            indexDocsToNode(isolatedSearchNode, docsToIndex, indexName);

            // Validate GET and MultiGet routing with stale coordinator
            validateGetRoutingWithStaleCoordinator(isolatedSearchNode, indexName, shard0docId, shard1docId);

            // Unblock reshard
            blockSplit.countDown();
        } finally {
            indexTransportService.clearAllRules();
            isolatedTransportService.clearAllRules();
        }

        publishTrivialClusterStateUpdate();

        // Wait for reshard to complete - account for the two specific documents we indexed
        finishReshardAndAssert(indexName, initialDocs, docsToIndex + 2, 2);
    }

    // Test forwarding of indexing requests (by the primary shard) when the coordinator knows about
    // resharding, but thinks that the target is still in CLONE state, and hence sends the request
    // only to the source primary.
    // This test also validates that realtime GET requests are properly routed to both source and target
    // shards when the coordinator has stale cluster state.
    public void testIndexRoutingWhenCoordinatorStaleReshardState() throws Exception {
        // Create master and index nodes
        String masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        // Create an index with one shard
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        // Index some initial documents
        int initialDocs = randomIntBetween(10, 20);
        indexDocs(indexName, initialDocs);
        flush(indexName);

        // Get index metadata to calculate post-split routing
        final Index index = resolveIndex(indexName);
        final IndexMetadata currentIndexMetadata = indexMetadata(internalCluster().clusterService(indexNode).state(), index);
        final IndexMetadata indexMetadataPostSplit = IndexMetadata.builder(currentIndexMetadata).reshardAddShards(2).build();
        final IndexRouting indexRoutingPostSplit = IndexRouting.fromIndexMetadata(indexMetadataPostSplit);

        // Generate document IDs that will route to specific shards after split
        final String shard0docId = makeRoutingValueForShard(indexRoutingPostSplit, 0);
        final String shard1docId = makeRoutingValueForShard(indexRoutingPostSplit, 1);

        // Create a search node to act as the isolated coordinator
        // Search nodes cannot hold index shards, so target shard will be on indexNode
        String isolatedSearchNode = startSearchNode();
        ensureStableCluster(4);

        // Isolate the search node from getting cluster state updates by blocking publication
        MockTransportService isolatedTransportService = MockTransportService.getInstance(isolatedSearchNode);

        CyclicBarrier handoffTransitionBlock = new CyclicBarrier(2);
        CountDownLatch blockSplit = new CountDownLatch(1);

        // Intercept on the index node where target shard will send the state transition request
        // Since search nodes can't hold shards, the target shard will be on indexNode
        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        holdAtHandoff(indexTransportService, handoffTransitionBlock, blockSplit);

        int docsToIndex = randomIntBetween(50, 100);
        // Reshard the index to two shards - send to master node (starts async)
        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

        waitForClusterState(isolatedSearchNode, clusterState -> {
            IndexReshardingMetadata reshardingMetadata = clusterState.projectState().metadata().index(indexName).getReshardingMetadata();
            return reshardingMetadata != null
                && reshardingMetadata.getSplit().getTargetShardState(1) == IndexReshardingState.Split.TargetShardState.CLONE;
        }).actionGet();

        isolatedTransportService.addRequestHandlingBehavior(
            PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
            (handler, request, channel, task) -> {
                // Block cluster state updates by sending an error response
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                logger.info("Blocking cluster state publication on isolated search node");
                channel.sendResponse(new IllegalStateException("cluster state updates blocked"));
            }
        );

        try {
            // Wait for all target shards to arrive at HANDOFF point (blocked)
            handoffTransitionBlock.await();

            // Index documents with specific routing values to the isolated search node (which only knows about CLONE state)
            // Index one document that will go to shard 0 (source shard)
            client(isolatedSearchNode).prepareIndex(indexName)
                .setId(shard0docId)
                .setRouting(shard0docId)
                .setSource("field", "value_shard0")
                .get();

            // Index one document that will go to shard 1 (target shard)
            client(isolatedSearchNode).prepareIndex(indexName)
                .setId(shard1docId)
                .setRouting(shard1docId)
                .setSource("field", "value_shard1")
                .get();

            // Index additional random documents
            indexDocsToNode(isolatedSearchNode, docsToIndex, indexName);

            // Validate GET and MultiGet routing with stale coordinator
            validateGetRoutingWithStaleCoordinator(isolatedSearchNode, indexName, shard0docId, shard1docId);

            // Unblock reshard
            blockSplit.countDown();
        } finally {
            indexTransportService.clearAllRules();
            isolatedTransportService.clearAllRules();
        }

        publishTrivialClusterStateUpdate();

        // Account for the two specific documents we indexed
        finishReshardAndAssert(indexName, initialDocs, docsToIndex + 2, 2);
    }

    // Test forwarding of flush requests by the primary shard when the coordinator is so stale that
    // it is not even aware of ongoing resharding.
    // Steps:
    // A flush request is sent by a stale coordinator to just the source primary.
    // The source and target index shards are blocked on the transition to HANDOFF.
    // Flush request is forwarded to the new shard by the source primary.
    // Flush response indicates the flush request was executed on both shards.
    public void testFlushRoutingWhenCoordinatorStale() throws Exception {
        testBroadcastRoutingWhenCoordinatorStale(false);
    }

    // Similar to testFlushRoutingWhenCoordinatorStale(), except the coordinator knows about resharding.
    // The coordinator is stale and only knows about CLONE state.
    public void testFlushRoutingWhenCoordinatorStaleReshardingState() throws Exception {
        testBroadcastRoutingWhenCoordinatorStale(true);
    }

    public void testRefreshBlockedUntilPrimaryObservesHandoff() throws InterruptedException {
        testOperationBlockedUntilPrimaryObservesHandoff("refresh");
    }

    public void testFlushBlockedUntilPrimaryObservesHandoff() throws InterruptedException {
        testOperationBlockedUntilPrimaryObservesHandoff("flush");
    }

    public void testStaleIndexRoutingAfterReshardingMetadataGone() throws Exception {
        testStaleOperationRoutingAfterReshardingMetadataGone("write");
    }

    public void testStaleRefreshRoutingAfterReshardingMetadataGone() throws Exception {
        testStaleOperationRoutingAfterReshardingMetadataGone("refresh");
    }

    public void testStaleFlushRoutingAfterReshardingMetadataGone() throws Exception {
        testStaleOperationRoutingAfterReshardingMetadataGone("flush");
    }

    public void testStaleIndexRoutingAfterMultipleReshards() throws Exception {
        testVeryStaleOperationsFail("write", true);
    }

    public void testStaleRefreshRoutingAfterMultipleReshards() throws Exception {
        testVeryStaleOperationsFail("refresh", true);
    }

    public void testStaleFlushRoutingAfterMultipleReshards() throws Exception {
        testVeryStaleOperationsFail("flush", true);
    }

    // Test that stale Operations are rejected after Reshard metadata is Gone.
    private void testStaleOperationRoutingAfterReshardingMetadataGone(String actionType) throws Exception {
        testVeryStaleOperationsFail(actionType, false);
    }

    // Replication Operations (bulk shard indexing, flush and refresh) that are so stale that the resharding metadata is gone
    // or there have been multiple reshard split operations in the meantime, should be failed.
    private void testVeryStaleOperationsFail(String actionType, boolean multipleReshards) throws Exception {
        // Create master and index nodes
        String masterNode = startMasterOnlyNode();
        startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        // Create an index with one shard
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        // Index some initial documents
        int initialDocs = randomIntBetween(50, 100);
        indexDocs(indexName, initialDocs);
        flush(indexName);

        String coordinatingNode = startIndexNode();
        ensureStableCluster(4);

        final var SHARD_REPLICATION_ACTION = getActionName(actionType);
        final var replicationRequestPrepared = new CountDownLatch(1);
        final var reshardDone = new CountDownLatch(1);
        final var transportService = MockTransportService.getInstance(coordinatingNode);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            // block Replication request once it is prepared until resharding completes
            if ((SHARD_REPLICATION_ACTION).equals(action)) {
                logger.info("Block shard replication action");
                // signal that get has been prepared so resharding can start
                replicationRequestPrepared.countDown();
                safeAwait(reshardDone);
            }
            connection.sendRequest(requestId, action, request, options);
        });

        int docsToIndex = 0;
        if (actionType.equals("write")) {
            docsToIndex = randomIntBetween(50, 100);
        }

        logger.info("Submit bulk requests async");
        ActionFuture<? extends RefCounted> future;
        if (actionType.equals("write")) {
            future = indexDocsToNodeAsync(coordinatingNode, docsToIndex, indexName);
        } else {
            future = executeBroadcastRequestAsync(coordinatingNode, indexName, actionType);
        }

        final var waitForResponseThread = new Thread(() -> {
            if (actionType.equals("write")) {
                final BulkResponse response = (BulkResponse) future.actionGet();
                assertThat(response.buildFailureMessage(), response.hasFailures(), equalTo(true));
            } else {
                final BroadcastResponse response = (BroadcastResponse) future.actionGet();
                assertThat(response.getStatus(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
                assertThat(response.getSuccessfulShards(), equalTo(0));
            }
        });
        waitForResponseThread.start();

        // Reshard the index to two shards - send to master node (starts async)
        // don't start resharding until request is waiting on the index node
        replicationRequestPrepared.await();
        logger.info("Replication request submitted and blocked, initiate reshard");
        int newShardCount = 2;
        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, newShardCount)).actionGet();
        waitForReshardCompletion(indexName);
        logger.info("Reshard complete");
        if (multipleReshards) {
            newShardCount = 4;
            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, newShardCount)).actionGet();
            waitForReshardCompletion(indexName);
            logger.info("Second Reshard complete");
        }

        // Verify that request is still pending
        assertThat(future.isDone(), equalTo(false));
        logger.info("Verified that stale request is still pending");

        reshardDone.countDown();

        waitForResponseThread.join();

        // Verify number of documents is same as before
        finishReshardAndAssert(indexName, initialDocs, 0, newShardCount);
    }

    // This test is to make sure that requests that are received once target is in HANDOFF,
    // are routed correctly by a source shard even if it has not observed HANDOFF yet.
    // In fact, in this case the source is holding onto permits while waiting to see the HANDOFF state
    // and will only process requests (forwarding requests to target if needed) once it releases the permits.
    private void testOperationBlockedUntilPrimaryObservesHandoff(String actionType) throws InterruptedException {
        // -- Cluster setup
        String masterNode = startMasterOnlyNode();
        // We will delay the publication of the HANDOFF (cluster state update) on this node
        // and test if requests are routed correctly.
        String indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        // -- Index creation
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        NumShards numShards = getNumShards(indexName);

        // -- Index initial documents
        int initialDocs = randomIntBetween(10, 20);
        indexDocs(indexName, initialDocs);
        BroadcastResponse response = flush(indexName);
        assertNoFailures(response);
        assertThat(response.getSuccessfulShards(), equalTo(numShards.numPrimaries));

        // Create another indexing node that will observe all state transitions.
        String targetIndexNode = startIndexNode();
        ensureStableCluster(4);

        // We will isolate the source index node from getting cluster state updates by blocking publication
        MockTransportService isolatedTransportService = MockTransportService.getInstance(indexNode);

        // Before sending HANDOFF ACTION from source node, block further updates on it.
        isolatedTransportService.addSendBehavior((connection, requestId, action, request1, options) -> {
            if (TransportReshardSplitAction.SPLIT_HANDOFF_ACTION_NAME.equals(action)) {
                // Now isolate the node with the source shard before updating target state to HANDOFF
                isolatedTransportService.addRequestHandlingBehavior(
                    PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
                    (handler, request, channel, task) -> {
                        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                        logger.info("Blocking cluster state publication on source shard index node");
                        channel.sendResponse(new IllegalStateException("cluster state updates blocked"));
                    }
                );
            }
            connection.sendRequest(requestId, action, request1, options);
        });

        // Reshard the index to two shards - send to master node (starts async)
        int multiple = 2;
        int newShardCount = multiple * numShards.numPrimaries;
        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, newShardCount)).actionGet();

        // Wait for source node to see target CLONE state
        waitForClusterState(indexNode, clusterState -> {
            IndexReshardingMetadata reshardingMetadata = clusterState.projectState().metadata().index(indexName).getReshardingMetadata();
            return reshardingMetadata != null
                && reshardingMetadata.getSplit().getTargetShardState(1) == IndexReshardingState.Split.TargetShardState.CLONE;
        }).actionGet();

        // Wait for unblocked index node to see target HANDOFF state
        waitForClusterState(targetIndexNode, clusterState -> {
            IndexReshardingMetadata reshardingMetadata = clusterState.projectState().metadata().index(indexName).getReshardingMetadata();
            return reshardingMetadata != null
                && reshardingMetadata.getSplit().getTargetShardState(1) == IndexReshardingState.Split.TargetShardState.HANDOFF;
        }).actionGet();

        logger.info("Submit bulk indexing request async");
        int docsToIndex = randomIntBetween(50, 100);
        final var bulkFuture = indexDocsToNodeAsync(indexNode, docsToIndex, indexName);

        logger.info("Submit broadcast action async");
        // Execute async broadcast action (flush or refresh) on isolated node
        final var future = executeBroadcastRequestAsync(indexNode, indexName, actionType);
        // Indexing, Flush or Refresh will not complete until we allow cluster updates to be seen by source node
        LockSupport.parkNanos(new TimeValue(100, TimeUnit.MILLISECONDS).nanos());
        assertThat(future.isDone(), equalTo(false));
        assertThat(bulkFuture.isDone(), equalTo(false));
        // Once the source node sees target in HANDOFF state, the broadcast action will get unblocked.
        final var futureClusterState = waitForClusterState(indexNode, clusterState -> {
            IndexReshardingMetadata reshardingMetadata = clusterState.projectState().metadata().index(indexName).getReshardingMetadata();
            return reshardingMetadata != null
                && reshardingMetadata.getSplit().targetStateAtLeast(1, IndexReshardingState.Split.TargetShardState.HANDOFF);
        });
        final var futureThread = new Thread(() -> {
            final var bulkResponse = bulkFuture.actionGet();
            final var futureResponse = future.actionGet();
            // Verify that HANDOFF has been observed on source shard node when indexing and flush/refresh get unblocked
            assertThat(futureClusterState.isDone(), equalTo(true));
            assertNoFailures(futureResponse);
            assertNoFailures(bulkResponse);
            assertThat(futureResponse.getSuccessfulShards(), equalTo(newShardCount));
        });
        futureThread.start();

        isolatedTransportService.clearAllRules();
        logger.info("Unblocked Source node cluster state updates");

        publishTrivialClusterStateUpdate();

        // Once the source node sees target in HANDOFF state, the broadcast action will get unblocked.
        futureClusterState.actionGet();

        // The broadcast action that was blocked earlier should complete now
        futureThread.join(SAFE_AWAIT_TIMEOUT.millis());

        finishReshardAndAssert(indexName, initialDocs, docsToIndex, newShardCount);
    }

    // Test forwarding of flush requests (by the primary shard) when the coordinator does not know the most
    // current state of resharding, and hence sends the request only to the source primary.
    // waitForReshardingState == false simulates the case when stale coordinator is unaware of ongoing resharding
    // waitForReshardingState == true simulates the case when stale coordinator knows about resharding, but thinks
    // that the target is still in CLONE state
    private void testBroadcastRoutingWhenCoordinatorStale(boolean waitForReshardingState) throws Exception {
        // -- Cluster setup
        String masterNode = startMasterOnlyNode();
        String indexNode = startIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        // -- Index creation
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        NumShards numShards = getNumShards(indexName);
        logger.info("Initial Number of shards in index primary: {}, replica: {}", numShards.numPrimaries, numShards.numReplicas);

        // -- Index initial documents
        int initialDocs = randomIntBetween(10, 20);
        indexDocs(indexName, initialDocs);
        BroadcastResponse response = flush(indexName);
        assertNoFailures(response);
        assertThat(response.getSuccessfulShards(), equalTo(numShards.numPrimaries));

        // -- Isolate a coordinator node
        // Use the cluster.routing.allocation.exclude setting to prevent allocations on this node
        // This simulates a coordinator only node. Otherwise, as soon as the target index shard goes to HANDOFF state,
        // we might try to allocate search shards on this node, which will be a problem since cluster updates are blocked on this node.
        String isolatedSearchNode = startSearchNode();
        client().admin()
            .cluster()
            .prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT)
            .setPersistentSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", isolatedSearchNode))
            .get();
        ensureStableCluster(4);
        MockTransportService isolatedTransportService = MockTransportService.getInstance(isolatedSearchNode);

        CyclicBarrier handoffTransitionBlock = new CyclicBarrier(2);
        CyclicBarrier splitTransitionBlock = new CyclicBarrier(2);
        CountDownLatch blockDone = new CountDownLatch(1);

        // Intercept on the index node
        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
        holdAtHandoffAndSplit(indexTransportService, handoffTransitionBlock, splitTransitionBlock, blockDone);

        int docsToIndex = randomIntBetween(50, 100);
        int multiple = 2;
        int newShardCount = multiple * numShards.numPrimaries;

        if (waitForReshardingState) {
            // Reshard the index to two shards - send to master node (starts async)
            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();

            waitForClusterState(isolatedSearchNode, clusterState -> {
                IndexReshardingMetadata reshardingMetadata = clusterState.projectState()
                    .metadata()
                    .index(indexName)
                    .getReshardingMetadata();
                return reshardingMetadata != null
                    && reshardingMetadata.getSplit().getTargetShardState(1) == IndexReshardingState.Split.TargetShardState.CLONE;
            }).actionGet();

            // Isolate the search node from getting cluster state updates by blocking publication (once it knows about CLONE state)
            isolatedTransportService.addRequestHandlingBehavior(
                PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    // Block cluster state updates by sending an error response
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                    logger.info("Blocking cluster state publication on isolated search node");
                    channel.sendResponse(new IllegalStateException("cluster state updates blocked"));
                }
            );
        } else {
            // Block cluster state updates on the isolated node
            isolatedTransportService.addRequestHandlingBehavior(
                PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
                (handler, request, channel, task) -> {
                    LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
                    logger.info("Blocking cluster state publication on isolated search node");
                    channel.sendResponse(new IllegalStateException("cluster state updates blocked"));
                }
            );
            // Reshard the index to two shards - send to master node (starts async)
            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet();
        }

        try {
            // Wait for shards to reach HANDOFF point
            handoffTransitionBlock.await();

            // Index more documents. This is just to add a layer of verification to this test.
            indexDocsToNode(indexNode, docsToIndex, indexName);

            // Perform broadcast action (flush or refresh) on isolated node
            verifyBroadcastRequestExecution(isolatedSearchNode, indexName, "flush", newShardCount);

            // Wait for all target shards to arrive at SPLIT point (blocked)
            splitTransitionBlock.await();

            // Index more documents. This is just to add a layer of verification to this test.
            indexDocsToNode(indexNode, docsToIndex, indexName);

            // Perform broadcast action (flush or refresh) on isolated node
            verifyBroadcastRequestExecution(isolatedSearchNode, indexName, "flush", newShardCount);

            isolatedTransportService.clearAllRules();
            // Since `isolatedSearchNode` was isolated we will wait for it to observe the transition to SPLIT
            // in the cluster state.
            // But at this point cluster state doesn't change so we'll simply remove this node to be able to progress.
            internalCluster().stopNode(isolatedSearchNode);

            blockDone.countDown();
        } finally {
            indexTransportService.clearAllRules();
            isolatedTransportService.clearAllRules();
        }

        publishTrivialClusterStateUpdate();
        numShards = getNumShards(indexName);
        logger.info("Final Number of shards in index primary: {}, replica: {}", numShards.numPrimaries, numShards.numReplicas);
        // Validate completion
        finishReshardAndAssert(indexName, initialDocs, 2 * docsToIndex, newShardCount);
    }

    private static void verifyBroadcastRequestExecution(String isolatedNode, String indexName, String actionType, int numShards) {
        BroadcastResponse response = executeBroadcastRequestAsync(isolatedNode, indexName, actionType).actionGet();

        assertNoFailures(response);
        assertThat(response.getSuccessfulShards(), equalTo(numShards));
    }

    private static ActionFuture<BroadcastResponse> executeBroadcastRequestAsync(String isolatedNode, String indexName, String actionType) {
        ActionFuture<BroadcastResponse> future;
        if ("flush".equals(actionType)) {
            future = client(isolatedNode).admin().indices().prepareFlush(indexName).execute();
        } else if ("refresh".equals(actionType)) {
            future = client(isolatedNode).admin().indices().prepareRefresh(indexName).execute();
        } else {
            throw new IllegalArgumentException("Unsupported action type: " + actionType);
        }
        return (future);
    }

    private static void holdAtHandoff(
        MockTransportService indexTransportService,
        CyclicBarrier handoffTransitionBlock,
        CountDownLatch blockSplit
    ) {
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    // Unwrap TermOverridingMasterNodeRequest if needed
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                    // Only block transition to HANDOFF state
                    if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.HANDOFF) {
                            // Line up all target shards here
                            handoffTransitionBlock.await();
                        } else if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                            blockSplit.await();
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static void holdAtHandoffAndSplit(
        MockTransportService indexTransportService,
        CyclicBarrier handoffTransitionBlock,
        CyclicBarrier splitTransitionBlock,
        CountDownLatch blockDone
    ) {
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitTargetShardStateAction.TYPE.name().equals(action)) {
                    // Unwrap TermOverridingMasterNodeRequest if needed
                    TransportRequest actualRequest = MasterNodeRequestHelper.unwrapTermOverride(request);

                    // Block transition to HANDOFF state, then SPLIT
                    if (actualRequest instanceof SplitStateRequest splitStateRequest) {
                        if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.HANDOFF) {
                            // Line up all target shards here
                            handoffTransitionBlock.await();
                        } else if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.SPLIT) {
                            splitTransitionBlock.await();
                        } else if (splitStateRequest.getNewTargetShardState() == IndexReshardingState.Split.TargetShardState.DONE) {
                            blockDone.await();
                        }
                    }
                }
                connection.sendRequest(requestId, action, request, options);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static ActionFuture<BulkResponse> indexDocsToNodeAsync(String node, int docsToIndex, String indexName) {
        BulkRequestBuilder bulkRequestBuilder = client(node).prepareBulk();
        for (int i = 0; i < docsToIndex; i++) {
            bulkRequestBuilder.add(new IndexRequest(indexName).source("field", randomAlphaOfLength(10)));
        }
        ActionFuture<BulkResponse> future = bulkRequestBuilder.execute();
        return future;
    }

    private void indexDocsToNode(String node, int docsToIndex, String indexName) {
        // Send an index request to the isolated search node (which doesn't know about the reshard)
        BulkRequestBuilder bulkRequestBuilder = client(node).prepareBulk();
        for (int i = 0; i < docsToIndex; i++) {
            bulkRequestBuilder.add(new IndexRequest(indexName).source("field", randomAlphaOfLength(10)));
        }
        BulkResponse response = bulkRequestBuilder.get();
        try {
            assertNoFailures(response);
        } catch (AssertionError e) {
            for (BulkItemResponse itemResponse : response) {
                if (itemResponse.isFailed()) {
                    logger.error("Failed bulk item", ExceptionsHelper.unwrapCause(itemResponse.getFailure().getCause()));
                }
            }
            throw e;
        }
    }

    // The callers of this function don't explicitly ensure that documents are routed to all available shards, but we assert
    // here that they do. We just index enough random document ids into the index to maximize the probability that documents route to
    // all available shards.
    private void finishReshardAndAssert(String indexName, int initialDocs, int docsToIndex, int numShards) {
        // Wait for reshard to complete
        waitForReshardCompletion(indexName);

        ensureGreen(indexName);
        refresh(indexName);

        // Validate that all the documents exist after the partition during reshard
        int expectedTotalDocs = initialDocs + docsToIndex;
        assertResponse(
            prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(expectedTotalDocs).setTrackTotalHits(true),
            searchResponse -> {
                assertNoFailures(searchResponse);
                assertThat(
                    StreamSupport.stream(searchResponse.getHits().spliterator(), false)
                        .map(hit -> hit.getShard().getShardId())
                        .collect(Collectors.toSet())
                        .size(),
                    equalTo(numShards)
                );
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) expectedTotalDocs));
            }
        );
    }

    private void waitForReshardCompletion(String indexName) {
        // Reshard metadata is removed after completion
        awaitClusterState(clusterState -> clusterState.projectState().metadata().index(indexName).getReshardingMetadata() == null);
    }

    private static void publishTrivialClusterStateUpdate() {
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .submitUnbatchedStateUpdateTask("trivial cluster state update", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    return ClusterState.builder(currentState).build();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
    }

    private PlainActionFuture<ClusterState> waitForClusterState(String node, Predicate<ClusterState> predicate) {
        return StatelessReshardIT.waitForClusterState(logger, internalCluster().clusterService(node), predicate);
    }

    /**
     * Validates GET and MultiGet behavior when coordinator has stale cluster state during reshard.
     * Tests that:
     * - Document on source shard (shard 0) can be retrieved via realtime GET
     * - Document on target shard (shard 1) fails with realtime GET (forwarding not implemented)
     * - Document on target shard returns not found with non-realtime GET
     * - MultiGet returns mixed results (success for shard 0, failure for shard 1)
     */
    private void validateGetRoutingWithStaleCoordinator(String coordinatorNode, String indexName, String shard0docId, String shard1docId) {
        // Perform realtime GET requests for both documents from the same stale coordinator
        GetResponse getShard0Response = client(coordinatorNode).prepareGet(indexName, shard0docId)
            .setRouting(shard0docId)
            .setRealtime(true)
            .get();
        assertThat("Document routed to source shard should be found", getShard0Response.isExists(), equalTo(true));
        assertThat(getShard0Response.getSource().get("field"), equalTo("value_shard0"));

        // The target shard forwarding and retries are not implemented yet.
        expectThrows(
            ElasticsearchStatusException.class,
            () -> client(coordinatorNode).prepareGet(indexName, shard1docId).setRouting(shard1docId).setRealtime(true).get()
        );

        GetResponse getShard1Response = client(coordinatorNode).prepareGet(indexName, shard1docId)
            .setRouting(shard1docId)
            .setRealtime(false)
            .get();
        assertThat("Target document not found on source shard", getShard1Response.isExists(), equalTo(false));

        // Perform realtime multiget request for both documents from the same stale coordinator
        MultiGetResponse mgetResponse = client(coordinatorNode).prepareMultiGet()
            .add(indexName, shard0docId)
            .add(indexName, shard1docId)
            .setRealtime(true)
            .get();

        // Verify we have results for both documents
        assertThat(mgetResponse.getResponses().length, equalTo(2));

        // Document that stays on shard 0 (source) should succeed
        MultiGetItemResponse item0 = mgetResponse.getResponses()[0];
        assertThat("Document on source shard should succeed", item0.isFailed(), equalTo(false));
        assertThat(item0.getResponse().isExists(), equalTo(true));
        assertThat(item0.getResponse().getSource().get("field"), equalTo("value_shard0"));

        // Document that moves to shard 1 (target) should fail due to stale routing
        MultiGetItemResponse item1 = mgetResponse.getResponses()[1];
        assertThat("Document moved to target shard should fail due to stale routing", item1.isFailed(), equalTo(true));
        assertThat(item1.getFailure().getFailure(), instanceOf(ElasticsearchStatusException.class));
    }

    /**
     * Generate a string that if used as id or routing value will route to the given shardId of indexRouting.
     */
    private String makeRoutingValueForShard(IndexRouting indexRouting, int shardId) {
        while (true) {
            String routingValue = randomAlphaOfLength(5);
            int routedShard = indexRouting.indexShard(new IndexRequest().id("dummy").routing(routingValue));
            if (routedShard == shardId) {
                return routingValue;
            }
        }
    }

    private static IndexMetadata indexMetadata(ClusterState state, Index index) {
        return state.projectState().metadata().index(index);
    }

    private String getActionName(String actionType) {
        final String actionName = Map.of(
            "refresh",
            TransportShardRefreshAction.NAME,
            "flush",
            TransportShardFlushAction.NAME,
            "write",
            TransportShardBulkAction.ACTION_NAME
        ).get(actionType);
        return actionName;
    }
}
