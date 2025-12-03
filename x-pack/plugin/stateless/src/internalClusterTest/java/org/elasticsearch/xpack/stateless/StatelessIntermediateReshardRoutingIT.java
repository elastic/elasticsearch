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

import co.elastic.elasticsearch.stateless.reshard.ReshardIndexRequest;
import co.elastic.elasticsearch.stateless.reshard.SplitStateRequest;
import co.elastic.elasticsearch.stateless.reshard.TransportReshardAction;
import co.elastic.elasticsearch.stateless.reshard.TransportUpdateSplitStateAction;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;

import java.util.Locale;
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

// When the coordinator that is coordinating a replication request has stale cluster state (w.r.t resharding state),
// the replication request has to be forwarded to the target shard(s) at the primary node. This test is for
// testing such scenarios for various replication requests such as indexing, refresh and flush.
public class StatelessIntermediateReshardRoutingIT extends AbstractServerlessStatelessPluginIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        // Randomly the test framework sets this to 0, but we need retries in case the primary is not ready during target delegation.
        return super.nodeSettings().put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s");
    }

    // Test forwarding of indexing requests (by the primary shard) when the coordinator is so stale that
    // it is not even aware of ongoing resharding.
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
        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet();
        try {
            // Wait for all target shards to arrive at HANDOFF point (blocked)
            handoffTransitionBlock.await();

            // Send an index request to the isolated search node (which doesn't know about the reshard)
            indexDocsToNode(isolatedSearchNode, docsToIndex, indexName);

            // Unblock reshard
            blockSplit.countDown();
        } finally {
            indexTransportService.clearAllRules();
            isolatedTransportService.clearAllRules();
        }

        publishTrivialClusterStateUpdate();

        // Wait for reshard to complete
        finishReshardAndAssert(indexName, initialDocs, docsToIndex);
    }

    // Test forwarding of indexing requests (by the primary shard) when the coordinator knows about
    // resharding, but thinks that the target is still in CLONE state, and hence sends the request
    // only to the source primary.
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
        client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, 2)).actionGet();

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

            // Send an index request to the isolated search node (which only knows about CLONE state)
            indexDocsToNode(isolatedSearchNode, docsToIndex, indexName);

            // Unblock reshard
            blockSplit.countDown();
        } finally {
            indexTransportService.clearAllRules();
            isolatedTransportService.clearAllRules();
        }

        publishTrivialClusterStateUpdate();

        finishReshardAndAssert(indexName, initialDocs, docsToIndex);
    }

    // Test forwarding of flush requests by the primary shard when the coordinator is so stale that
    // it is not even aware of ongoing resharding.
    // Steps:
    // A flush request is sent by a stale coordinator to just the source primary.
    // The source and target index shards are blocked on the transition to HANDOFF.
    // Flush request is forwarded to the new shard by the source primary.
    // Flush response indicates the flush request was executed on both shards.
    public void testFlushRoutingWhenCoordinatorStale() throws Exception {
        testBroadcastRoutingWhenCoordinatorStale("flush", false);
    }

    // Similar to testFlushRoutingWhenCoordinatorStale(), except the coordinator knows about resharding.
    // The coordinator is stale and only knows about CLONE state.
    public void testFlushRoutingWhenCoordinatorStaleReshardingState() throws Exception {
        testBroadcastRoutingWhenCoordinatorStale("flush", true);
    }

    // Test forwarding of refresh requests by the primary shard when the coordinator is so stale that
    // it is not even aware of ongoing resharding.
    public void testRefreshRoutingWhenCoordinatorStale() throws Exception {
        testBroadcastRoutingWhenCoordinatorStale("refresh", false);
    }

    public void testRefreshRoutingWhenCoordinatorStaleReshardingState() throws Exception {
        testBroadcastRoutingWhenCoordinatorStale("refresh", true);
    }

    private void testBroadcastRoutingWhenCoordinatorStale(String actionType, boolean waitForReshardingState) throws Exception {
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

        if (waitForReshardingState) {
            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple)).actionGet();

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
            // Trigger reshard
            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple)).actionGet();
        }

        try {
            // Wait for shards to reach HANDOFF point
            handoffTransitionBlock.await();

            // Index more documents. This is just to add a layer of verification to this test.
            indexDocsToNode(indexNode, docsToIndex, indexName);

            // Perform broadcast action (flush or refresh) on isolated node
            verifyBroadcastRequestExecution(isolatedSearchNode, indexName, actionType, numShards.numPrimaries * multiple);

            // Wait for all target shards to arrive at SPLIT point (blocked)
            splitTransitionBlock.await();

            // Index more documents. This is just to add a layer of verification to this test.
            indexDocsToNode(indexNode, docsToIndex, indexName);

            // Perform broadcast action (flush or refresh) on isolated node
            verifyBroadcastRequestExecution(isolatedSearchNode, indexName, actionType, numShards.numPrimaries * multiple);

            // Unblock reshard
            blockDone.countDown();
        } finally {
            indexTransportService.clearAllRules();
            isolatedTransportService.clearAllRules();
        }

        numShards = getNumShards(indexName);
        logger.info("Final Number of shards in index primary: {}, replica: {}", numShards.numPrimaries, numShards.numReplicas);
        // Validate completion
        finishReshardAndAssert(indexName, initialDocs, 2 * docsToIndex);
    }

    private static void verifyBroadcastRequestExecution(String isolatedSearchNode, String indexName, String actionType, int numShards) {
        BroadcastResponse response;
        if ("flush".equals(actionType)) {
            response = client(isolatedSearchNode).admin().indices().prepareFlush(indexName).get();
        } else if ("refresh".equals(actionType)) {
            response = client(isolatedSearchNode).admin().indices().prepareRefresh(indexName).get();
        } else {
            throw new IllegalArgumentException("Unsupported action type: " + actionType);
        }

        assertNoFailures(response);
        assertThat(response.getSuccessfulShards(), equalTo(numShards));
    }

    private static void holdAtHandoff(
        MockTransportService indexTransportService,
        CyclicBarrier handoffTransitionBlock,
        CountDownLatch blockSplit
    ) {
        indexTransportService.addSendBehavior((connection, requestId, action, request, options) -> {
            try {
                if (TransportUpdateSplitStateAction.TYPE.name().equals(action)) {
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
                if (TransportUpdateSplitStateAction.TYPE.name().equals(action)) {
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

    private void finishReshardAndAssert(String indexName, int initialDocs, int docsToIndex) {
        // Wait for reshard to complete
        awaitClusterState(clusterState -> clusterState.projectState().metadata().index(indexName).getReshardingMetadata() == null);

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
                    equalTo(2)
                );
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo((long) expectedTotalDocs));
            }
        );
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
}
