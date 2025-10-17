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
import org.elasticsearch.action.support.master.MasterNodeRequestHelper;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.PublicationTransportHandler;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportRequest;

import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;

public class StatelessIntermediateReshardRoutingIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        // Randomly the test framework sets this to 0, but we need retries in case the primary is not ready during target delegation.
        return super.nodeSettings().put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s");
    }

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

        Index index = resolveIndex(indexName);

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

        int multiple = 2;
        CyclicBarrier handoffTransitionBlock = new CyclicBarrier(2);
        CountDownLatch blockSplit = new CountDownLatch(1);

        // Intercept on the index node where target shard will send the state transition request
        // Since search nodes can't hold shards, the target shard will be on indexNode
        MockTransportService indexTransportService = MockTransportService.getInstance(indexNode);
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

        int docsToIndex = randomIntBetween(50, 100);
        try {
            // Reshard the index to two shards - send to master node (starts async)
            client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName, multiple)).actionGet();

            // Wait for all target shards to arrive at handoff point (blocked)
            handoffTransitionBlock.await();

            // Send an index request to the isolated search node (which doesn't know about the reshard)
            BulkRequestBuilder bulkRequestBuilder = client(isolatedSearchNode).prepareBulk();
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

            // Unblock reshard
            blockSplit.countDown();
        } finally {
            indexTransportService.clearAllRules();
            isolatedTransportService.clearAllRules();
        }

        publishTrivialClusterStateUpdate();

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
}
