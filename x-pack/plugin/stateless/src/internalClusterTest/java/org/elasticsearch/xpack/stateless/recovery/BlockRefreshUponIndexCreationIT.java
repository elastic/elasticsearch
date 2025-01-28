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

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BlockRefreshUponIndexCreationIT extends AbstractStatelessIntegTestCase {

    public void testIndexCreatedWithRefreshBlock() {
        startMasterAndIndexNode(useRefreshBlockSetting(true));

        int nbReplicas = randomIntBetween(0, 3);
        if (0 < nbReplicas) {
            startSearchNodes(nbReplicas);
        }

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, nbReplicas)));
        ensureGreen(indexName);

        var blocks = clusterBlocks();
        assertThat(blocks.hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(0 < nbReplicas));
    }

    public void testIndexCreatedWithRefreshBlockDisabled() {
        startMasterAndIndexNode(useRefreshBlockSetting(false));

        int nbReplicas = randomIntBetween(0, 3);
        if (0 < nbReplicas) {
            startSearchNodes(nbReplicas);
        }

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, nbReplicas)));
        ensureGreen(indexName);

        var blocks = clusterBlocks();
        assertThat(blocks.hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(false));
    }

    public void testRefreshBlockRemovedAfterRestart() throws Exception {
        var masterNode = startMasterOnlyNode(
            Settings.builder()
                .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .put(useRefreshBlockSetting(true))
                .build()
        );
        startIndexNode();

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1)).setWaitForActiveShards(ActiveShardCount.NONE));
        ensureYellow(indexName);

        assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), is(true));

        internalCluster().restartNode(masterNode);
        ensureYellow(indexName);

        assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
    }

    public void testRefreshesAreBlockedUntilBlockIsRemoved() throws Exception {
        var useRefreshBlock = frequently();
        startMasterAndIndexNode(
            Settings.builder()
                .put(useRefreshBlockSetting(useRefreshBlock))
                // Ensure that all search shards can be recovered at the same time
                // (the max number of replica shards in the current test is 48 spread across multiple nodes,
                // but just to be on the safe side we allow up to 50 concurrent recoveries)
                .put("cluster.routing.allocation.node_concurrent_recoveries", 50)
                .put("cluster.routing.allocation.node_concurrent_incoming_recoveries", 50)
                .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 50)
                .build()
        );
        var maxNumberOfReplicas = randomIntBetween(2, 4);
        var searchNodes = startSearchNodes(maxNumberOfReplicas);

        var numberOfIndices = randomIntBetween(2, 4);
        var indices = new ArrayList<String>();
        var totalUnpromotableShardsPerIndex = new HashMap<String, Integer>();
        for (int i = 0; i < numberOfIndices; i++) {
            var indexName = randomIdentifier();
            var numberOfShards = randomIntBetween(1, 3);
            var numberOfReplicas = randomIntBetween(2, maxNumberOfReplicas);
            totalUnpromotableShardsPerIndex.put(indexName, numberOfShards * numberOfReplicas);
            assertAcked(
                prepareCreate(indexName).setSettings(
                    indexSettings(numberOfShards, numberOfReplicas).put(
                        "index.routing.allocation.exclude._name",
                        String.join(",", searchNodes)
                    )
                )
                    // Do not wait for any shards to be available as ONE in serverless waits for the search replica to be available
                    .setWaitForActiveShards(ActiveShardCount.NONE)
            );
            ensureYellowAndNoInitializingShards(indexName);
            indices.add(indexName);
        }

        if (randomBoolean()) {
            // This index will be blocked for refreshes for the entire test, but it shouldn't interfere with the bulk/refreshes from
            // the rest of the indices in this test.
            assertAcked(
                prepareCreate(randomIdentifier()).setSettings(
                    indexSettings(1, 2).put("index.routing.allocation.exclude._name", String.join(",", searchNodes))
                )
                    // Do not wait for any shards to be available as ONE in serverless waits for the search replica to be available
                    .setWaitForActiveShards(ActiveShardCount.NONE)
            );
        }

        var concurrentBulkFutures = new ArrayList<ActionFuture<BulkResponse>>();
        var concurrentBulkRequests = randomIntBetween(3, 5);
        for (int i = 0; i < concurrentBulkRequests; i++) {
            var bulkFuture = indexDocsWithRefreshPolicy(
                indices,
                randomIntBetween(1, 10),
                randomFrom(WriteRequest.RefreshPolicy.IMMEDIATE, WriteRequest.RefreshPolicy.WAIT_UNTIL)
            );
            concurrentBulkFutures.add(bulkFuture);
        }

        if (randomBoolean()) {
            var bulkWithRefreshPolicyNoneFuture = indexDocsWithRefreshPolicy(
                indices,
                randomIntBetween(1, 10),
                WriteRequest.RefreshPolicy.NONE
            );
            // Since the RefreshPolicy is NONE, the bulk should return immediately even if some of the search shards are still
            // unavailable
            assertNoFailures(safeGet(bulkWithRefreshPolicyNoneFuture));
            assertAllUnpromotableShardsAreInState(ShardRoutingState.UNASSIGNED);
        }

        var concurrentRefreshFutures = new ArrayList<ActionFuture<BroadcastResponse>>();
        var concurrentRefreshesCount = randomIntBetween(0, 10);
        for (int i = 0; i < concurrentRefreshesCount; i++) {
            concurrentRefreshFutures.add(indicesAdmin().prepareRefresh(indices.toArray(new String[] {})).execute());
        }

        // Let half of the indices proceed with the search shard allocation, but block some search shard recoveries
        List<String> indicesWithSearchShardsAllocated = randomSubsetOf(indices.size() / 2, indices);
        var numberOfRecoveringUnpromotableShards = indicesWithSearchShardsAllocated.stream()
            .mapToInt(totalUnpromotableShardsPerIndex::get)
            .sum();

        var delayRegisterCommitForRecovery = new AtomicBoolean(true);
        var registerCommitForRecoverySentLatch = new CountDownLatch(numberOfRecoveringUnpromotableShards);
        Queue<CheckedRunnable<Exception>> delayedCommitRegistrations = new ConcurrentLinkedQueue<>();
        for (String searchNode : searchNodes) {
            var transportService = MockTransportService.getInstance(searchNode);
            transportService.addSendBehavior((connection, requestId, action, request, options) -> {
                if (action.equals(TransportRegisterCommitForRecoveryAction.NAME) && delayRegisterCommitForRecovery.get()) {
                    delayedCommitRegistrations.add(() -> connection.sendRequest(requestId, action, request, options));
                    registerCommitForRecoverySentLatch.countDown();
                } else {
                    connection.sendRequest(requestId, action, request, options);
                }
            });
        }

        for (String indexName : indicesWithSearchShardsAllocated) {
            updateIndexSettings(Settings.builder().putNull("index.routing.allocation.exclude._name"), indexName);
        }

        safeAwait(registerCommitForRecoverySentLatch);
        // Allow a few search shards to be started
        var startedSearchShardCount = randomIntBetween(1, delayedCommitRegistrations.size() / 2);
        for (int i = 0; i < startedSearchShardCount; i++) {
            var pendingCommitRegistration = delayedCommitRegistrations.poll();
            assertThat(pendingCommitRegistration, is(notNullValue()));
            pendingCommitRegistration.run();
        }

        if (useRefreshBlock) {
            assertAllUnpromotableShardsAreInState(ShardRoutingState.UNASSIGNED, ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED);

            // TODO: Once ES-10278 is implemented, the index will be unblocked as soon as at least one replica for each shard has started,
            // we should assert that refreshes are unblocked as soon as that criteria is met.

            for (var bulkFuture : concurrentBulkFutures) {
                assertThat(bulkFuture.isDone(), is(false));
            }
            for (ActionFuture<BroadcastResponse> concurrentRefreshFuture : concurrentRefreshFutures) {
                assertThat(concurrentRefreshFuture.isDone(), is(false));
            }
            delayRegisterCommitForRecovery.set(false);

            CheckedRunnable<Exception> delayedCommitRegistration;
            while ((delayedCommitRegistration = delayedCommitRegistrations.poll()) != null) {
                delayedCommitRegistration.run();
            }

            for (String index : indices) {
                updateIndexSettings(Settings.builder().putNull("index.routing.allocation.exclude._name"), index);
            }

            ensureGreen(indices.toArray(new String[] {}));
            // TODO: Remove once ES-10278 is implemented
            indices.forEach(this::removeIndexRefreshBlock);
        }

        concurrentBulkFutures.forEach(bulkFuture -> assertNoFailures(safeGet(bulkFuture)));
        concurrentRefreshFutures.forEach(refreshFuture -> assertNoFailures(safeGet(refreshFuture)));
    }

    private void assertAllUnpromotableShardsAreInState(ShardRoutingState... shardRoutingStates) throws Exception {
        assertBusy(() -> {
            var state = client().admin().cluster().prepareState(TimeValue.timeValueSeconds(5)).get().getState();
            var states = state.routingTable()
                .allShards()
                .filter(indexShardRoutingTable -> indexShardRoutingTable.role().equals(ShardRouting.Role.SEARCH_ONLY))
                .map(ShardRouting::state)
                .collect(Collectors.toSet());

            assertThat(states, equalTo(Set.of(shardRoutingStates)));
        });
    }

    private ActionFuture<BulkResponse> indexDocsWithRefreshPolicy(
        List<String> indices,
        int docCountPerIndex,
        WriteRequest.RefreshPolicy refreshPolicy
    ) {
        var bulkRequest = client().prepareBulk();
        bulkRequest.setRefreshPolicy(refreshPolicy);
        for (String index : indices) {
            for (int i = 0; i < docCountPerIndex; i++) {
                bulkRequest.add(new IndexRequest(index).source("field", randomUnicodeOfCodepointLengthBetween(1, 25)));
            }
        }

        // In serverless the default ActiveShardCount waits for the search replica to be active before allowing any indexing,
        // hence the reason to use ActiveShardCount.NONE
        bulkRequest.setWaitForActiveShards(ActiveShardCount.NONE);
        return bulkRequest.execute();
    }

    private void removeIndexRefreshBlock(String indexName) {
        var latch = new CountDownLatch(1);
        var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        clusterService.submitUnbatchedStateUpdateTask("remove-index-refresh-block-test", new ClusterStateUpdateTask() {
            @Override
            public ClusterState execute(ClusterState currentState) {
                return ClusterState.builder(currentState)
                    .blocks(ClusterBlocks.builder(currentState.blocks()).removeIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK))
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }
        });
        safeAwait(latch);
    }

    private static ClusterBlocks clusterBlocks() {
        return client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(true).get().getState().blocks();
    }

    private static Settings useRefreshBlockSetting(boolean value) {
        return Settings.builder().put(Stateless.USE_INDEX_REFRESH_BLOCK_SETTING.getKey(), value).build();
    }
}
