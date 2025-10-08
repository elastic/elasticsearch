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
import org.elasticsearch.action.admin.indices.readonly.AddIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.RemoveIndexBlockRequest;
import org.elasticsearch.action.admin.indices.readonly.TransportAddIndexBlockAction;
import org.elasticsearch.action.admin.indices.readonly.TransportRemoveIndexBlockAction;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class BlockRefreshUponIndexCreationIT extends AbstractStatelessIntegTestCase {

    public void testIndexCreatedWithRefreshBlock() throws Exception {
        startMasterAndIndexNode(useRefreshBlockSetting(true));

        int nbReplicas = randomIntBetween(0, 3);

        var indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, nbReplicas)).setWaitForActiveShards(ActiveShardCount.NONE));
        ensureYellowAndNoInitializingShards(indexName);

        var blocks = clusterBlocks();
        assertThat(blocks.hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(0 < nbReplicas));

        if (0 < nbReplicas) {
            startSearchNodes(nbReplicas);
            ensureGreen(indexName);
            assertBusy(() -> assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(false)));
        }
    }

    public void testRefreshBlockIsRemovedOnceTheIndexIsSearchable() throws Exception {
        var nodeSettings = Settings.builder()
            .put(useRefreshBlockSetting(true))
            .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
            .put("cluster.routing.allocation.node_concurrent_incoming_recoveries", 3)
            .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 3)
            .build();
        startMasterAndIndexNode(nodeSettings);
        var searchNode = startSearchNode(nodeSettings);
        int nbShards = randomIntBetween(2, 3);
        // Even though we cannot allocate all the replicas, we'll be able to accommodate at least 1 replica per shard
        // in the searchNode. Thus, the index will be searchable and therefore the block should be removed.
        int nbReplicas = randomIntBetween(2, 3);

        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(nbShards, nbReplicas).put("index.routing.allocation.exclude._name", searchNode)
            ).setWaitForActiveShards(ActiveShardCount.NONE)
        );
        ensureYellowAndNoInitializingShards(indexName);

        assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(true));

        Queue<CheckedRunnable<Exception>> delayedCommitRegistrations = new ConcurrentLinkedQueue<>();

        var transportService = MockTransportService.getInstance(searchNode);
        var registerCommitForRecoverySentLatch = new CountDownLatch(nbShards);
        transportService.addSendBehavior((connection, requestId, action, request, options) -> {
            if (action.equals(TransportRegisterCommitForRecoveryAction.NAME)) {
                delayedCommitRegistrations.add(() -> connection.sendRequest(requestId, action, request, options));
                registerCommitForRecoverySentLatch.countDown();
            } else {
                connection.sendRequest(requestId, action, request, options);
            }
        });

        updateIndexSettings(Settings.builder().putNull("index.routing.allocation.exclude._name"), indexName);

        safeAwait(registerCommitForRecoverySentLatch);

        // Let all search shards but one to be started
        for (int i = 0; i < (nbShards - 1); i++) {
            var delayedCommitRegistration = delayedCommitRegistrations.poll();
            assertThat(delayedCommitRegistration, notNullValue());
            delayedCommitRegistration.run();
        }

        assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(true));

        // Let the latest search shard to be started
        var delayedCommitRegistration = delayedCommitRegistrations.poll();
        assertThat(delayedCommitRegistration, notNullValue());
        delayedCommitRegistration.run();

        ensureYellowAndNoInitializingShards(indexName);
        assertBusy(() -> assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(false)));

        assertResponse(client().prepareSearch(indexName).setQuery(new MatchAllQueryBuilder()), ElasticsearchAssertions::assertNoFailures);

        // Once the refresh block is removed it won't be added again even if the index becomes unavailable for searches
        internalCluster().stopNode(searchNode);
        assertThat(clusterBlocks().hasIndexBlock(indexName, IndexMetadata.INDEX_REFRESH_BLOCK), equalTo(false));
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

        assertAllUnpromotableShardsAreInState(ShardRoutingState.UNASSIGNED, ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED);

        if (useRefreshBlock) {
            // If we don't use refresh blocks, the bulks might be waiting for a refresh to be executed in the assigned unpromotable nodes,
            // but if the refresh policy was IMMEDIATE, it might have finished already. Therefore, we only assert that the bulks are not
            // done when we use the refresh block.
            for (var bulkFuture : concurrentBulkFutures) {
                if (bulkFuture.isDone()) {
                    // Just to debug CI failures
                    assertNoFailures(safeGet(bulkFuture));
                }
                assertThat(bulkFuture.isDone(), is(false));
            }

            for (ActionFuture<BroadcastResponse> concurrentRefreshFuture : concurrentRefreshFutures) {
                if (concurrentRefreshFuture.isDone()) {
                    // Just to debug CI failures
                    assertNoFailures(safeGet(concurrentRefreshFuture));
                }
                assertThat(concurrentRefreshFuture.isDone(), is(false));
            }
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

    public void testRefreshBlockRemovedAfterDelay() throws Exception {
        var nodeSettings = Settings.builder()
            .put(StoreHeartbeatService.MAX_MISSED_HEARTBEATS.getKey(), 1)
            .put(StoreHeartbeatService.HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
            .put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueHours(1L))
            .put(useRefreshBlockSetting(true))
            .build();

        String masterNode = startMasterAndIndexNode(nodeSettings);

        final int nbBlockedIndices = randomIntBetween(1, 4);
        final Set<Index> blockedIndices = new HashSet<>(nbBlockedIndices);
        for (int i = 0; i < nbBlockedIndices; i++) {
            var indexName = "index-" + i;
            assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1)).setWaitForActiveShards(ActiveShardCount.NONE));
            blockedIndices.add(resolveIndex(indexName));
        }
        ensureYellow("index-*");

        blockedIndices.forEach(
            index -> assertThat(clusterBlocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK), is(true))
        );

        if (randomBoolean()) {
            startMasterAndIndexNode(nodeSettings);
            ensureStableCluster(2);

            internalCluster().stopNode(masterNode);
            ensureYellow("index-*");

            blockedIndices.forEach(
                index -> assertThat(clusterBlocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK), is(true))
            );
        }

        assertBusy(() -> {
            var removeRefreshClusterBlockService = internalCluster().getCurrentMasterNodeInstance(RemoveRefreshClusterBlockService.class);
            assertThat(removeRefreshClusterBlockService.blockedIndices(), equalTo(blockedIndices));
        });

        updateClusterSettings(
            Settings.builder().put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueSeconds(1L))
        );

        assertBusy(() -> {
            var removeRefreshClusterBlockService = internalCluster().getCurrentMasterNodeInstance(RemoveRefreshClusterBlockService.class);
            assertThat(removeRefreshClusterBlockService.blockedIndices(), hasSize(0));
        });

        blockedIndices.forEach(
            index -> assertThat(clusterBlocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK), is(false))
        );
    }

    public void testRefreshBlockRemovedAfterReplicasUpdate() {
        startMasterAndIndexNode(
            Settings.builder()
                .put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put(useRefreshBlockSetting(true))
                .build()
        );

        final int nbBlockedIndices = randomIntBetween(2, 5);
        final Set<Index> blockedIndices = new HashSet<>(nbBlockedIndices);
        for (int i = 0; i < nbBlockedIndices; i++) {
            var indexName = "index-" + i;
            assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 1)).setWaitForActiveShards(ActiveShardCount.NONE));
            blockedIndices.add(resolveIndex(indexName));
        }
        ensureYellow("index-*");

        blockedIndices.forEach(
            index -> assertThat(clusterBlocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK), is(true))
        );

        var blockedIndicesNoReplicas = randomSubsetOf(randomIntBetween(1, blockedIndices.size()), blockedIndices).stream()
            .map(Index::getName)
            .toArray(String[]::new);
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0), blockedIndicesNoReplicas);
        ensureGreen(blockedIndicesNoReplicas);

        for (var blockedIndexNoReplicas : blockedIndicesNoReplicas) {
            assertThat(clusterBlocks().hasIndexBlock(blockedIndexNoReplicas, IndexMetadata.INDEX_REFRESH_BLOCK), is(false));
        }

        updateIndexSettings(
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0),
            blockedIndices.stream().map(Index::getName).toArray(String[]::new)
        );
        ensureGreen("index-*");

        blockedIndices.forEach(
            index -> assertThat(clusterBlocks().hasIndexBlock(index.getName(), IndexMetadata.INDEX_REFRESH_BLOCK), is(false))
        );
    }

    public void testUnrelatedClusterBlocksAreNotTakenIntoAccount() {
        startMasterAndIndexNode(
            Settings.builder()
                .put(RemoveRefreshClusterBlockService.EXPIRE_AFTER_SETTING.getKey(), TimeValue.timeValueHours(1L))
                .put(useRefreshBlockSetting(true))
                .build()
        );

        String indexName = randomIdentifier();
        assertAcked(prepareCreate(indexName).setSettings(indexSettings(1, 0)));
        ensureGreen(indexName);

        // The cluster state listener that's in charge of removing refresh blocks should only care about REFRESH blocks.
        // This test just ensures that we can add random blocks and it still works.
        var block = randomValueOtherThan(IndexMetadata.APIBlock.READ_ONLY_ALLOW_DELETE, () -> randomFrom(IndexMetadata.APIBlock.values()));
        assertAcked(client().execute(TransportAddIndexBlockAction.TYPE, new AddIndexBlockRequest(block, indexName)));

        assertAcked(
            client().execute(
                TransportRemoveIndexBlockAction.TYPE,
                new RemoveIndexBlockRequest(
                    AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                    AcknowledgedRequest.DEFAULT_ACK_TIMEOUT,
                    block,
                    indexName
                )
            )
        );
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

    private static ClusterBlocks clusterBlocks() {
        return client().admin().cluster().prepareState(TEST_REQUEST_TIMEOUT).clear().setBlocks(true).get().getState().blocks();
    }

    private static Settings useRefreshBlockSetting(boolean value) {
        return Settings.builder().put(Stateless.USE_INDEX_REFRESH_BLOCK_SETTING.getKey(), value).build();
    }
}
