/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresRequest;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.admin.indices.shards.TransportIndicesShardStoresAction;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrimaryAllocationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // disruption tests need MockTransportService
        return Arrays.asList(MockTransportService.TestPlugin.class, InternalSettingsPlugin.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        // testForceStaleReplicaToBePromotedToPrimary replies on the flushing when a shard is no longer assigned.
        return false;
    }

    public void testBulkWeirdScenario() throws Exception {
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(2);

        assertAcked(
            indicesAdmin().prepareCreate("test").setSettings(indexSettings(1, 1).put("index.global_checkpoint_sync.interval", "1s"))
        );
        ensureGreen();

        BulkResponse bulkResponse = client().prepareBulk()
            .add(prepareIndex("test").setId("1").setSource("field1", "value1"))
            .add(client().prepareUpdate().setIndex("test").setId("1").setDoc("field2", "value2"))
            .get();

        assertThat(bulkResponse.hasFailures(), equalTo(false));
        assertThat(bulkResponse.getItems().length, equalTo(2));

        logger.info(Strings.toString(bulkResponse, true, true));

        internalCluster().assertSeqNos();

        assertThat(bulkResponse.getItems()[0].getResponse().getId(), equalTo("1"));
        assertThat(bulkResponse.getItems()[0].getResponse().getVersion(), equalTo(1L));
        assertThat(bulkResponse.getItems()[0].getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(bulkResponse.getItems()[1].getResponse().getId(), equalTo("1"));
        assertThat(bulkResponse.getItems()[1].getResponse().getVersion(), equalTo(2L));
        assertThat(bulkResponse.getItems()[1].getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));
    }

    // returns data paths settings of in-sync shard copy
    private Settings createStaleReplicaScenario(String master) throws Exception {
        prepareIndex("test").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        refresh();
        ClusterState state = clusterAdmin().prepareState().all().get().getState();
        List<ShardRouting> shards = state.routingTable().allShards("test");
        assertThat(shards.size(), equalTo(2));

        final String primaryNode;
        final String replicaNode;
        if (shards.get(0).primary()) {
            primaryNode = state.getRoutingNodes().node(shards.get(0).currentNodeId()).node().getName();
            replicaNode = state.getRoutingNodes().node(shards.get(1).currentNodeId()).node().getName();
        } else {
            primaryNode = state.getRoutingNodes().node(shards.get(1).currentNodeId()).node().getName();
            replicaNode = state.getRoutingNodes().node(shards.get(0).currentNodeId()).node().getName();
        }

        NetworkDisruption partition = new NetworkDisruption(
            new TwoPartitions(Sets.newHashSet(master, replicaNode), Collections.singleton(primaryNode)),
            NetworkDisruption.DISCONNECT
        );
        internalCluster().setDisruptionScheme(partition);
        logger.info("--> partitioning node with primary shard from rest of cluster");
        partition.startDisrupting();

        ensureStableCluster(2, master);

        logger.info("--> index a document into previous replica shard (that is now primary)");
        client(replicaNode).prepareIndex("test").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();

        logger.info("--> shut down node that has new acknowledged document");
        final Settings inSyncDataPathSettings = internalCluster().dataPathSettings(replicaNode);
        internalCluster().stopNode(replicaNode);

        ensureStableCluster(1, master);

        partition.stopDisrupting();

        logger.info("--> waiting for node with old primary shard to rejoin the cluster");
        ensureStableCluster(2, master);

        logger.info("--> check that old primary shard does not get promoted to primary again");
        // kick reroute and wait for all shard states to be fetched
        client(master).admin().cluster().prepareReroute().get();
        assertBusy(
            () -> assertThat(internalCluster().getInstance(GatewayAllocator.class, master).getNumberOfInFlightFetches(), equalTo(0))
        );
        // kick reroute a second time and check that all shards are unassigned
        assertThat(client(master).admin().cluster().prepareReroute().get().getState().getRoutingNodes().unassigned().size(), equalTo(2));
        return inSyncDataPathSettings;
    }

    public void testDoNotAllowStaleReplicasToBePromotedToPrimary() throws Exception {
        logger.info("--> starting 3 nodes, 1 master, 2 data");
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(2);
        createIndex("test", 1, 1);
        ensureGreen();
        final Settings inSyncDataPathSettings = createStaleReplicaScenario(master);

        logger.info("--> starting node that reuses data folder with the up-to-date primary shard");
        internalCluster().startDataOnlyNode(inSyncDataPathSettings);

        logger.info("--> check that the up-to-date primary shard gets promoted and that documents are available");
        ensureYellow("test");
        assertHitCount(prepareSearch().setSize(0).setQuery(matchAllQuery()), 2L);
    }

    public void testFailedAllocationOfStalePrimaryToDataNodeWithNoData() throws Exception {
        String dataNodeWithShardCopy = internalCluster().startNode();

        logger.info("--> create single shard index");
        createIndex("test", 1, 0);
        ensureGreen("test");

        String dataNodeWithNoShardCopy = internalCluster().startNode();
        ensureStableCluster(2);

        internalCluster().stopNode(dataNodeWithShardCopy);
        ensureStableCluster(1);
        assertThat(
            clusterAdmin().prepareState()
                .get()
                .getState()
                .getRoutingTable()
                .index("test")
                .shard(0)
                .primaryShard()
                .unassignedInfo()
                .getReason(),
            equalTo(UnassignedInfo.Reason.NODE_LEFT)
        );

        logger.info("--> force allocation of stale copy to node that does not have shard copy");
        Throwable iae = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().prepareReroute().add(new AllocateStalePrimaryAllocationCommand("test", 0, dataNodeWithNoShardCopy, true))
        );
        assertThat(iae.getMessage(), equalTo("No data for shard [0] of index [test] found on any node"));

        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertTrue(
            clusterAdmin().prepareState().get().getState().toString(),
            clusterAdmin().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned()
        );
        assertThat(
            clusterAdmin().prepareState()
                .get()
                .getState()
                .getRoutingTable()
                .index("test")
                .shard(0)
                .primaryShard()
                .unassignedInfo()
                .getReason(),
            equalTo(UnassignedInfo.Reason.NODE_LEFT)
        );
    }

    public void testForceStaleReplicaToBePromotedToPrimary() throws Exception {
        logger.info("--> starting 3 nodes, 1 master, 2 data");
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(2);
        createIndex("test", 1, 1);
        ensureGreen();
        Set<String> historyUUIDs = Arrays.stream(indicesAdmin().prepareStats("test").clear().get().getShards())
            .map(shard -> shard.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY))
            .collect(Collectors.toSet());
        createStaleReplicaScenario(master);
        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose("test").setWaitForActiveShards(0));
        }
        boolean useStaleReplica = randomBoolean(); // if true, use stale replica, otherwise a completely empty copy
        logger.info("--> explicitly promote old primary shard");
        final String idxName = "test";
        Map<Integer, List<IndicesShardStoresResponse.StoreStatus>> storeStatuses = client().execute(
            TransportIndicesShardStoresAction.TYPE,
            new IndicesShardStoresRequest(idxName)
        ).get().getStoreStatuses().get(idxName);
        ClusterRerouteRequestBuilder rerouteBuilder = clusterAdmin().prepareReroute();
        for (Map.Entry<Integer, List<IndicesShardStoresResponse.StoreStatus>> shardStoreStatuses : storeStatuses.entrySet()) {
            int shardId = shardStoreStatuses.getKey();
            IndicesShardStoresResponse.StoreStatus storeStatus = randomFrom(shardStoreStatuses.getValue());
            logger.info("--> adding allocation command for shard {}", shardId);
            // force allocation based on node id
            if (useStaleReplica) {
                rerouteBuilder.add(new AllocateStalePrimaryAllocationCommand(idxName, shardId, storeStatus.getNode().getId(), true));
            } else {
                rerouteBuilder.add(new AllocateEmptyPrimaryAllocationCommand(idxName, shardId, storeStatus.getNode().getId(), true));
            }
        }

        final Set<String> expectedAllocationIds = useStaleReplica
            ? Collections.singleton(RecoverySource.ExistingStoreRecoverySource.FORCED_ALLOCATION_ID)
            : Collections.emptySet();

        final CountDownLatch clusterStateChangeLatch = new CountDownLatch(1);
        final ClusterStateListener clusterStateListener = event -> {
            final Set<String> allocationIds = event.state().metadata().index(idxName).inSyncAllocationIds(0);
            if (expectedAllocationIds.equals(allocationIds)) {
                clusterStateChangeLatch.countDown();
            }
            logger.info("expected allocation ids: {} actual allocation ids: {}", expectedAllocationIds, allocationIds);
        };
        final ClusterService clusterService = internalCluster().getInstance(ClusterService.class, master);
        clusterService.addListener(clusterStateListener);

        rerouteBuilder.get();

        assertTrue(clusterStateChangeLatch.await(30, TimeUnit.SECONDS));
        clusterService.removeListener(clusterStateListener);

        logger.info("--> check that the stale primary shard gets allocated and that documents are available");
        ensureYellow(idxName);

        if (useStaleReplica == false) {
            // When invoking AllocateEmptyPrimaryAllocationCommand, due to the UnassignedInfo.Reason being changed to INDEX_CREATION,
            // its possible that the shard has not completed initialization, even though the cluster health is yellow, so the
            // search can throw an "all shards failed" exception. We will wait until the shard initialization has completed before
            // verifying the search hit count.
            assertBusy(
                () -> assertTrue(clusterAdmin().prepareState().get().getState().routingTable().index(idxName).allPrimaryShardsActive())
            );
        }
        ShardStats[] shardStats = indicesAdmin().prepareStats("test")
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED)
            .get()
            .getShards();
        for (ShardStats shardStat : shardStats) {
            assertThat(shardStat.getCommitStats().getNumDocs(), equalTo(useStaleReplica ? 1 : 0));
        }
        // allocation id of old primary was cleaned from the in-sync set
        final ClusterState state = clusterAdmin().prepareState().get().getState();

        assertEquals(
            Collections.singleton(state.routingTable().index(idxName).shard(0).primary.allocationId().getId()),
            state.metadata().index(idxName).inSyncAllocationIds(0)
        );

        Set<String> newHistoryUUIds = Stream.of(shardStats)
            .map(shard -> shard.getCommitStats().getUserData().get(Engine.HISTORY_UUID_KEY))
            .collect(Collectors.toSet());
        assertThat(newHistoryUUIds, everyItem(is(not(in(historyUUIDs)))));
        assertThat(newHistoryUUIds, hasSize(1));
    }

    public void testForceStaleReplicaToBePromotedToPrimaryOnWrongNode() throws Exception {
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(2);
        final String idxName = "test";
        createIndex(idxName, 1, 1);
        ensureGreen();
        createStaleReplicaScenario(master);
        // Ensure the stopped primary's data is deleted so that it doesn't get picked up by the next datanode we start
        internalCluster().wipePendingDataDirectories();
        internalCluster().startDataOnlyNodes(1);
        ensureStableCluster(3, master);
        final int shardId = 0;
        final List<String> nodeNames = new ArrayList<>(Arrays.asList(internalCluster().getNodeNames()));
        nodeNames.remove(master);
        client().execute(TransportIndicesShardStoresAction.TYPE, new IndicesShardStoresRequest(idxName))
            .get()
            .getStoreStatuses()
            .get(idxName)
            .get(shardId)
            .forEach(status -> nodeNames.remove(status.getNode().getName()));
        assertThat(nodeNames, hasSize(1));
        final String nodeWithoutData = nodeNames.get(0);
        Throwable iae = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().prepareReroute().add(new AllocateStalePrimaryAllocationCommand(idxName, shardId, nodeWithoutData, true))
        );
        assertThat(
            iae.getMessage(),
            equalTo("No data for shard [" + shardId + "] of index [" + idxName + "] found on node [" + nodeWithoutData + ']')
        );
    }

    public void testForceStaleReplicaToBePromotedForGreenIndex() {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        final List<String> dataNodes = internalCluster().startDataOnlyNodes(2);
        final String idxName = "test";
        createIndex(idxName, 1, 1);
        ensureGreen();
        final String nodeWithoutData = randomFrom(dataNodes);
        final int shardId = 0;
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            clusterAdmin().prepareReroute().add(new AllocateStalePrimaryAllocationCommand(idxName, shardId, nodeWithoutData, true))
        );
        assertThat(iae.getMessage(), equalTo("[allocate_stale_primary] primary [" + idxName + "][" + shardId + "] is already assigned"));
    }

    public void testForceStaleReplicaToBePromotedForMissingIndex() {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        final String dataNode = internalCluster().startDataOnlyNode();
        final String idxName = "test";
        IndexNotFoundException ex = expectThrows(
            IndexNotFoundException.class,
            clusterAdmin().prepareReroute().add(new AllocateStalePrimaryAllocationCommand(idxName, 0, dataNode, true))
        );
        assertThat(ex.getIndex().getName(), equalTo(idxName));
    }

    public void testForcePrimaryShardIfAllocationDecidersSayNoAfterIndexCreation() throws ExecutionException, InterruptedException {
        String node = internalCluster().startNode();
        indicesAdmin().prepareCreate("test")
            .setWaitForActiveShards(ActiveShardCount.NONE)
            .setSettings(indexSettings(1, 0).put("index.routing.allocation.exclude._name", node))
            .get();

        assertThat(clusterAdmin().prepareState().get().getState().getRoutingTable().shardRoutingTable("test", 0).assignedShards(), empty());

        clusterAdmin().prepareReroute().add(new AllocateEmptyPrimaryAllocationCommand("test", 0, node, true)).get();
        ensureGreen("test");
    }

    public void testDoNotRemoveAllocationIdOnNodeLeave() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        assertAcked(
            indicesAdmin().prepareCreate("test").setSettings(indexSettings(1, 1).put("index.unassigned.node_left.delayed_timeout", "0ms"))
        );
        String replicaNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        ensureGreen("test");
        final Settings inSyncDataPathSettings = internalCluster().dataPathSettings(replicaNode);
        internalCluster().stopNode(replicaNode);
        ensureYellow("test");
        assertEquals(2, clusterAdmin().prepareState().get().getState().metadata().index("test").inSyncAllocationIds(0).size());
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertBusy(
            () -> assertTrue(clusterAdmin().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned())
        );
        assertEquals(2, clusterAdmin().prepareState().get().getState().metadata().index("test").inSyncAllocationIds(0).size());

        logger.info("--> starting node that reuses data folder with the up-to-date shard");
        internalCluster().startDataOnlyNode(inSyncDataPathSettings);
        ensureGreen("test");
    }

    public void testRemoveAllocationIdOnWriteAfterNodeLeave() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        assertAcked(
            indicesAdmin().prepareCreate("test").setSettings(indexSettings(1, 1).put("index.unassigned.node_left.delayed_timeout", "0ms"))
        );
        String replicaNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        ensureGreen("test");
        final Settings inSyncDataPathSettings = internalCluster().dataPathSettings(replicaNode);
        internalCluster().stopNode(replicaNode);
        ensureYellow("test");
        assertEquals(2, clusterAdmin().prepareState().get().getState().metadata().index("test").inSyncAllocationIds(0).size());
        logger.info("--> indexing...");
        prepareIndex("test").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        assertEquals(1, clusterAdmin().prepareState().get().getState().metadata().index("test").inSyncAllocationIds(0).size());
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertBusy(
            () -> assertTrue(clusterAdmin().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned())
        );
        assertEquals(1, clusterAdmin().prepareState().get().getState().metadata().index("test").inSyncAllocationIds(0).size());

        logger.info("--> starting node that reuses data folder with the up-to-date shard");
        internalCluster().startDataOnlyNode(inSyncDataPathSettings);
        assertBusy(
            () -> assertTrue(clusterAdmin().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned())
        );
    }

    public void testNotWaitForQuorumCopies() throws Exception {
        logger.info("--> starting 3 nodes");
        List<String> nodes = internalCluster().startNodes(3);
        logger.info("--> creating index with 1 primary and 2 replicas");
        createIndex("test", randomIntBetween(1, 3), 2);
        ensureGreen("test");
        prepareIndex("test").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        logger.info("--> removing 2 nodes from cluster");
        internalCluster().stopNode(nodes.get(1));
        internalCluster().stopNode(nodes.get(2));
        internalCluster().restartRandomDataNode();
        logger.info("--> checking that index still gets allocated with only 1 shard copy being available");
        ensureYellow("test");
        assertHitCount(prepareSearch().setSize(0).setQuery(matchAllQuery()), 1L);
    }

    /**
     * This test ensures that for an unassigned primary shard that has a valid shard copy on at least one node,
     * we will force allocate the primary shard to one of those nodes, even if the allocation deciders all return
     * a NO decision to allocate.
     */
    public void testForceAllocatePrimaryOnNoDecision() throws Exception {
        logger.info("--> starting 1 node");
        final String node = internalCluster().startNode();
        logger.info("--> creating index with 1 primary and 0 replicas");
        final String indexName = "test-idx";
        createIndex(indexName, 1, 0);
        logger.info("--> update the settings to prevent allocation to the data node");
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", node), indexName);
        logger.info("--> full cluster restart");
        internalCluster().fullRestart();
        logger.info("--> checking that the primary shard is force allocated to the data node despite being blocked by the exclude filter");
        ensureGreen(indexName);
        assertEquals(
            1,
            clusterAdmin().prepareState().get().getState().routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size()
        );
    }

    /**
     * This test asserts that replicas failed to execute resync operations will be failed but not marked as stale.
     */
    public void testPrimaryReplicaResyncFailed() throws Exception {
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        final int numberOfReplicas = between(2, 3);
        final String oldPrimary = internalCluster().startDataOnlyNode();
        assertAcked(prepareCreate("test", indexSettings(1, numberOfReplicas)));
        final ShardId shardId = new ShardId(clusterService().state().metadata().index("test").getIndex(), 0);
        final Set<String> replicaNodes = new HashSet<>(internalCluster().startDataOnlyNodes(numberOfReplicas));
        ensureGreen();
        String timeout = randomFrom("0s", "1s", "2s");
        updateClusterSettings(
            Settings.builder().put("indices.replication.retry_timeout", timeout).put("cluster.routing.allocation.enable", "none")
        );
        logger.info("--> Indexing with gap in seqno to ensure that some operations will be replayed in resync");
        long numDocs = scaledRandomIntBetween(5, 50);
        for (int i = 0; i < numDocs; i++) {
            DocWriteResponse indexResult = indexDoc("test", Long.toString(i));
            assertThat(indexResult.getShardInfo().getSuccessful(), equalTo(numberOfReplicas + 1));
        }
        final IndexShard oldPrimaryShard = internalCluster().getInstance(IndicesService.class, oldPrimary).getShardOrNull(shardId);
        EngineTestCase.generateNewSeqNo(IndexShardTestCase.getEngine(oldPrimaryShard)); // Make gap in seqno.
        long moreDocs = scaledRandomIntBetween(1, 10);
        for (int i = 0; i < moreDocs; i++) {
            DocWriteResponse indexResult = indexDoc("test", Long.toString(numDocs + i));
            assertThat(indexResult.getShardInfo().getSuccessful(), equalTo(numberOfReplicas + 1));
        }
        final Set<String> replicasSide1 = Sets.newHashSet(randomSubsetOf(between(1, numberOfReplicas - 1), replicaNodes));
        final Set<String> replicasSide2 = Sets.difference(replicaNodes, replicasSide1);
        NetworkDisruption partition = new NetworkDisruption(new TwoPartitions(replicasSide1, replicasSide2), NetworkDisruption.DISCONNECT);
        internalCluster().setDisruptionScheme(partition);
        logger.info("--> isolating some replicas during primary-replica resync");
        partition.startDisrupting();
        internalCluster().stopNode(oldPrimary);
        // Checks that we fails replicas in one side but not mark them as stale.
        assertBusy(() -> {
            ClusterState state = client(master).admin().cluster().prepareState().get().getState();
            final IndexShardRoutingTable shardRoutingTable = state.routingTable().shardRoutingTable(shardId);
            final String newPrimaryNode = state.getRoutingNodes().node(shardRoutingTable.primary.currentNodeId()).node().getName();
            assertThat(newPrimaryNode, not(equalTo(oldPrimary)));
            Set<String> selectedPartition = replicasSide1.contains(newPrimaryNode) ? replicasSide1 : replicasSide2;
            assertThat(shardRoutingTable.activeShards(), hasSize(selectedPartition.size()));
            for (ShardRouting activeShard : shardRoutingTable.activeShards()) {
                assertThat(state.getRoutingNodes().node(activeShard.currentNodeId()).node().getName(), is(in(selectedPartition)));
            }
            assertThat(state.metadata().index("test").inSyncAllocationIds(shardId.id()), hasSize(numberOfReplicas + 1));
        }, 1, TimeUnit.MINUTES);
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.enable", "all"));
        partition.stopDisrupting();
        partition.ensureHealthy(internalCluster());
        logger.info("--> stop disrupting network and re-enable allocation");
        assertBusy(() -> {
            ClusterState state = client(master).admin().cluster().prepareState().get().getState();
            assertThat(state.routingTable().shardRoutingTable(shardId).activeShards(), hasSize(numberOfReplicas));
            assertThat(state.metadata().index("test").inSyncAllocationIds(shardId.id()), hasSize(numberOfReplicas + 1));
            for (String node : replicaNodes) {
                IndexShard shard = internalCluster().getInstance(IndicesService.class, node).getShardOrNull(shardId);
                assertThat(shard.getLocalCheckpoint(), equalTo(numDocs + moreDocs));
            }
        }, 30, TimeUnit.SECONDS);
        internalCluster().assertConsistentHistoryBetweenTranslogAndLuceneIndex();
    }

}
