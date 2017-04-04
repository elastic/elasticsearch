package org.elasticsearch.cluster.routing;

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequestBuilder;
import org.elasticsearch.action.admin.indices.shards.IndicesShardStoresResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.discovery.TestZenDiscovery;
import org.elasticsearch.test.disruption.NetworkDisruption;
import org.elasticsearch.test.disruption.NetworkDisruption.NetworkDisconnect;
import org.elasticsearch.test.disruption.NetworkDisruption.TwoPartitions;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class PrimaryAllocationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // disruption tests need MockTransportService
        return Arrays.asList(MockTransportService.TestPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(TestZenDiscovery.USE_MOCK_PINGS.getKey(), false).build();
    }

    private void createStaleReplicaScenario() throws Exception {
        logger.info("--> starting 3 nodes, 1 master, 2 data");
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodes(2);

        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", 1).put("index.number_of_replicas", 1)).get());
        ensureGreen();
        logger.info("--> indexing...");
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        refresh();

        ClusterState state = client().admin().cluster().prepareState().all().get().getState();
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
            new NetworkDisconnect());
        internalCluster().setDisruptionScheme(partition);
        logger.info("--> partitioning node with primary shard from rest of cluster");
        partition.startDisrupting();

        ensureStableCluster(2, master);

        logger.info("--> index a document into previous replica shard (that is now primary)");
        client(replicaNode).prepareIndex("test", "type1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();

        logger.info("--> shut down node that has new acknowledged document");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));

        ensureStableCluster(1, master);

        partition.stopDisrupting();

        logger.info("--> waiting for node with old primary shard to rejoin the cluster");
        ensureStableCluster(2, master);

        logger.info("--> check that old primary shard does not get promoted to primary again");
        // kick reroute and wait for all shard states to be fetched
        client(master).admin().cluster().prepareReroute().get();
        assertBusy(() -> assertThat(internalCluster().getInstance(GatewayAllocator.class, master).getNumberOfInFlightFetch(), equalTo(0)));
        // kick reroute a second time and check that all shards are unassigned
        assertThat(client(master).admin().cluster().prepareReroute().get().getState().getRoutingNodes().unassigned().size(), equalTo(2));
    }

    public void testDoNotAllowStaleReplicasToBePromotedToPrimary() throws Exception {
        createStaleReplicaScenario();

        logger.info("--> starting node that reuses data folder with the up-to-date primary shard");
        internalCluster().startDataOnlyNode(Settings.EMPTY);

        logger.info("--> check that the up-to-date primary shard gets promoted and that documents are available");
        ensureYellow("test");
        assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2L);
    }

    public void testFailedAllocationOfStalePrimaryToDataNodeWithNoData() throws Exception {
        String dataNodeWithShardCopy = internalCluster().startNode();

        logger.info("--> create single shard index");
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get());
        ensureGreen("test");

        String dataNodeWithNoShardCopy = internalCluster().startNode();
        ensureStableCluster(2);

        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(dataNodeWithShardCopy));
        ensureStableCluster(1);
        assertThat(client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").getShards().get(0).primaryShard().unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.NODE_LEFT));

        logger.info("--> force allocation of stale copy to node that does not have shard copy");
        client().admin().cluster().prepareReroute().add(new AllocateStalePrimaryAllocationCommand("test", 0, dataNodeWithNoShardCopy, true)).get();

        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertBusy(() ->
            assertTrue(client().admin().cluster().prepareState().get().getState().toString(),
                client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned()));
        assertThat(client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").getShards().get(0).primaryShard().unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
    }

    public void testForceStaleReplicaToBePromotedToPrimary() throws Exception {
        boolean useStaleReplica = randomBoolean(); // if true, use stale replica, otherwise a completely empty copy
        createStaleReplicaScenario();

        logger.info("--> explicitly promote old primary shard");
        final String idxName = "test";
        ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> storeStatuses = client().admin().indices().prepareShardStores(idxName).get().getStoreStatuses().get(idxName);
        ClusterRerouteRequestBuilder rerouteBuilder = client().admin().cluster().prepareReroute();
        for (IntObjectCursor<List<IndicesShardStoresResponse.StoreStatus>> shardStoreStatuses : storeStatuses) {
            int shardId = shardStoreStatuses.key;
            IndicesShardStoresResponse.StoreStatus storeStatus = randomFrom(shardStoreStatuses.value);
            logger.info("--> adding allocation command for shard {}", shardId);
            // force allocation based on node id
            if (useStaleReplica) {
                rerouteBuilder.add(new AllocateStalePrimaryAllocationCommand(idxName, shardId, storeStatus.getNode().getId(), true));
            } else {
                rerouteBuilder.add(new AllocateEmptyPrimaryAllocationCommand(idxName, shardId, storeStatus.getNode().getId(), true));
            }
        }
        rerouteBuilder.get();

        logger.info("--> check that the stale primary shard gets allocated and that documents are available");
        ensureYellow(idxName);

        if (useStaleReplica == false) {
            // When invoking AllocateEmptyPrimaryAllocationCommand, due to the UnassignedInfo.Reason being changed to INDEX_CREATION,
            // its possible that the shard has not completed initialization, even though the cluster health is yellow, so the
            // search can throw an "all shards failed" exception.  We will wait until the shard initialization has completed before
            // verifying the search hit count.
            assertBusy(() -> assertTrue(client().admin().cluster().prepareState().get()
                                            .getState().routingTable().index(idxName).allPrimaryShardsActive()));
        }
        assertHitCount(client().prepareSearch(idxName).setSize(0).setQuery(matchAllQuery()).get(), useStaleReplica ? 1L : 0L);

        // allocation id of old primary was cleaned from the in-sync set
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        assertEquals(Collections.singleton(state.routingTable().index(idxName).shard(0).primary.allocationId().getId()),
            state.metaData().index(idxName).inSyncAllocationIds(0));
    }

    public void testForcePrimaryShardIfAllocationDecidersSayNoAfterIndexCreation() throws ExecutionException, InterruptedException {
        String node = internalCluster().startNode();
        client().admin().indices().prepareCreate("test").setWaitForActiveShards(ActiveShardCount.NONE).setSettings(Settings.builder()
            .put("index.routing.allocation.exclude._name", node)
            .put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get();

        assertThat(client().admin().cluster().prepareState().get().getState().getRoutingTable().shardRoutingTable("test", 0).assignedShards(), empty());

        client().admin().cluster().prepareReroute().add(new AllocateEmptyPrimaryAllocationCommand("test", 0, node, true)).get();
        ensureGreen("test");
    }

    public void testDoNotRemoveAllocationIdOnNodeLeave() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", 1).put("index.number_of_replicas", 1).put("index.unassigned.node_left.delayed_timeout", "0ms")).get());
        String replicaNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        ensureGreen("test");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));
        ensureYellow("test");
        assertEquals(2, client().admin().cluster().prepareState().get().getState().metaData().index("test").inSyncAllocationIds(0).size());
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertBusy(() -> assertTrue(client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned()));
        assertEquals(2, client().admin().cluster().prepareState().get().getState().metaData().index("test").inSyncAllocationIds(0).size());

        logger.info("--> starting node that reuses data folder with the up-to-date shard");
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        ensureGreen("test");
    }

    public void testRemoveAllocationIdOnWriteAfterNodeLeave() throws Exception {
        internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", 1).put("index.number_of_replicas", 1).put("index.unassigned.node_left.delayed_timeout", "0ms")).get());
        String replicaNode = internalCluster().startDataOnlyNode(Settings.EMPTY);
        ensureGreen("test");
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(replicaNode));
        ensureYellow("test");
        assertEquals(2, client().admin().cluster().prepareState().get().getState().metaData().index("test").inSyncAllocationIds(0).size());
        logger.info("--> indexing...");
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        assertEquals(1, client().admin().cluster().prepareState().get().getState().metaData().index("test").inSyncAllocationIds(0).size());
        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public boolean clearData(String nodeName) {
                return true;
            }
        });
        logger.info("--> wait until shard is failed and becomes unassigned again");
        assertBusy(() -> assertTrue(client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned()));
        assertEquals(1, client().admin().cluster().prepareState().get().getState().metaData().index("test").inSyncAllocationIds(0).size());

        logger.info("--> starting node that reuses data folder with the up-to-date shard");
        internalCluster().startDataOnlyNode(Settings.EMPTY);
        assertBusy(() -> assertTrue(client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned()));
    }

    public void testNotWaitForQuorumCopies() throws Exception {
        logger.info("--> starting 3 nodes");
        internalCluster().startNodes(3);
        logger.info("--> creating index with 1 primary and 2 replicas");
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put("index.number_of_shards", randomIntBetween(1, 3)).put("index.number_of_replicas", 2)).get());
        ensureGreen("test");
        client().prepareIndex("test", "type1").setSource(jsonBuilder().startObject().field("field", "value1").endObject()).get();
        logger.info("--> removing 2 nodes from cluster");
        internalCluster().stopRandomDataNode();
        internalCluster().stopRandomDataNode();
        internalCluster().fullRestart();
        logger.info("--> checking that index still gets allocated with only 1 shard copy being available");
        ensureYellow("test");
        assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 1L);
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
        assertAcked(client().admin().indices()
                        .prepareCreate(indexName)
                        .setSettings(Settings.builder().put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                                         .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 0))
                        .get());
        logger.info("--> update the settings to prevent allocation to the data node");
        assertTrue(client().admin().indices().prepareUpdateSettings(indexName)
                       .setSettings(Settings.builder().put(IndexMetaData.INDEX_ROUTING_EXCLUDE_GROUP_SETTING.getKey() + "_name", node))
                       .get()
                       .isAcknowledged());
        logger.info("--> full cluster restart");
        internalCluster().fullRestart();
        logger.info("--> checking that the primary shard is force allocated to the data node despite being blocked by the exclude filter");
        ensureGreen(indexName);
        assertEquals(1, client().admin().cluster().prepareState().get().getState()
                            .routingTable().index(indexName).shardsWithState(ShardRoutingState.STARTED).size());
    }
}
