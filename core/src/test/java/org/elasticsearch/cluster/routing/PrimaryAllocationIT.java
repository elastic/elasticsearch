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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.elasticsearch.common.collect.ImmutableOpenIntMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisconnectPartition;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@ESIntegTestCase.SuppressLocalMode
public class PrimaryAllocationIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // disruption tests need MockTransportService
        return pluginList(MockTransportService.TestPlugin.class);
    }

    private void createStaleReplicaScenario() throws Exception {
        logger.info("--> starting 3 nodes, 1 master, 2 data");
        String master = internalCluster().startMasterOnlyNode(Settings.EMPTY);
        internalCluster().startDataOnlyNodesAsync(2).get();

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

        NetworkDisconnectPartition partition = new NetworkDisconnectPartition(
            new HashSet<>(Arrays.asList(master, replicaNode)), Collections.singleton(primaryNode), random());
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
        assertBusy(new Runnable() { 
            @Override
            public void run() {
                assertThat(internalCluster().getInstance(GatewayAllocator.class, master).getNumberOfInFlightFetch(), equalTo(0));
            }
        });
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
        assertBusy(() -> assertTrue(client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").allPrimaryShardsUnassigned()));
        assertThat(client().admin().cluster().prepareState().get().getState().getRoutingTable().index("test").getShards().get(0).primaryShard().unassignedInfo().getReason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
    }

    public void testForceStaleReplicaToBePromotedToPrimary() throws Exception {
        boolean useStaleReplica = randomBoolean(); // if true, use stale replica, otherwise a completely empty copy
        createStaleReplicaScenario();

        logger.info("--> explicitly promote old primary shard");
        ImmutableOpenIntMap<List<IndicesShardStoresResponse.StoreStatus>> storeStatuses = client().admin().indices().prepareShardStores("test").get().getStoreStatuses().get("test");
        ClusterRerouteRequestBuilder rerouteBuilder = client().admin().cluster().prepareReroute();
        for (IntObjectCursor<List<IndicesShardStoresResponse.StoreStatus>> shardStoreStatuses : storeStatuses) {
            int shardId = shardStoreStatuses.key;
            IndicesShardStoresResponse.StoreStatus storeStatus = randomFrom(shardStoreStatuses.value);
            logger.info("--> adding allocation command for shard {}", shardId);
            // force allocation based on node id
            if (useStaleReplica) {
                rerouteBuilder.add(new AllocateStalePrimaryAllocationCommand("test", shardId, storeStatus.getNode().getId(), true));
            } else {
                rerouteBuilder.add(new AllocateEmptyPrimaryAllocationCommand("test", shardId, storeStatus.getNode().getId(), true));
            }
        }
        rerouteBuilder.get();

        logger.info("--> check that the stale primary shard gets allocated and that documents are available");
        ensureYellow("test");

        assertHitCount(client().prepareSearch("test").setSize(0).setQuery(matchAllQuery()).get(), useStaleReplica ? 1L : 0L);
    }

    public void testForcePrimaryShardIfAllocationDecidersSayNoAfterIndexCreation() throws ExecutionException, InterruptedException {
        String node = internalCluster().startNode();
        client().admin().indices().prepareCreate("test").setSettings(Settings.builder()
            .put("index.routing.allocation.exclude._name", node)
            .put("index.number_of_shards", 1).put("index.number_of_replicas", 0)).get();

        assertThat(client().admin().cluster().prepareState().get().getState().getRoutingTable().shardRoutingTable("test", 0).assignedShards(), empty());

        client().admin().cluster().prepareReroute().add(new AllocateEmptyPrimaryAllocationCommand("test", 0, node, true)).get();
        ensureGreen("test");
    }

    public void testNotWaitForQuorumCopies() throws Exception {
        logger.info("--> starting 3 nodes");
        internalCluster().startNodesAsync(3).get();
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
}
