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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayAllocator;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.disruption.NetworkDisconnectPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
@ESIntegTestCase.SuppressLocalMode
public class PrimaryAllocationIT extends ESIntegTestCase {

    public void testDoNotAllowStaleReplicasToBePromotedToPrimary() throws Exception {
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
            primaryNode = state.getRoutingNodes().node(shards.get(0).currentNodeId()).node().name();
            replicaNode = state.getRoutingNodes().node(shards.get(1).currentNodeId()).node().name();
        } else {
            primaryNode = state.getRoutingNodes().node(shards.get(1).currentNodeId()).node().name();
            replicaNode = state.getRoutingNodes().node(shards.get(0).currentNodeId()).node().name();
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
        assertBusy(() -> assertThat(internalCluster().getInstance(GatewayAllocator.class, master).getNumberOfInFlightFetch(), equalTo(0)));
        // kick reroute a second time and check that all shards are unassigned
        assertThat(client(master).admin().cluster().prepareReroute().get().getState().getRoutingNodes().unassigned().size(), equalTo(2));

        logger.info("--> starting node that reuses data folder with the up-to-date primary shard");
        internalCluster().startDataOnlyNode(Settings.EMPTY);

        logger.info("--> check that the up-to-date primary shard gets promoted and that documents are available");
        ensureYellow("test");
        assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 2l);
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
        assertHitCount(client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get(), 1l);
    }
}
