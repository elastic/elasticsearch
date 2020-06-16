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

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

public class ShardVersioningTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ShardVersioningTests.class);

    public void testSimple() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(),
                ClusterRebalanceAllocationDecider.ClusterRebalanceType.ALWAYS.toString()).build());

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test1"))
                .addAsNew(metadata.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(routingTable).build();

        logger.info("start two nodes");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
            .add(newNode("node1")).add(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }

        logger.info("start all the primary shards for test1, replicas will start initializing");
        routingTable = startInitializingShardsAndReroute(strategy, clusterState, "test1").routingTable();

        for (int i = 0; i < routingTable.index("test1").shards().size(); i++) {
            assertThat(routingTable.index("test1").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test1").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(routingTable.index("test1").shard(i).replicaShards().get(0).state(), equalTo(INITIALIZING));
        }

        for (int i = 0; i < routingTable.index("test2").shards().size(); i++) {
            assertThat(routingTable.index("test2").shard(i).shards().size(), equalTo(2));
            assertThat(routingTable.index("test2").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(routingTable.index("test2").shard(i).replicaShards().get(0).state(), equalTo(UNASSIGNED));
        }
    }
}
