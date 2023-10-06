/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class UpdateNumberOfReplicasTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(UpdateNumberOfReplicasTests.class);

    public void testUpdateNumberOfReplicas() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(initialRoutingTable.index("test").size(), equalTo(1));
        assertThat(initialRoutingTable.index("test").shard(0).size(), equalTo(2));
        assertThat(initialRoutingTable.index("test").shard(0).size(), equalTo(2));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shard(1).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).currentNodeId(), nullValue());
        assertThat(initialRoutingTable.index("test").shard(0).shard(1).currentNodeId(), nullValue());

        logger.info("Adding two nodes and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start all the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("Start all the replica shards");
        ClusterState newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        final String nodeHoldingPrimary = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        final String nodeHoldingReplica = clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId();
        assertThat(nodeHoldingPrimary, not(equalTo(nodeHoldingReplica)));
        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo(nodeHoldingReplica));

        logger.info("add another replica");
        final String[] indices = { "test" };
        RoutingTable updatedRoutingTable = RoutingTable.builder(
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
            clusterState.routingTable()
        ).updateNumberOfReplicas(2, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(2, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();

        assertThat(clusterState.metadata().index("test").getNumberOfReplicas(), equalTo(2));

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(), equalTo(nodeHoldingReplica));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(1).state(), equalTo(UNASSIGNED));

        logger.info("Add another node and start the added replica");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).size(), equalTo(1));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(0).currentNodeId(),
            equalTo(nodeHoldingReplica)
        );
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(INITIALIZING).get(0).currentNodeId(),
            equalTo("node3")
        );

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(3));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).size(), equalTo(2));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(0).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShardsWithState(STARTED).get(1).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );

        logger.info("now remove a replica");
        updatedRoutingTable = RoutingTable.builder(clusterState.routingTable()).updateNumberOfReplicas(1, indices).build();
        metadata = Metadata.builder(clusterState.metadata()).updateNumberOfReplicas(1, indices).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(metadata).build();

        assertThat(clusterState.metadata().index("test").getNumberOfReplicas(), equalTo(1));

        assertThat(clusterState.routingTable().index("test").size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).replicaShards().get(0).state(), equalTo(STARTED));
        assertThat(
            clusterState.routingTable().index("test").shard(0).replicaShards().get(0).currentNodeId(),
            anyOf(equalTo(nodeHoldingReplica), equalTo("node3"))
        );

        logger.info("do a reroute, should remain the same");
        newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, equalTo(clusterState));
    }
}
