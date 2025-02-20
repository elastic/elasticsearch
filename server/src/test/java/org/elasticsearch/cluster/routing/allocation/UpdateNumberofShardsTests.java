/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class UpdateNumberofShardsTests extends ESAllocationTestCase {
    public void testUpdateNumberOfShards() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(createCustomRoleStrategy(1))
            .addAsNew(metadata.index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(initialRoutingTable.index("test").size(), equalTo(1));
        assertThat(initialRoutingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).currentNodeId(), nullValue());

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")))
            .build();

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start all the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        final String nodeHoldingPrimary = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));

        logger.info("Add another shard");
        final String index = "test";
        RoutingTable updatedRoutingTable = RoutingTable.builder(
            createCustomRoleStrategy(2),
            clusterState.routingTable()
        ).updateNumberOfShards(2, index).build();

        /*
        final IndexMetadata sourceIndexMetadata = clusterState.metadata().index(index);
        Settings.Builder settingsBuilder = Settings.builder().put(sourceIndexMetadata.getSettings());
        settingsBuilder.remove(IndexMetadata.SETTING_NUMBER_OF_SHARDS);
        settingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2);

        final Map<Index, Settings> updates = Maps.newHashMapWithExpectedSize(1);
        updates.put(sourceIndexMetadata.getIndex(), settingsBuilder.build());
        //final Metadata newMetadata = clusterState.metadata().withIndexSettingsUpdates(updates);
        */

        Metadata newMetadata = Metadata.builder(clusterState.metadata()).updateNumberOfShards(2, index).build();
        clusterState = ClusterState.builder(clusterState).routingTable(updatedRoutingTable).metadata(newMetadata).build();

        assertThat(clusterState.metadata().index("test").getNumberOfShards(), equalTo(2));

        assertThat(clusterState.routingTable().index("test").size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(1).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(1).primaryShard().state(), equalTo(UNASSIGNED));

        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.metadata().index("test").getNumberOfShards(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(1).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(1).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(1).primaryShard().state(), equalTo(INITIALIZING));

        newState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.metadata().index("test").getNumberOfShards(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(1).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(1).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(1).primaryShard().state(), equalTo(STARTED));
    }

    private static ShardRoutingRoleStrategy createCustomRoleStrategy(int indexShardCount) {
        return new ShardRoutingRoleStrategy() {
            @Override
            public ShardRouting.Role newEmptyRole(int copyIndex) {
                return copyIndex < indexShardCount ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY;
            }

            @Override
            public ShardRouting.Role newReplicaRole() {
                return ShardRouting.Role.SEARCH_ONLY;
            }
        };
    }
}
