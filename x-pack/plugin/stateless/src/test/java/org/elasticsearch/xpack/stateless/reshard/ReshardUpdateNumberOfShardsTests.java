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

package co.elastic.elasticsearch.stateless.reshard;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ReshardUpdateNumberOfShardsTests extends ESAllocationTestCase {

    public void testIncrementNumberOfShards() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put("cluster.routing.allocation.node_concurrent_recoveries", 10).build()
        );

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(0))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(createCustomRoleStrategy(1))
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(initialRoutingTable.index("test").size(), equalTo(1));
        assertThat(initialRoutingTable.index("test").shard(0).size(), equalTo(1));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).state(), equalTo(UNASSIGNED));
        assertThat(initialRoutingTable.index("test").shard(0).shard(0).currentNodeId(), nullValue());

        logger.info("Adding one node and performing rerouting");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();

        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        logger.info("Start all the primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        final String nodeHoldingPrimary = clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId();
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));

        logger.info("Add another shard");
        final String index = "test";

        var reshardingMetadata = IndexReshardingMetadata.newSplitByMultiple(1, 2);
        final RoutingTable updatedRoutingTable = ReshardIndexService.addShardsToRoutingTable(
            RoutingTable.builder(createCustomRoleStrategy(2), clusterState.projectState().routingTable()),
            metadata.getProject().index(index).getIndex(),
            reshardingMetadata
        ).build();

        ProjectMetadata projectMetadata = ReshardIndexService.metadataUpdateNumberOfShards(
            clusterState.projectState(),
            reshardingMetadata,
            metadata.getProject().index(index).getIndex()
        ).build();
        clusterState = ClusterState.builder(clusterState)
            .putProjectMetadata(projectMetadata)
            .putRoutingTable(metadata.getProject().id(), updatedRoutingTable)
            .build();

        assertThat(clusterState.metadata().getProject().index("test").getNumberOfShards(), equalTo(2));

        assertThat(clusterState.routingTable().index("test").size(), equalTo(2));
        assertThat(clusterState.routingTable().index("test").shard(0).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().state(), equalTo(STARTED));
        assertThat(clusterState.routingTable().index("test").shard(0).primaryShard().currentNodeId(), equalTo(nodeHoldingPrimary));
        assertThat(clusterState.routingTable().index("test").shard(1).size(), equalTo(1));
        assertThat(clusterState.routingTable().index("test").shard(1).primaryShard().state(), equalTo(UNASSIGNED));

        ClusterState newState = strategy.reroute(clusterState, "reroute", ActionListener.noop());
        assertThat(newState, not(equalTo(clusterState)));
        clusterState = newState;

        assertThat(clusterState.metadata().getProject().index("test").getNumberOfShards(), equalTo(2));
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

        assertThat(clusterState.metadata().getProject().index("test").getNumberOfShards(), equalTo(2));
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
