package org.elasticsearch.test.unit.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.junit.Test;

import static org.elasticsearch.cluster.ClusterState.newClusterStateBuilder;
import static org.elasticsearch.cluster.metadata.IndexMetaData.newIndexMetaDataBuilder;
import static org.elasticsearch.cluster.metadata.MetaData.newMetaDataBuilder;
import static org.elasticsearch.cluster.node.DiscoveryNodes.newNodesBuilder;
import static org.elasticsearch.cluster.routing.RoutingBuilders.routingTable;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.unit.cluster.routing.allocation.RoutingAllocationTests.newNode;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class SameShardRoutingTests {

    private final ESLogger logger = Loggers.getLogger(SameShardRoutingTests.class);

    @Test
    public void sameHost() {
        AllocationService strategy = new AllocationService(settingsBuilder().put(SameShardAllocationDecider.SAME_HOST_SETTING, true).build());

        MetaData metaData = newMetaDataBuilder()
                .put(newIndexMetaDataBuilder("test").numberOfShards(2).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = routingTable()
                .addAsNew(metaData.index("test"))
                .build();
        ClusterState clusterState = newClusterStateBuilder().metaData(metaData).routingTable(routingTable).build();

        logger.info("--> adding two nodes with the same host");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .put(newNode("node1", new InetSocketTransportAddress("test1", 80)))
                .put(newNode("node2", new InetSocketTransportAddress("test1", 80)))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.readOnlyRoutingNodes().numberOfShardsOfType(ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("--> start all primary shards, no replica will be started since its on the same host");
        routingTable = strategy.applyStartedShards(clusterState, clusterState.readOnlyRoutingNodes().shardsWithState(INITIALIZING)).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.readOnlyRoutingNodes().numberOfShardsOfType(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().numberOfShardsOfType(ShardRoutingState.INITIALIZING), equalTo(0));

        logger.info("--> add another node, with a different host, replicas will be allocating");
        clusterState = newClusterStateBuilder().state(clusterState).nodes(newNodesBuilder()
                .putAll(clusterState.nodes())
                .put(newNode("node3", new InetSocketTransportAddress("test2", 80)))).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = newClusterStateBuilder().state(clusterState).routingTable(routingTable).build();

        assertThat(clusterState.readOnlyRoutingNodes().numberOfShardsOfType(ShardRoutingState.STARTED), equalTo(2));
        assertThat(clusterState.readOnlyRoutingNodes().numberOfShardsOfType(ShardRoutingState.INITIALIZING), equalTo(2));
        for (MutableShardRouting shardRouting : clusterState.readOnlyRoutingNodes().shardsWithState(INITIALIZING)) {
            assertThat(shardRouting.currentNodeId(), equalTo("node3"));
        }
    }
}
