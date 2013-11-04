package org.elasticsearch.cluster.serialization;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;

/**
 *
 */
public class ClusterStateToStringTests extends ElasticsearchTestCase {
    @Test
    public void testClusterStateSerialization() throws Exception {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test_idx").numberOfShards(10).numberOfReplicas(1))
                .put(IndexTemplateMetaData.builder("test_template").build())
                .build();

        RoutingTable routingTable = RoutingTable.builder()
                .addAsNew(metaData.index("test_idx"))
                .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder().put(new DiscoveryNode("node_foo", DummyTransportAddress.INSTANCE, Version.CURRENT)).localNodeId("node_foo").masterNodeId("node_foo").build();

        ClusterState clusterState = ClusterState.builder().nodes(nodes).metaData(metaData).routingTable(routingTable).build();

        AllocationService strategy = new AllocationService();
        clusterState = ClusterState.builder(clusterState).routingTable(strategy.reroute(clusterState).routingTable()).build();

        String clusterStateString = clusterState.toString();
        assertNotNull(clusterStateString);

        assertThat(clusterStateString, containsString("test_idx"));
        assertThat(clusterStateString, containsString("test_template"));
        assertThat(clusterStateString, containsString("node_foo"));

    }

}
