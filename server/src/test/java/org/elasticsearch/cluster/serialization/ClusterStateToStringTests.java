/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.serialization;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.common.Strings;

import java.util.Arrays;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;

public class ClusterStateToStringTests extends ESAllocationTestCase {
    public void testClusterStateSerialization() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test_idx").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
            .put(
                IndexTemplateMetadata.builder("test_template")
                    .patterns(Arrays.asList(generateRandomStringArray(10, 100, false, false)))
                    .build()
            )
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test_idx"))
            .build();

        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(TestDiscoveryNode.create("node_foo", buildNewFakeTransportAddress(), emptyMap(), emptySet()))
            .localNodeId("node_foo")
            .masterNodeId("node_foo")
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        AllocationService strategy = createAllocationService();
        clusterState = ClusterState.builder(clusterState)
            .routingTable(strategy.reroute(clusterState, "reroute", ActionListener.noop()).routingTable())
            .build();

        String clusterStateString = Strings.toString(clusterState);
        assertNotNull(clusterStateString);

        assertThat(clusterStateString, containsString("test_idx"));
        assertThat(clusterStateString, containsString("test_template"));
        assertThat(clusterStateString, containsString("node_foo"));
    }
}
