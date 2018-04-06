/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;


import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.decider.FilterAllocationDecider;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

public class AllocationRoutedStepTests extends ESTestCase {

    public void testCanStay() {

    }

    private void assertAllocateStatus(Index index, int shards, int replicas, AllocateAction action, Settings.Builder existingSettings,
                                      Settings.Builder node1Settings, Settings.Builder node2Settings,
                                      IndexRoutingTable.Builder indexRoutingTable, boolean expectComplete) {
        IndexMetaData indexMetadata = IndexMetaData.builder(index.getName()).settings(existingSettings).numberOfShards(shards)
                .numberOfReplicas(replicas).build();
        ImmutableOpenMap.Builder<String, IndexMetaData> indices = ImmutableOpenMap.<String, IndexMetaData> builder().fPut(index.getName(),
                indexMetadata);

        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metaData(MetaData.builder().indices(indices.build()))
                .nodes(DiscoveryNodes.builder()
                        .add(DiscoveryNode.createLocal(node1Settings.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9200),
                                "node1"))
                        .add(DiscoveryNode.createLocal(node2Settings.build(), new TransportAddress(TransportAddress.META_ADDRESS, 9201),
                                "node2")))
                .routingTable(RoutingTable.builder().add(indexRoutingTable).build()).build();
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY,
                Sets.newHashSet(FilterAllocationDecider.CLUSTER_ROUTING_INCLUDE_GROUP_SETTING,
                        FilterAllocationDecider.CLUSTER_ROUTING_EXCLUDE_GROUP_SETTING,
                        FilterAllocationDecider.CLUSTER_ROUTING_REQUIRE_GROUP_SETTING));
    }
}
