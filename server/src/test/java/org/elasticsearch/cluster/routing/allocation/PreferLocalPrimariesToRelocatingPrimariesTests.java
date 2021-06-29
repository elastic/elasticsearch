/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

public class PreferLocalPrimariesToRelocatingPrimariesTests extends ESAllocationTestCase {

    public void testPreferLocalPrimaryAllocationOverFiltered() {
        int concurrentRecoveries = randomIntBetween(1, 10);
        int primaryRecoveries = randomIntBetween(1, 10);
        int numberOfShards = randomIntBetween(5, 20);
        int totalNumberOfShards = numberOfShards * 2;

        logger.info("create an allocation with [{}] initial primary recoveries and [{}] concurrent recoveries",
            primaryRecoveries, concurrentRecoveries);
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", concurrentRecoveries)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", primaryRecoveries)
                .build());

        logger.info("create 2 indices with [{}] no replicas, and wait till all are allocated", numberOfShards);

        Metadata metadata = Metadata.builder()
                .put(IndexMetadata.builder("test1").settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards).numberOfReplicas(0))
                .put(IndexMetadata.builder("test2").settings(settings(Version.CURRENT))
                    .numberOfShards(numberOfShards).numberOfReplicas(0))
                .build();

        RoutingTable initialRoutingTable = RoutingTable.builder()
                .addAsNew(metadata.index("test1"))
                .addAsNew(metadata.index("test2"))
                .build();

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING
            .getDefault(Settings.EMPTY)).metadata(metadata).routingTable(initialRoutingTable).build();

        logger.info("adding two nodes and performing rerouting till all are allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder()
                .add(newNode("node1")).add(newNode("node2"))).build();

        clusterState = strategy.reroute(clusterState, "reroute");


        while (clusterState.getRoutingNodes().shardsWithState(INITIALIZING).isEmpty() == false) {
            clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        }

        logger.info("remove one of the nodes and apply filter to move everything from another node");

        metadata = Metadata.builder()
                .put(IndexMetadata.builder(clusterState.metadata().index("test1")).settings(settings(Version.CURRENT)
                        .put("index.number_of_shards", numberOfShards)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.exclude._name", "node2")
                        .build()))
                .put(IndexMetadata.builder(clusterState.metadata().index("test2")).settings(settings(Version.CURRENT)
                        .put("index.number_of_shards", numberOfShards)
                        .put("index.number_of_replicas", 0)
                        .put("index.routing.allocation.exclude._name", "node2")
                        .build()))
                .build();
        clusterState = ClusterState.builder(clusterState).metadata(metadata)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).remove("node1")).build();
        clusterState = strategy.disassociateDeadNodes(clusterState, true, "reroute");

        logger.info("[{}] primaries should be still started but [{}] other primaries should be unassigned",
            numberOfShards, numberOfShards);
        assertThat(clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(numberOfShards));
        assertThat(clusterState.getRoutingNodes().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(numberOfShards));

        logger.info("start node back up");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes())
                .add(newNode("node1", singletonMap("tag1", "value1")))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        while (clusterState.getRoutingNodes().shardsWithState(STARTED).size() < totalNumberOfShards) {
            int localInitializations = 0;
            int relocatingInitializations = 0;
            for (ShardRouting routing : clusterState.getRoutingNodes().shardsWithState(INITIALIZING)) {
                if (routing.relocatingNodeId() == null) {
                    localInitializations++;
                } else {
                    relocatingInitializations++;
                }
            }
            int needToInitialize = totalNumberOfShards - clusterState.getRoutingNodes().shardsWithState(STARTED).size()
                - clusterState.getRoutingNodes().shardsWithState(RELOCATING).size();
            logger.info("local initializations: [{}], relocating: [{}], need to initialize: {}",
                localInitializations, relocatingInitializations, needToInitialize);
            assertThat(localInitializations, equalTo(Math.min(primaryRecoveries, needToInitialize)));
            clusterState = startRandomInitializingShard(clusterState, strategy);
        }
    }
}
