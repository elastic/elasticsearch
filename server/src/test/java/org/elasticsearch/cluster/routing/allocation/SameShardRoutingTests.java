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
import org.elasticsearch.Version;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;

import java.util.Collections;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.allocation.RoutingNodesUtils.numberOfShardsOfType;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class SameShardRoutingTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(SameShardRoutingTests.class);

    public void testSameHost() {
        AllocationService strategy = createAllocationService(
            Settings.builder().put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), true).build()
        );

        Metadata metadata = initIndexMetadata();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        logger.info("--> adding two nodes with the same host");
        clusterState = initTwoNodes(clusterState, "test1", "test1", "test1", "test1");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("--> start all primary shards, no replica will be started since its on the same host");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(0));

        logger.info("--> add another node, with a different host, replicas will be allocating");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(
                        new DiscoveryNode(
                            "node3",
                            "node3",
                            "node3",
                            "test2",
                            "test2",
                            buildNewFakeTransportAddress(),
                            emptyMap(),
                            MASTER_DATA_ROLES,
                            Version.CURRENT
                        )
                    )
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));
        for (ShardRouting shardRouting : shardsWithState(clusterState.getRoutingNodes(), INITIALIZING)) {
            assertThat(shardRouting.currentNodeId(), equalTo("node3"));
        }
    }

    public void testSameHostNameDifferentHostAddressWithHostNameOff() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), true)
                .put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_NAME.getKey(), false)
                .build()
        );

        Metadata metadata = initIndexMetadata();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        logger.info(
            "--> adding two nodes with the same host name and different host address. "
                + "They are treated as different hosts when ALWAYS_CHECK_HOSTNAME is off"
        );
        clusterState = initTwoNodes(clusterState, "test1", "test1_1", "test1", "test1_2");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("--> start all primary shards");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));
        logger.info("--> all shards will be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(4));
    }

    public void testSameHostNameDifferentHostAddressWithHostNameOn() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), true)
                .put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_NAME.getKey(), true)
                .build()
        );

        Metadata metadata = initIndexMetadata();

        RoutingTable routingTable = RoutingTable.builder().addAsNew(metadata.index("test")).build();
        ClusterState clusterState = ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(routingTable)
            .build();

        logger.info(
            "--> adding two nodes with the same host name and different host address. "
                + "They are treated as the same host when ALWAYS_CHECK_HOSTNAME is on"
        );
        clusterState = initTwoNodes(clusterState, "test1", "test1_1", "test1", "test1_2");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));

        logger.info("--> start all primary shards, no replica will be started since its on the same host");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(0));

        logger.info("--> add another node, with a different host, replicas will be allocating");
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes())
                    .add(
                        new DiscoveryNode(
                            "node3",
                            "node3",
                            "node3",
                            "test2",
                            "test2",
                            buildNewFakeTransportAddress(),
                            emptyMap(),
                            MASTER_DATA_ROLES,
                            Version.CURRENT
                        )
                    )
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.STARTED), equalTo(2));
        assertThat(numberOfShardsOfType(clusterState.getRoutingNodes(), ShardRoutingState.INITIALIZING), equalTo(2));
        for (ShardRouting shardRouting : shardsWithState(clusterState.getRoutingNodes(), INITIALIZING)) {
            assertThat(shardRouting.currentNodeId(), equalTo("node3"));
        }
    }

    private Metadata initIndexMetadata() {
        final Settings.Builder indexSettings = settings(Version.CURRENT);
        if (randomBoolean()) {
            indexSettings.put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1");
        }

        return Metadata.builder().put(IndexMetadata.builder("test").settings(indexSettings).numberOfShards(2).numberOfReplicas(1)).build();
    }

    private ClusterState initTwoNodes(ClusterState clusterState, String hostName1, String address1, String hostName2, String address2) {
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(
                        new DiscoveryNode(
                            "node1",
                            "node1",
                            "node1",
                            hostName1,
                            address1,
                            buildNewFakeTransportAddress(),
                            emptyMap(),
                            MASTER_DATA_ROLES,
                            Version.CURRENT
                        )
                    )
                    .add(
                        new DiscoveryNode(
                            "node2",
                            "node2",
                            "node2",
                            hostName2,
                            address2,
                            buildNewFakeTransportAddress(),
                            emptyMap(),
                            MASTER_DATA_ROLES,
                            Version.CURRENT
                        )
                    )
            )
            .build();
        return clusterState;
    }

    public void testSameHostCheckDisabledByAutoExpandReplicas() {
        final AllocationService strategy = createAllocationService(
            Settings.builder().put(SameShardAllocationDecider.CLUSTER_ROUTING_ALLOCATION_SAME_HOST_SETTING.getKey(), true).build()
        );

        final Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder("test")
                    .settings(
                        settings(Version.CURRENT).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 99)
                            .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-all")
                    )
            )
            .build();

        final DiscoveryNode node1 = new DiscoveryNode(
            "node1",
            "node1",
            "node1",
            "test1",
            "test1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            MASTER_DATA_ROLES,
            Version.CURRENT
        );

        final DiscoveryNode node2 = new DiscoveryNode(
            "node2",
            "node2",
            "node2",
            "test1",
            "test1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            MASTER_DATA_ROLES,
            Version.CURRENT
        );

        final ClusterState clusterState = applyStartedShardsUntilNoChange(
            ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
                .metadata(metadata)
                .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build())
                .nodes(DiscoveryNodes.builder().add(node1).add(node2))
                .build(),
            strategy
        );

        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED), empty());
    }

    public void testForceAllocatePrimaryOnSameNodeNotAllowed() {
        SameShardAllocationDecider decider = new SameShardAllocationDecider(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomIntBetween(2, 4), 1);
        Index index = clusterState.getMetadata().index("idx").getIndex();
        ShardRouting primaryShard = clusterState.routingTable().index(index).shard(0).primaryShard();
        RoutingNode routingNode = clusterState.getRoutingNodes().node(primaryShard.currentNodeId());
        RoutingAllocation routingAllocation = new RoutingAllocation(
            new AllocationDeciders(Collections.emptyList()),
            new RoutingNodes(clusterState, false),
            clusterState,
            ClusterInfo.EMPTY,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );

        // can't force allocate same shard copy to the same node
        ShardRouting newPrimary = TestShardRouting.newShardRouting(primaryShard.shardId(), null, true, ShardRoutingState.UNASSIGNED);
        Decision decision = decider.canForceAllocatePrimary(newPrimary, routingNode, routingAllocation);
        assertEquals(Decision.Type.NO, decision.type());

        // can force allocate to a different node
        RoutingNode unassignedNode = null;
        for (RoutingNode node : clusterState.getRoutingNodes()) {
            if (node.isEmpty()) {
                unassignedNode = node;
                break;
            }
        }
        decision = decider.canForceAllocatePrimary(newPrimary, unassignedNode, routingAllocation);
        assertEquals(Decision.Type.YES, decision.type());
    }
}
