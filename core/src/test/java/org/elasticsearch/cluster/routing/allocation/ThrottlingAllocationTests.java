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

import com.carrotsearch.hppc.IntHashSet;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RestoreSource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESAllocationTestCase;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class ThrottlingAllocationTests extends ESAllocationTestCase {
    private final ESLogger logger = Loggers.getLogger(ThrottlingAllocationTests.class);

    public void testPrimaryRecoveryThrottling() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = createRecoveryRoutingTable(metaData.index("test"));

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(17));

        logger.info("start initializing, another 3 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(14));

        logger.info("start initializing, another 3 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(6));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(11));

        logger.info("start initializing, another 1 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(9));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(10));

        logger.info("start initializing, all primaries should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(10));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(10));
    }

    public void testReplicaAndPrimaryRecoveryThrottling() {
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.concurrent_source_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = createRecoveryRoutingTable(metaData.index("test"));

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(7));

        logger.info("start initializing, another 2 should initialize");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start initializing, all primaries should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start another node, replicas should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing replicas");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(8));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));

        logger.info("start initializing replicas, all should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(10));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThrottleIncomingAndOutgoing() {
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 5)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 5)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 5)
            .build();
        AllocationService strategy = createAllocationService(settings);
        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(9).numberOfReplicas(0))
            .build();

        RoutingTable routingTable = createRecoveryRoutingTable(metaData.index("test"));

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 5 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(4));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 5);

        logger.info("start initializing, all primaries should be started");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(4));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));

        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("start another 2 nodes, 5 shards should be relocating - at most 5 are allowed per node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2")).add(newNode("node3"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(4));
        assertThat(routingTable.shardsWithState(RELOCATING).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(5));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 3);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 2);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 5);

        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        logger.info("start the relocating shards, one more shard should relocate away from node1");
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(8));
        assertThat(routingTable.shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 0);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 1);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);
    }

    public void testOutgoingThrottlesAllocation() {
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 1)
            .build());

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        RoutingTable routingTable = createRecoveryRoutingTable(metaData.index("test"));

        ClusterState clusterState = ClusterState.builder(org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metaData(metaData).routingTable(routingTable).build();

        logger.info("start one node, do reroute, only 1 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start one more node, first non-primary should start being allocated");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("start initializing non-primary");
        routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);

        logger.info("start one more node, initializing second non-primary");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("start one more node");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        routingTable = strategy.reroute(clusterState, "reroute").routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();

        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("move started non-primary to new node");
        RoutingAllocation.Result reroute = strategy.reroute(clusterState, new AllocationCommands(
            new MoveAllocationCommand("test", 0, "node2", "node4")), true, false);
        assertEquals(reroute.explanations().explanations().size(), 1);
        assertEquals(reroute.explanations().explanations().get(0).decisions().type(), Decision.Type.THROTTLE);
        // even though it is throttled, move command still forces allocation

        clusterState = ClusterState.builder(clusterState).routingResult(reroute).build();
        routingTable = clusterState.routingTable();
        assertThat(routingTable.shardsWithState(STARTED).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(routingTable.shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(routingTable.shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 2);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), 0);
    }

    private RoutingTable createRecoveryRoutingTable(IndexMetaData indexMetaData) {
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        switch (randomInt(5)) {
            case 0: routingTableBuilder.addAsRecovery(indexMetaData); break;
            case 1: routingTableBuilder.addAsFromCloseToOpen(indexMetaData); break;
            case 2: routingTableBuilder.addAsFromDangling(indexMetaData); break;
            case 3: routingTableBuilder.addAsNewRestore(indexMetaData,
                new RestoreSource(new Snapshot("repo", new SnapshotId("snap", "randomId")), Version.CURRENT,
                indexMetaData.getIndex().getName()), new IntHashSet()); break;
            case 4: routingTableBuilder.addAsRestore(indexMetaData,
                new RestoreSource(new Snapshot("repo", new SnapshotId("snap", "randomId")), Version.CURRENT,
                indexMetaData.getIndex().getName())); break;
            case 5: routingTableBuilder.addAsNew(indexMetaData); break;
            default: throw new IndexOutOfBoundsException();
        }

        return routingTableBuilder.build();
    }

}
