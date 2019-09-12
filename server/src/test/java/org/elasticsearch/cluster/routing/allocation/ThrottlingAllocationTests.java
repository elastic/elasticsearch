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
import com.carrotsearch.hppc.cursors.ObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.cluster.ClusterName.CLUSTER_NAME_SETTING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;

public class ThrottlingAllocationTests extends ESAllocationTestCase {
    private final Logger logger = LogManager.getLogger(ThrottlingAllocationTests.class);

    public void testPrimaryRecoveryThrottling() {

        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build(), gatewayAllocator);

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(10).numberOfReplicas(1))
                .build();

        ClusterState clusterState = createRecoveryStateAndInitalizeAllocations(metaData, gatewayAllocator);

        logger.info("start one node, do reroute, only 3 should initialize");
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder().add(newNode("node1"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(17));

        logger.info("start initializing, another 3 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(14));

        logger.info("start initializing, another 3 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(6));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(11));

        logger.info("start initializing, another 1 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(9));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(10));

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(10));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(10));
    }

    public void testReplicaAndPrimaryRecoveryThrottling() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        AllocationService strategy = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 3)
                .put("cluster.routing.allocation.concurrent_source_recoveries", 3)
                .put("cluster.routing.allocation.node_initial_primaries_recoveries", 3)
                .build(),
            gatewayAllocator);

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        ClusterState clusterState = createRecoveryStateAndInitalizeAllocations(metaData, gatewayAllocator);

        logger.info("with one node, do reroute, only 3 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(7));

        logger.info("start initializing, another 2 should initialize");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(5));

        logger.info("start another node, replicas should start being allocated");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(3));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing replicas");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));

        logger.info("start initializing replicas, all should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(10));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
    }

    public void testThrottleIncomingAndOutgoing() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        Settings settings = Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_recoveries", 5)
            .put("cluster.routing.allocation.node_initial_primaries_recoveries", 5)
            .put("cluster.routing.allocation.cluster_concurrent_rebalance", 5)
            .build();
        AllocationService strategy = createAllocationService(settings, gatewayAllocator);
        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(9).numberOfReplicas(0))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitalizeAllocations(metaData, gatewayAllocator);

        logger.info("with one node, do reroute, only 5 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute");
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(4));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 5);

        logger.info("start initializing, all primaries should be started");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(4));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start another 2 nodes, 5 shards should be relocating - at most 5 are allowed per node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2")).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(4));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(5));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 3);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 2);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 5);

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        logger.info("start the relocating shards, one more shard should relocate away from node1");
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(8));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node2"), 0);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node3"), 1);
        assertEquals(clusterState.getRoutingNodes().getIncomingRecoveries("node1"), 0);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);
    }

    public void testOutgoingThrottlesAllocation() {
        TestGatewayAllocator gatewayAllocator = new TestGatewayAllocator();
        AllocationService strategy = createAllocationService(Settings.builder()
            .put("cluster.routing.allocation.node_concurrent_outgoing_recoveries", 1)
            .build(), gatewayAllocator);

        logger.info("Building initial routing table");

        MetaData metaData = MetaData.builder()
            .put(IndexMetaData.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(2))
            .build();

        ClusterState clusterState = createRecoveryStateAndInitalizeAllocations(metaData, gatewayAllocator);

        logger.info("with one node, do reroute, only 1 should initialize");
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(2));

        logger.info("start one more node, first non-primary should start being allocated");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node2"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("start initializing non-primary");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(0));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(1));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 0);

        logger.info("start one more node, initializing second non-primary");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("start one more node");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute");

        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 1);

        logger.info("move started non-primary to new node");
        AllocationService.CommandsResult commandsResult = strategy.reroute(clusterState, new AllocationCommands(
            new MoveAllocationCommand("test", 0, "node2", "node4")), true, false);
        assertEquals(commandsResult.explanations().explanations().size(), 1);
        assertEquals(commandsResult.explanations().explanations().get(0).decisions().type(), Decision.Type.THROTTLE);
        boolean foundThrottledMessage = false;
        for (Decision decision : commandsResult.explanations().explanations().get(0).decisions().getDecisions()) {
            if (decision.label().equals(ThrottlingAllocationDecider.NAME)) {
                assertEquals("reached the limit of outgoing shard recoveries [1] on the node [node1] which holds the primary, " 
                        + "cluster setting [cluster.routing.allocation.node_concurrent_outgoing_recoveries=1] " 
                        + "(can also be set via [cluster.routing.allocation.node_concurrent_recoveries])", 
                        decision.getExplanation());
                assertEquals(Decision.Type.THROTTLE, decision.type());
                foundThrottledMessage = true;
            }
        }
        assertTrue(foundThrottledMessage);
        // even though it is throttled, move command still forces allocation

        clusterState = commandsResult.getClusterState();
        assertThat(clusterState.routingTable().shardsWithState(STARTED).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(RELOCATING).size(), equalTo(1));
        assertThat(clusterState.routingTable().shardsWithState(INITIALIZING).size(), equalTo(2));
        assertThat(clusterState.routingTable().shardsWithState(UNASSIGNED).size(), equalTo(0));
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node1"), 2);
        assertEquals(clusterState.getRoutingNodes().getOutgoingRecoveries("node2"), 0);
    }

    private ClusterState createRecoveryStateAndInitalizeAllocations(MetaData metaData, TestGatewayAllocator gatewayAllocator) {
        DiscoveryNode node1 = newNode("node1");
        MetaData.Builder metaDataBuilder = new MetaData.Builder(metaData);
        RoutingTable.Builder routingTableBuilder = RoutingTable.builder();
        Snapshot snapshot = new Snapshot("repo", new SnapshotId("snap", "randomId"));
        Set<String> snapshotIndices = new HashSet<>();
        String restoreUUID = UUIDs.randomBase64UUID();
        for (ObjectCursor<IndexMetaData> cursor: metaData.indices().values()) {
            Index index = cursor.value.getIndex();
            IndexMetaData.Builder indexMetaDataBuilder = IndexMetaData.builder(cursor.value);
            final int recoveryType = randomInt(5);
            if (recoveryType <= 4) {
                addInSyncAllocationIds(index, indexMetaDataBuilder, gatewayAllocator, node1);
            }
            IndexMetaData indexMetaData = indexMetaDataBuilder.build();
            metaDataBuilder.put(indexMetaData, false);
            switch (recoveryType) {
                case 0:
                    routingTableBuilder.addAsRecovery(indexMetaData);
                    break;
                case 1:
                    routingTableBuilder.addAsFromCloseToOpen(indexMetaData);
                    break;
                case 2:
                    routingTableBuilder.addAsFromDangling(indexMetaData);
                    break;
                case 3:
                    snapshotIndices.add(index.getName());
                    routingTableBuilder.addAsNewRestore(indexMetaData,
                        new SnapshotRecoverySource(
                            restoreUUID, snapshot, Version.CURRENT, indexMetaData.getIndex().getName()), new IntHashSet());
                    break;
                case 4:
                    snapshotIndices.add(index.getName());
                    routingTableBuilder.addAsRestore(indexMetaData,
                        new SnapshotRecoverySource(
                            restoreUUID, snapshot, Version.CURRENT, indexMetaData.getIndex().getName()));
                    break;
                case 5:
                    routingTableBuilder.addAsNew(indexMetaData);
                    break;
                default:
                    throw new IndexOutOfBoundsException();
            }
        }

        final RoutingTable routingTable = routingTableBuilder.build();

        final ImmutableOpenMap.Builder<String, ClusterState.Custom> restores = ImmutableOpenMap.builder();
        if (snapshotIndices.isEmpty() == false) {
            // Some indices are restored from snapshot, the RestoreInProgress must be set accordingly
            ImmutableOpenMap.Builder<ShardId, RestoreInProgress.ShardRestoreStatus> restoreShards = ImmutableOpenMap.builder();
            for (ShardRouting shard : routingTable.allShards()) {
                if (shard.primary() && shard.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    ShardId shardId = shard.shardId();
                    restoreShards.put(shardId, new RestoreInProgress.ShardRestoreStatus(node1.getId(), RestoreInProgress.State.INIT));
                }
            }

            RestoreInProgress.Entry restore = new RestoreInProgress.Entry(restoreUUID, snapshot, RestoreInProgress.State.INIT,
                new ArrayList<>(snapshotIndices), restoreShards.build());
            restores.put(RestoreInProgress.TYPE, new RestoreInProgress.Builder().add(restore).build());
        }

        return ClusterState.builder(CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .nodes(DiscoveryNodes.builder().add(node1))
            .metaData(metaDataBuilder.build())
            .routingTable(routingTable)
            .customs(restores.build())
            .build();
    }

    private void addInSyncAllocationIds(Index index, IndexMetaData.Builder indexMetaData,
                                        TestGatewayAllocator gatewayAllocator, DiscoveryNode node1) {
        for (int shard = 0; shard < indexMetaData.numberOfShards(); shard++) {

            final boolean primary = randomBoolean();
            final ShardRouting unassigned = ShardRouting.newUnassigned(new ShardId(index, shard), primary,
                primary ?
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE :
                    RecoverySource.PeerRecoverySource.INSTANCE,
                new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test")
            );
            ShardRouting started = ShardRoutingHelper.moveToStarted(ShardRoutingHelper.initialize(unassigned, node1.getId()));
            indexMetaData.putInSyncAllocationIds(shard, Collections.singleton(started.allocationId().getId()));
            gatewayAllocator.addKnownAllocation(started);
        }
    }
}
