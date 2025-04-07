/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.IndexVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.shuffle;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.elasticsearch.cluster.routing.TestShardRouting.shardRoutingBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class IndexVersionAllocationDeciderTests extends ESAllocationTestCase {

    public void testDoNotAllocateFromPrimary() {
        AllocationService strategy = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(initialRoutingTable).build();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(5));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(2).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(2).currentNodeId(), nullValue());
        }

        logger.info("start two nodes and fully start the shards");
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")))
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(2));

        }

        logger.info("start all the primary shards, replicas will start initializing");
        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(INITIALIZING).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(1));
        }

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(1));
        }

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", Version.CURRENT, IndexVersionUtils.getPreviousVersion()))
            )
            .build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(UNASSIGNED).size(), equalTo(1));
        }

        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node4"))).build();
        clusterState = strategy.reroute(clusterState, "reroute", ActionListener.noop());

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(1));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(INITIALIZING).size(), equalTo(1));
        }

        clusterState = startInitializingShardsAndReroute(strategy, clusterState);

        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).primaryShard().state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).replicaShardsWithState(STARTED).size(), equalTo(2));
        }
    }

    public void testRandom() {
        AllocationService service = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table");
        Metadata.Builder builder = Metadata.builder();
        RoutingTable.Builder rtBuilder = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        int numIndices = between(1, 20);
        for (int i = 0; i < numIndices; i++) {
            builder.put(
                IndexMetadata.builder("test_" + i)
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(between(1, 5))
                    .numberOfReplicas(between(0, 2))
            );
        }
        Metadata metadata = builder.build();

        for (int i = 0; i < numIndices; i++) {
            rtBuilder.addAsNew(metadata.getProject().index("test_" + i));
        }
        RoutingTable routingTable = rtBuilder.build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();
        assertThat(shardsWithState(clusterState.getRoutingNodes(), UNASSIGNED).size(), equalTo((int) routingTable.allShards().count()));
        List<DiscoveryNode> nodes = new ArrayList<>();
        int nodeIdx = 0;
        int iters = scaledRandomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder();
            int numNodes = between(1, 20);
            if (nodes.size() > numNodes) {
                shuffle(nodes, random());
                nodes = nodes.subList(0, numNodes);
            } else {
                for (int j = nodes.size(); j < numNodes; j++) {
                    if (frequently()) {
                        if (randomBoolean()) {
                            nodes.add(newNode("node" + (nodeIdx++), Version.CURRENT, IndexVersionUtils.getPreviousVersion()));
                        } else {
                            nodes.add(newNode("node" + (nodeIdx++), Version.CURRENT, IndexVersion.current()));
                        }
                    } else {
                        nodes.add(newNode("node" + (nodeIdx++), Version.CURRENT, IndexVersionUtils.randomVersion()));
                    }
                }
            }
            for (DiscoveryNode node : nodes) {
                nodesBuilder.add(node);
            }
            clusterState = ClusterState.builder(clusterState).nodes(nodesBuilder).build();
            clusterState = stabilize(clusterState, service);
        }
    }

    public void testRollingRestart() {
        AllocationService service = createAllocationService(
            Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", 10)
                .put(ClusterRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ALLOW_REBALANCE_SETTING.getKey(), "always")
                .put("cluster.routing.allocation.cluster_concurrent_rebalance", -1)
                .build()
        );

        logger.info("Building initial routing table");

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(5).numberOfReplicas(2))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(metadata).routingTable(routingTable).build();

        assertThat(clusterState.routingTable().index("test").size(), equalTo(5));
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(2).state(), equalTo(UNASSIGNED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), nullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(2).currentNodeId(), nullValue());
        }
        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("old0", Version.CURRENT, IndexVersionUtils.getPreviousVersion()))
                    .add(newNode("old1", Version.CURRENT, IndexVersionUtils.getPreviousVersion()))
                    .add(newNode("old2", Version.CURRENT, IndexVersionUtils.getPreviousVersion()))
            )
            .build();
        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("old0", Version.CURRENT, IndexVersionUtils.getPreviousVersion()))
                    .add(newNode("old1", Version.CURRENT, IndexVersionUtils.getPreviousVersion()))
                    .add(newNode("new0"))
            )
            .build();

        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("node0", Version.CURRENT, IndexVersionUtils.getPreviousVersion()))
                    .add(newNode("new1"))
                    .add(newNode("new0"))
            )
            .build();

        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("new2")).add(newNode("new1")).add(newNode("new0")))
            .build();

        clusterState = stabilize(clusterState, service);
        for (int i = 0; i < clusterState.routingTable().index("test").size(); i++) {
            assertThat(clusterState.routingTable().index("test").shard(i).size(), equalTo(3));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(2).state(), equalTo(STARTED));
            assertThat(clusterState.routingTable().index("test").shard(i).shard(0).currentNodeId(), notNullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(1).currentNodeId(), notNullValue());
            assertThat(clusterState.routingTable().index("test").shard(i).shard(2).currentNodeId(), notNullValue());
        }
    }

    public void testRebalanceDoesNotAllocatePrimaryAndReplicasOnDifferentVersionNodes() {
        ShardId shard1 = new ShardId("test1", "_na_", 0);
        ShardId shard2 = new ShardId("test2", "_na_", 0);
        final DiscoveryNode newNode = DiscoveryNodeUtils.builder("newNode").roles(MASTER_DATA_ROLES).build();
        final DiscoveryNode oldNode1 = DiscoveryNodeUtils.builder("oldNode1")
            .roles(MASTER_DATA_ROLES)
            .version(Version.CURRENT, IndexVersions.MINIMUM_COMPATIBLE, IndexVersionUtils.getPreviousVersion())
            .build();
        final DiscoveryNode oldNode2 = DiscoveryNodeUtils.builder("oldNode2")
            .roles(MASTER_DATA_ROLES)
            .version(Version.CURRENT, IndexVersions.MINIMUM_COMPATIBLE, IndexVersionUtils.getPreviousVersion())
            .build();
        AllocationId allocationId1P = AllocationId.newInitializing();
        AllocationId allocationId1R = AllocationId.newInitializing();
        AllocationId allocationId2P = AllocationId.newInitializing();
        AllocationId allocationId2R = AllocationId.newInitializing();
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shard1.getIndexName())
                    .settings(settings(IndexVersion.current()).put(Settings.EMPTY))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Sets.newHashSet(allocationId1P.getId(), allocationId1R.getId()))
            )
            .put(
                IndexMetadata.builder(shard2.getIndexName())
                    .settings(settings(IndexVersion.current()).put(Settings.EMPTY))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Sets.newHashSet(allocationId2P.getId(), allocationId2R.getId()))
            )
            .build();
        RoutingTable routingTable = RoutingTable.builder()
            .add(
                IndexRoutingTable.builder(shard1.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shard1).addShard(
                            shardRoutingBuilder(shard1.getIndexName(), shard1.getId(), newNode.getId(), true, ShardRoutingState.STARTED)
                                .withAllocationId(allocationId1P)
                                .build()
                        )
                            .addShard(
                                shardRoutingBuilder(
                                    shard1.getIndexName(),
                                    shard1.getId(),
                                    oldNode1.getId(),
                                    false,
                                    ShardRoutingState.STARTED
                                ).withAllocationId(allocationId1R).build()
                            )
                    )
            )
            .add(
                IndexRoutingTable.builder(shard2.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shard2).addShard(
                            shardRoutingBuilder(shard2.getIndexName(), shard2.getId(), newNode.getId(), true, ShardRoutingState.STARTED)
                                .withAllocationId(allocationId2P)
                                .build()
                        )
                            .addShard(
                                shardRoutingBuilder(
                                    shard2.getIndexName(),
                                    shard2.getId(),
                                    oldNode1.getId(),
                                    false,
                                    ShardRoutingState.STARTED
                                ).withAllocationId(allocationId2R).build()
                            )
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode).add(oldNode1).add(oldNode2))
            .build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(Collections.singleton(new IndexVersionAllocationDecider()));
        AllocationService strategy = new MockAllocationService(
            allocationDeciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            EmptySnapshotsInfoService.INSTANCE
        );
        state = strategy.reroute(state, new AllocationCommands(), true, false, false, ActionListener.noop()).clusterState();
        // the two indices must stay as is, the replicas cannot move to oldNode2 because versions don't match
        assertThat(state.routingTable().index(shard2.getIndex()).shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(0));
        assertThat(state.routingTable().index(shard1.getIndex()).shardsWithState(ShardRoutingState.RELOCATING).size(), equalTo(0));
    }

    public void testRestoreDoesNotAllocateSnapshotOnOlderNodes() {
        final DiscoveryNode newNode = DiscoveryNodeUtils.builder("newNode").roles(MASTER_DATA_ROLES).build();
        final DiscoveryNode oldNode1 = DiscoveryNodeUtils.builder("oldNode1")
            .roles(MASTER_DATA_ROLES)
            .version(Version.CURRENT, IndexVersions.MINIMUM_COMPATIBLE, IndexVersionUtils.getPreviousVersion())
            .build();
        final DiscoveryNode oldNode2 = DiscoveryNodeUtils.builder("oldNode2")
            .roles(MASTER_DATA_ROLES)
            .version(Version.CURRENT, IndexVersions.MINIMUM_COMPATIBLE, IndexVersionUtils.getPreviousVersion())
            .build();

        final Snapshot snapshot = new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID()));
        final IndexId indexId = new IndexId("test", UUIDs.randomBase64UUID(random()));

        final int numberOfShards = randomIntBetween(1, 3);
        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(randomIntBetween(0, 3));
        for (int i = 0; i < numberOfShards; i++) {
            indexMetadata.putInSyncAllocationIds(i, Collections.singleton("_test_"));
        }
        Metadata metadata = Metadata.builder().put(indexMetadata).build();

        final Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes = new HashMap<>(numberOfShards);
        final Index index = metadata.getProject().index("test").getIndex();
        for (int i = 0; i < numberOfShards; i++) {
            final ShardId shardId = new ShardId(index, i);
            snapshotShardSizes.put(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, shardId), randomNonNegativeLong());
        }

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsRestore(
                        metadata.getProject().index("test"),
                        new SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, IndexVersion.current(), indexId)
                    )
                    .build()
            )
            .nodes(DiscoveryNodes.builder().add(newNode).add(oldNode1).add(oldNode2))
            .build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            Arrays.asList(new ReplicaAfterPrimaryActiveAllocationDecider(), new IndexVersionAllocationDecider())
        );
        AllocationService strategy = new MockAllocationService(
            allocationDeciders,
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            EmptyClusterInfoService.INSTANCE,
            () -> new SnapshotShardSizeInfo(snapshotShardSizes)
        );
        state = strategy.reroute(state, new AllocationCommands(), true, false, false, ActionListener.noop()).clusterState();

        // Make sure that primary shards are only allocated on the new node
        for (int i = 0; i < numberOfShards; i++) {
            assertEquals("newNode", state.routingTable().index("test").shard(i).primaryShard().currentNodeId());
        }
    }

    private ClusterState stabilize(ClusterState clusterState, AllocationService service) {
        logger.trace("RoutingNodes: {}", clusterState.getRoutingNodes());

        clusterState = service.disassociateDeadNodes(clusterState, true, "reroute");
        RoutingNodes routingNodes = clusterState.getRoutingNodes();
        assertRecoveryIndexVersions(routingNodes);

        logger.info("complete rebalancing");
        boolean changed;
        do {
            logger.trace("RoutingNodes: {}", clusterState.getRoutingNodes());
            ClusterState newState = startInitializingShardsAndReroute(service, clusterState);
            changed = newState.equals(clusterState) == false;
            clusterState = newState;
            routingNodes = clusterState.getRoutingNodes();
            assertRecoveryIndexVersions(routingNodes);
        } while (changed);
        return clusterState;
    }

    private void assertRecoveryIndexVersions(RoutingNodes routingNodes) {
        logger.trace("RoutingNodes: {}", routingNodes);

        List<ShardRouting> mutableShardRoutings = shardsWithState(routingNodes, ShardRoutingState.RELOCATING);
        for (ShardRouting r : mutableShardRoutings) {
            if (r.primary()) {
                String toId = r.relocatingNodeId();
                String fromId = r.currentNodeId();
                assertThat(fromId, notNullValue());
                assertThat(toId, notNullValue());
                logger.trace(
                    "From: {} with IndexVersion: {} to: {} with IndexVersion: {}",
                    fromId,
                    routingNodes.node(fromId).node().getMaxIndexVersion(),
                    toId,
                    routingNodes.node(toId).node().getMaxIndexVersion()
                );
                assertTrue(
                    routingNodes.node(toId).node().getMaxIndexVersion().onOrAfter(routingNodes.node(fromId).node().getMaxIndexVersion())
                );
            } else {
                ShardRouting primary = routingNodes.activePrimary(r.shardId());
                assertThat(primary, notNullValue());
                String fromId = primary.currentNodeId();
                String toId = r.relocatingNodeId();
                logger.trace(
                    "From: {} with IndexVersion: {} to: {} with IndexVersion: {}",
                    fromId,
                    routingNodes.node(fromId).node().getMaxIndexVersion(),
                    toId,
                    routingNodes.node(toId).node().getMaxIndexVersion()
                );
                assertTrue(
                    routingNodes.node(toId).node().getMaxIndexVersion().onOrAfter(routingNodes.node(fromId).node().getMaxIndexVersion())
                );
            }
        }

        mutableShardRoutings = shardsWithState(routingNodes, ShardRoutingState.INITIALIZING);
        for (ShardRouting r : mutableShardRoutings) {
            if (r.primary() == false) {
                ShardRouting primary = routingNodes.activePrimary(r.shardId());
                assertThat(primary, notNullValue());
                String fromId = primary.currentNodeId();
                String toId = r.currentNodeId();
                logger.trace(
                    "From: {} with IndexVersion: {} to: {} with IndexVersion: {}",
                    fromId,
                    routingNodes.node(fromId).node().getMaxIndexVersion(),
                    toId,
                    routingNodes.node(toId).node().getMaxIndexVersion()
                );
                assertTrue(
                    routingNodes.node(toId).node().getMaxIndexVersion().onOrAfter(routingNodes.node(fromId).node().getMaxIndexVersion())
                );
            }
        }
    }

    public void testMessages() {

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.getProject().index("test"))
            .build();

        RoutingNode newNode = RoutingNodesHelper.routingNode("newNode", newNode("newNode", Version.CURRENT, IndexVersion.current()));
        RoutingNode oldNode = RoutingNodesHelper.routingNode(
            "oldNode",
            newNode("oldNode", Version.CURRENT, IndexVersionUtils.getPreviousVersion())
        );

        final ClusterName clusterName = ClusterName.DEFAULT;
        ClusterState clusterState = ClusterState.builder(clusterName)
            .metadata(metadata)
            .routingTable(initialRoutingTable)
            .nodes(DiscoveryNodes.builder().add(newNode.node()).add(oldNode.node()))
            .build();

        final ShardId shardId = clusterState.routingTable().index("test").shard(0).shardId();
        final ShardRouting primaryShard = clusterState.routingTable().shardRoutingTable(shardId).primaryShard();
        final ShardRouting replicaShard = clusterState.routingTable().shardRoutingTable(shardId).replicaShards().get(0);

        RoutingAllocation routingAllocation = new RoutingAllocation(null, clusterState, null, null, 0);
        routingAllocation.debugDecision(true);

        final IndexVersionAllocationDecider allocationDecider = new IndexVersionAllocationDecider();
        Decision decision = allocationDecider.canAllocate(primaryShard, newNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(decision.getExplanation(), is("no existing allocation, assuming compatible"));

        decision = allocationDecider.canAllocate(ShardRoutingHelper.initialize(primaryShard, "oldNode"), newNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            is(
                "can relocate primary shard from a node with index version ["
                    + oldNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "] to a node with equal-or-newer index version ["
                    + newNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "]"
            )
        );

        decision = allocationDecider.canAllocate(ShardRoutingHelper.initialize(primaryShard, "newNode"), oldNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            is(
                "cannot relocate primary shard from a node with index version ["
                    + newNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "] to a node with older index version ["
                    + oldNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "]"
            )
        );

        final IndexId indexId = new IndexId("test", UUIDs.randomBase64UUID(random()));
        final SnapshotRecoverySource newVersionSnapshot = new SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
            newNode.node().getMaxIndexVersion(),
            indexId
        );
        final SnapshotRecoverySource oldVersionSnapshot = new SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
            oldNode.node().getMaxIndexVersion(),
            indexId
        );

        decision = allocationDecider.canAllocate(
            ShardRoutingHelper.newWithRestoreSource(primaryShard, newVersionSnapshot),
            oldNode,
            routingAllocation
        );
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            is(
                "max supported index version ["
                    + oldNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "] is older than the snapshot version ["
                    + newNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "]"
            )
        );

        decision = allocationDecider.canAllocate(
            ShardRoutingHelper.newWithRestoreSource(primaryShard, oldVersionSnapshot),
            newNode,
            routingAllocation
        );
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            is(
                "max supported index version ["
                    + newNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "] is the same or newer than snapshot version ["
                    + oldNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "]"
            )
        );

        final RoutingChangesObserver routingChangesObserver = new RoutingChangesObserver() {
        };
        final RoutingNodes routingNodes = clusterState.mutableRoutingNodes();
        final ShardRouting startedPrimary = routingNodes.startShard(
            routingNodes.initializeShard(primaryShard, "newNode", null, 0, routingChangesObserver),
            routingChangesObserver,
            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
        );
        routingAllocation = new RoutingAllocation(null, routingNodes, clusterState, null, null, 0);
        routingAllocation.debugDecision(true);

        decision = allocationDecider.canAllocate(replicaShard, oldNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            is(
                "cannot allocate replica shard to a node with index version ["
                    + oldNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "] since this is older than the primary index version ["
                    + newNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "]"
            )
        );

        routingNodes.startShard(
            routingNodes.relocateShard(startedPrimary, "oldNode", 0, "test", routingChangesObserver).v2(),
            routingChangesObserver,
            ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE
        );
        routingAllocation = new RoutingAllocation(null, routingNodes, clusterState, null, null, 0);
        routingAllocation.debugDecision(true);

        decision = allocationDecider.canAllocate(replicaShard, newNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            is(
                "can allocate replica shard to a node with index version ["
                    + newNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "] since this is equal-or-newer than the primary index version ["
                    + oldNode.node().getMaxIndexVersion().toReleaseVersion()
                    + "]"
            )
        );
    }
}
