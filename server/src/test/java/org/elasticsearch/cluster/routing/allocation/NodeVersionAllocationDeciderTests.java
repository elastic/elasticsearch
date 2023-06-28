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
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.command.AllocationCommands;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.ClusterRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.NodeVersionAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.InternalSnapshotsInfoService;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

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
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class NodeVersionAllocationDeciderTests extends ESAllocationTestCase {

    private final Logger logger = LogManager.getLogger(NodeVersionAllocationDeciderTests.class);

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
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(2))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
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
            .nodes(DiscoveryNodes.builder(clusterState.nodes()).add(newNode("node3", VersionUtils.getPreviousVersion())))
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
                    .settings(settings(Version.CURRENT))
                    .numberOfShards(between(1, 5))
                    .numberOfReplicas(between(0, 2))
            );
        }
        Metadata metadata = builder.build();

        for (int i = 0; i < numIndices; i++) {
            rtBuilder.addAsNew(metadata.index("test_" + i));
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
                        nodes.add(newNode("node" + (nodeIdx++), randomBoolean() ? VersionUtils.getPreviousVersion() : Version.CURRENT));
                    } else {
                        nodes.add(newNode("node" + (nodeIdx++), randomVersion(random())));
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
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(2))
            .build();

        RoutingTable routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
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
                    .add(newNode("old0", VersionUtils.getPreviousVersion()))
                    .add(newNode("old1", VersionUtils.getPreviousVersion()))
                    .add(newNode("old2", VersionUtils.getPreviousVersion()))
            )
            .build();
        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder()
                    .add(newNode("old0", VersionUtils.getPreviousVersion()))
                    .add(newNode("old1", VersionUtils.getPreviousVersion()))
                    .add(newNode("new0"))
            )
            .build();

        clusterState = stabilize(clusterState, service);

        clusterState = ClusterState.builder(clusterState)
            .nodes(
                DiscoveryNodes.builder().add(newNode("node0", VersionUtils.getPreviousVersion())).add(newNode("new1")).add(newNode("new0"))
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
            .version(VersionUtils.getPreviousVersion())
            .build();
        final DiscoveryNode oldNode2 = DiscoveryNodeUtils.builder("oldNode2")
            .roles(MASTER_DATA_ROLES)
            .version(VersionUtils.getPreviousVersion())
            .build();
        AllocationId allocationId1P = AllocationId.newInitializing();
        AllocationId allocationId1R = AllocationId.newInitializing();
        AllocationId allocationId2P = AllocationId.newInitializing();
        AllocationId allocationId2R = AllocationId.newInitializing();
        Metadata metadata = Metadata.builder()
            .put(
                IndexMetadata.builder(shard1.getIndexName())
                    .settings(settings(Version.CURRENT).put(Settings.EMPTY))
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .putInSyncAllocationIds(0, Sets.newHashSet(allocationId1P.getId(), allocationId1R.getId()))
            )
            .put(
                IndexMetadata.builder(shard2.getIndexName())
                    .settings(settings(Version.CURRENT).put(Settings.EMPTY))
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
                            TestShardRouting.newShardRouting(
                                shard1.getIndexName(),
                                shard1.getId(),
                                newNode.getId(),
                                null,
                                true,
                                ShardRoutingState.STARTED,
                                allocationId1P
                            )
                        )
                            .addShard(
                                TestShardRouting.newShardRouting(
                                    shard1.getIndexName(),
                                    shard1.getId(),
                                    oldNode1.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.STARTED,
                                    allocationId1R
                                )
                            )
                    )
            )
            .add(
                IndexRoutingTable.builder(shard2.getIndex())
                    .addIndexShard(
                        new IndexShardRoutingTable.Builder(shard2).addShard(
                            TestShardRouting.newShardRouting(
                                shard2.getIndexName(),
                                shard2.getId(),
                                newNode.getId(),
                                null,
                                true,
                                ShardRoutingState.STARTED,
                                allocationId2P
                            )
                        )
                            .addShard(
                                TestShardRouting.newShardRouting(
                                    shard2.getIndexName(),
                                    shard2.getId(),
                                    oldNode1.getId(),
                                    null,
                                    false,
                                    ShardRoutingState.STARTED,
                                    allocationId2R
                                )
                            )
                    )
            )
            .build();
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(routingTable)
            .nodes(DiscoveryNodes.builder().add(newNode).add(oldNode1).add(oldNode2))
            .build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(Collections.singleton(new NodeVersionAllocationDecider()));
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
            .version(VersionUtils.getPreviousVersion())
            .build();
        final DiscoveryNode oldNode2 = DiscoveryNodeUtils.builder("oldNode2")
            .roles(MASTER_DATA_ROLES)
            .version(VersionUtils.getPreviousVersion())
            .build();

        final Snapshot snapshot = new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID()));
        final IndexId indexId = new IndexId("test", UUIDs.randomBase64UUID(random()));

        final int numberOfShards = randomIntBetween(1, 3);
        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(randomIntBetween(0, 3));
        for (int i = 0; i < numberOfShards; i++) {
            indexMetadata.putInSyncAllocationIds(i, Collections.singleton("_test_"));
        }
        Metadata metadata = Metadata.builder().put(indexMetadata).build();

        final Map<InternalSnapshotsInfoService.SnapshotShard, Long> snapshotShardSizes = new HashMap<>(numberOfShards);
        final Index index = metadata.index("test").getIndex();
        for (int i = 0; i < numberOfShards; i++) {
            final ShardId shardId = new ShardId(index, i);
            snapshotShardSizes.put(new InternalSnapshotsInfoService.SnapshotShard(snapshot, indexId, shardId), randomNonNegativeLong());
        }

        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(metadata)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
                    .addAsRestore(
                        metadata.index("test"),
                        new SnapshotRecoverySource(UUIDs.randomBase64UUID(), snapshot, IndexVersion.current(), indexId)
                    )
                    .build()
            )
            .nodes(DiscoveryNodes.builder().add(newNode).add(oldNode1).add(oldNode2))
            .build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            Arrays.asList(new ReplicaAfterPrimaryActiveAllocationDecider(), new NodeVersionAllocationDecider())
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
        assertRecoveryNodeVersions(routingNodes);

        logger.info("complete rebalancing");
        boolean changed;
        do {
            logger.trace("RoutingNodes: {}", clusterState.getRoutingNodes());
            ClusterState newState = startInitializingShardsAndReroute(service, clusterState);
            changed = newState.equals(clusterState) == false;
            clusterState = newState;
            routingNodes = clusterState.getRoutingNodes();
            assertRecoveryNodeVersions(routingNodes);
        } while (changed);
        return clusterState;
    }

    private void assertRecoveryNodeVersions(RoutingNodes routingNodes) {
        logger.trace("RoutingNodes: {}", routingNodes);

        List<ShardRouting> mutableShardRoutings = shardsWithState(routingNodes, ShardRoutingState.RELOCATING);
        for (ShardRouting r : mutableShardRoutings) {
            if (r.primary()) {
                String toId = r.relocatingNodeId();
                String fromId = r.currentNodeId();
                assertThat(fromId, notNullValue());
                assertThat(toId, notNullValue());
                logger.trace(
                    "From: {} with Version: {} to: {} with Version: {}",
                    fromId,
                    routingNodes.node(fromId).node().getVersion(),
                    toId,
                    routingNodes.node(toId).node().getVersion()
                );
                assertTrue(routingNodes.node(toId).node().getVersion().onOrAfter(routingNodes.node(fromId).node().getVersion()));
            } else {
                ShardRouting primary = routingNodes.activePrimary(r.shardId());
                assertThat(primary, notNullValue());
                String fromId = primary.currentNodeId();
                String toId = r.relocatingNodeId();
                logger.trace(
                    "From: {} with Version: {} to: {} with Version: {}",
                    fromId,
                    routingNodes.node(fromId).node().getVersion(),
                    toId,
                    routingNodes.node(toId).node().getVersion()
                );
                assertTrue(routingNodes.node(toId).node().getVersion().onOrAfter(routingNodes.node(fromId).node().getVersion()));
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
                    "From: {} with Version: {} to: {} with Version: {}",
                    fromId,
                    routingNodes.node(fromId).node().getVersion(),
                    toId,
                    routingNodes.node(toId).node().getVersion()
                );
                assertTrue(routingNodes.node(toId).node().getVersion().onOrAfter(routingNodes.node(fromId).node().getVersion()));
            }
        }
    }

    public void testMessages() {

        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)).numberOfShards(1).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY)
            .addAsNew(metadata.index("test"))
            .build();

        RoutingNode newNode = RoutingNodesHelper.routingNode("newNode", newNode("newNode", Version.CURRENT));
        RoutingNode oldNode = RoutingNodesHelper.routingNode("oldNode", newNode("oldNode", VersionUtils.getPreviousVersion()));

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

        final NodeVersionAllocationDecider allocationDecider = new NodeVersionAllocationDecider();
        Decision decision = allocationDecider.canAllocate(primaryShard, newNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(decision.getExplanation(), is("the primary shard is new or already existed on the node"));

        decision = allocationDecider.canAllocate(ShardRoutingHelper.initialize(primaryShard, "oldNode"), newNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            is(
                "can relocate primary shard from a node with version ["
                    + oldNode.node().getVersion()
                    + "] to a node with equal-or-newer version ["
                    + newNode.node().getVersion()
                    + "]"
            )
        );

        decision = allocationDecider.canAllocate(ShardRoutingHelper.initialize(primaryShard, "newNode"), oldNode, routingAllocation);
        assertThat(decision.type(), is(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            is(
                "cannot relocate primary shard from a node with version ["
                    + newNode.node().getVersion()
                    + "] to a node with older version ["
                    + oldNode.node().getVersion()
                    + "]"
            )
        );

        final IndexId indexId = new IndexId("test", UUIDs.randomBase64UUID(random()));
        final SnapshotRecoverySource newVersionSnapshot = new SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
            newNode.node().getVersion().indexVersion,
            indexId
        );
        final SnapshotRecoverySource oldVersionSnapshot = new SnapshotRecoverySource(
            UUIDs.randomBase64UUID(),
            new Snapshot("rep1", new SnapshotId("snp1", UUIDs.randomBase64UUID())),
            oldNode.node().getVersion().indexVersion,
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
                "node version ["
                    + oldNode.node().getVersion()
                    + "] is older than the snapshot version ["
                    + newNode.node().getVersion()
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
                "node version ["
                    + newNode.node().getVersion()
                    + "] is the same or newer than snapshot version ["
                    + oldNode.node().getVersion()
                    + "]"
            )
        );

        final RoutingChangesObserver routingChangesObserver = new RoutingChangesObserver() {
        };
        final RoutingNodes routingNodes = clusterState.mutableRoutingNodes();
        final ShardRouting startedPrimary = routingNodes.startShard(
            logger,
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
                "cannot allocate replica shard to a node with version ["
                    + oldNode.node().getVersion()
                    + "] since this is older than the primary version ["
                    + newNode.node().getVersion()
                    + "]"
            )
        );

        routingNodes.startShard(
            logger,
            routingNodes.relocateShard(startedPrimary, "oldNode", 0, routingChangesObserver).v2(),
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
                "can allocate replica shard to a node with version ["
                    + newNode.node().getVersion()
                    + "] since this is equal-or-newer than the primary version ["
                    + oldNode.node().getVersion()
                    + "]"
            )
        );
    }
}
