/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS;
import static org.elasticsearch.cluster.routing.ShardRoutingState.RELOCATING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.equalTo;

public class NodeReplacementAllocationDeciderTests extends ESAllocationTestCase {
    private static final DiscoveryNode NODE_A = newNode("node-a", "node-a", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private static final DiscoveryNode NODE_B = newNode("node-b", "node-b", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private static final DiscoveryNode NODE_C = newNode("node-c", "node-c", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"),
        ShardRouting.Role.DEFAULT
    );
    private final ClusterSettings clusterSettings = createBuiltInClusterSettings();
    private final NodeReplacementAllocationDecider decider = new NodeReplacementAllocationDecider();
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(
            decider,
            new SameShardAllocationDecider(clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider(),
            new NodeShutdownAllocationDecider()
        )
    );
    private final AllocationService service = new AllocationService(
        allocationDeciders,
        new TestGatewayAllocator(),
        new BalancedShardsAllocator(Settings.EMPTY),
        EmptyClusterInfoService.INSTANCE,
        EmptySnapshotsInfoService.INSTANCE,
        TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
    );

    private final String idxName = "test-idx";
    private final String idxUuid = "test-idx-uuid";
    private final IndexMetadata indexMetadata = IndexMetadata.builder(idxName)
        .settings(indexSettings(Version.CURRENT, 1, 0).put(IndexMetadata.SETTING_INDEX_UUID, idxUuid))
        .build();

    public void testNoReplacements() {
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().add(NODE_A).add(NODE_B).add(NODE_C).build())
            .build();

        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        DiscoveryNode node = randomFrom(NODE_A, NODE_B, NODE_C);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(node.getId(), node, shard);
        allocation.debugDecision(true);

        Decision decision = decider.canAllocate(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo(NodeReplacementAllocationDecider.YES__NO_REPLACEMENTS.getExplanation()));

        decision = decider.canRemain(null, shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo(NodeReplacementAllocationDecider.YES__NO_REPLACEMENTS.getExplanation()));
    }

    public void testCanForceAllocate() {
        ClusterState state = prepareState(
            service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()),
            NODE_A.getId(),
            NODE_B.getName()
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(NODE_A.getId(), NODE_A, shard);
        allocation.debugDecision(true);

        ShardRouting assignedShard = ShardRouting.newUnassigned(
            new ShardId("myindex", "myindex", 0),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"),
            ShardRouting.Role.DEFAULT
        );
        assignedShard = assignedShard.initialize(NODE_A.getId(), null, 1);
        assignedShard = assignedShard.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);

        Decision decision = decider.canForceAllocateDuringReplace(assignedShard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            equalTo("shard is not on the source of a node replacement relocated to the replacement target")
        );

        routingNode = RoutingNodesHelper.routingNode(NODE_B.getId(), NODE_B, assignedShard);

        decision = decider.canForceAllocateDuringReplace(assignedShard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo(
                "node [" + NODE_A.getId() + "] is being replaced by node [" + NODE_B.getId() + "], and can be force vacated to the target"
            )
        );

        routingNode = RoutingNodesHelper.routingNode(NODE_C.getId(), NODE_C, assignedShard);

        decision = decider.canForceAllocateDuringReplace(assignedShard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            equalTo("shard is not on the source of a node replacement relocated to the replacement target")
        );
    }

    public void testCannotRemainOnReplacedNode() {
        ClusterState state = prepareState(
            service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()),
            NODE_A.getId(),
            NODE_B.getName()
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(NODE_A.getId(), NODE_A, shard);
        allocation.debugDecision(true);

        Decision decision = decider.canRemain(indexMetadata, shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + NODE_A.getId() + "] is being replaced by node [" + NODE_B.getId() + "], so no data may remain on it")
        );

        routingNode = RoutingNodesHelper.routingNode(NODE_B.getId(), NODE_B, shard);

        decision = decider.canRemain(indexMetadata, shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertEquals(NodeReplacementAllocationDecider.YES__NO_APPLICABLE_REPLACEMENTS, decision);

        routingNode = RoutingNodesHelper.routingNode(NODE_C.getId(), NODE_C, shard);

        decision = decider.canRemain(indexMetadata, shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertEquals(NodeReplacementAllocationDecider.YES__NO_APPLICABLE_REPLACEMENTS, decision);
    }

    public void testCanAllocateToNeitherSourceNorTarget() {
        ClusterState state = prepareState(
            service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()),
            NODE_A.getId(),
            NODE_B.getName()
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(NODE_A.getId(), NODE_A, shard);
        allocation.debugDecision(true);

        ShardRouting testShard = this.shard;
        if (randomBoolean()) {
            testShard = shard.initialize(NODE_C.getId(), null, 1);
            testShard = testShard.moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
        }
        Decision decision = decider.canAllocate(testShard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + NODE_A.getId() + "] is being replaced by [" + NODE_B.getName() + "], so no data may be allocated to it")
        );

        routingNode = RoutingNodesHelper.routingNode(NODE_B.getId(), NODE_B, testShard);

        decision = decider.canAllocate(testShard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            equalTo(
                "node ["
                    + NODE_B.getId()
                    + "] is replacing the vacating node ["
                    + NODE_A.getId()
                    + "], only data currently allocated "
                    + "to the source node may be allocated to it until the replacement is complete"
            )
        );

        routingNode = RoutingNodesHelper.routingNode(NODE_C.getId(), NODE_C, testShard);

        decision = decider.canAllocate(testShard, routingNode, allocation);
        assertThat(decision.getExplanation(), decision.type(), equalTo(Decision.Type.YES));
        assertEquals(NodeReplacementAllocationDecider.YES__NO_APPLICABLE_REPLACEMENTS, decision);
    }

    public void testShouldNotContractAutoExpandReplicasDuringNodeReplacement() {

        var indexMetadata = IndexMetadata.builder(idxName)
            .settings(indexSettings(Version.CURRENT, 1, 0).put(SETTING_AUTO_EXPAND_REPLICAS, "0-1"))
            .build();
        var shardId = new ShardId(indexMetadata.getIndex(), 0);

        var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(NODE_A).add(NODE_C).build())
            .metadata(Metadata.builder().put(IndexMetadata.builder(indexMetadata)))
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(indexMetadata.getIndex())
                            .addShard(newShardRouting(shardId, NODE_A.getId(), true, STARTED))
                            .addShard(newShardRouting(shardId, NODE_C.getId(), false, STARTED))
                            .build()
                    )
                    .build()
            )
            .build();

        var allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);

        // index is already allocated on both nodes
        assertThat(
            decider.shouldAutoExpandToNode(indexMetadata, NODE_A, allocation),
            equalTo(NodeReplacementAllocationDecider.YES__NO_REPLACEMENTS)
        );
        assertThat(
            decider.shouldAutoExpandToNode(indexMetadata, NODE_C, allocation),
            equalTo(NodeReplacementAllocationDecider.YES__NO_REPLACEMENTS)
        );

        // when replacing NODE_A with NODE_B
        state = ClusterState.builder(state)
            .nodes(DiscoveryNodes.builder().add(NODE_A).add(NODE_B).add(NODE_C).build())
            .metadata(
                Metadata.builder(state.metadata())
                    .putCustom(NodesShutdownMetadata.TYPE, createNodeShutdownReplacementMetadata(NODE_A.getId(), NODE_B.getName()))
                    .build()
            )
            .build();
        allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);

        // index should not contract
        assertThatDecision(
            decider.canAllocate(
                allocation.routingNodes().node(NODE_A.getId()).getByShardId(shardId),
                allocation.routingNodes().node(NODE_B.getId()),
                allocation
            ),
            Decision.Type.YES,
            Strings.format("node [%s] is replacing node [%s], and may receive shards from it", NODE_B.getId(), NODE_A.getId())
        );
        assertThatAutoExpandReplicasDidNotContract(indexMetadata, allocation);

        // when index is relocating
        state = ClusterState.builder(state)
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(indexMetadata.getIndex())
                            .addShard(newShardRouting(shardId, NODE_A.getId(), NODE_B.getId(), true, RELOCATING))
                            .addShard(newShardRouting(shardId, NODE_C.getId(), false, STARTED))
                            .build()
                    )
                    .build()
            )
            .build();
        allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        assertThatAutoExpandReplicasDidNotContract(indexMetadata, allocation);

        // when index is relocated
        state = ClusterState.builder(state)
            .routingTable(
                RoutingTable.builder()
                    .add(
                        IndexRoutingTable.builder(indexMetadata.getIndex())
                            .addShard(newShardRouting(shardId, NODE_B.getId(), true, STARTED))
                            .addShard(newShardRouting(shardId, NODE_C.getId(), false, STARTED))
                            .build()
                    )
                    .build()
            )
            .build();
        allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        assertThatAutoExpandReplicasDidNotContract(indexMetadata, allocation);
    }

    private void assertThatAutoExpandReplicasDidNotContract(IndexMetadata indexMetadata, RoutingAllocation allocation) {
        assertThat(indexMetadata.getAutoExpandReplicas().getDesiredNumberOfReplicas(indexMetadata, allocation), equalTo(1));
        assertThatDecision(
            decider.shouldAutoExpandToNode(indexMetadata, NODE_A, allocation),
            Decision.Type.NO,
            "node [node-a] is being replaced by [node-b], shards cannot auto expand to be on it"
        );
        assertThatDecision(
            decider.shouldAutoExpandToNode(indexMetadata, NODE_B, allocation),
            Decision.Type.YES,
            "node [node-b] is a node replacement target for node [node-a], "
                + "shards can auto expand to it as they were already present of the source node"
        );
        assertThatDecision(
            decider.shouldAutoExpandToNode(indexMetadata, NODE_C, allocation),
            Decision.Type.YES,
            "none of the ongoing node replacements relate to the allocation of this shard"
        );
    }

    private NodesShutdownMetadata createNodeShutdownReplacementMetadata(String sourceNodeId, String targetNodeName) {
        return new NodesShutdownMetadata(new HashMap<>()).putSingleNodeMetadata(
            SingleNodeShutdownMetadata.builder()
                .setNodeId(sourceNodeId)
                .setTargetNodeName(targetNodeName)
                .setType(SingleNodeShutdownMetadata.Type.REPLACE)
                .setReason(this.getTestName())
                .setStartedAtMillis(1L)
                .build()
        );
    }

    private ClusterState prepareState(ClusterState clusterState, String sourceNodeId, String targetNodeName) {
        return ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(NODE_A).add(NODE_B).add(NODE_C).build())
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder(indexMetadata))
                    .putCustom(NodesShutdownMetadata.TYPE, createNodeShutdownReplacementMetadata(sourceNodeId, targetNodeName))
            )
            .build();
    }

    private static void assertThatDecision(Decision decision, Decision.Type type, String explanation) {
        assertThat(decision.type(), equalTo(type));
        assertThat(decision.getExplanation(), equalTo(explanation));
    }
}
