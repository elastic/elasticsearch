/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.index.shard.ShardId;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

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
        ClusterState state = prepareState(ClusterState.EMPTY_STATE, NODE_A.getId(), NODE_B.getName());
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
        ClusterState state = prepareState(ClusterState.EMPTY_STATE, NODE_A.getId(), NODE_B.getName());
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
        ClusterState state = prepareState(ClusterState.EMPTY_STATE, NODE_A.getId(), NODE_B.getName());
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

    private ClusterState prepareState(ClusterState initialState, String sourceNodeId, String targetNodeName) {
        final SingleNodeShutdownMetadata nodeShutdownMetadata = SingleNodeShutdownMetadata.builder()
            .setNodeId(sourceNodeId)
            .setTargetNodeName(targetNodeName)
            .setType(SingleNodeShutdownMetadata.Type.REPLACE)
            .setReason(this.getTestName())
            .setStartedAtMillis(1L)
            .build();
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(new HashMap<>()).putSingleNodeMetadata(
            nodeShutdownMetadata
        );
        return ClusterState.builder(initialState)
            .nodes(DiscoveryNodes.builder().add(NODE_A).add(NODE_B).add(NODE_C).build())
            .metadata(
                Metadata.builder().put(IndexMetadata.builder(indexMetadata)).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata)
            )
            .build();
    }
}
