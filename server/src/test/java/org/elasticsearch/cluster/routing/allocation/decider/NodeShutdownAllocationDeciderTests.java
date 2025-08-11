/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.hamcrest.Matchers.equalTo;

public class NodeShutdownAllocationDeciderTests extends ESAllocationTestCase {
    private static final DiscoveryNode DATA_NODE = newNode("node-data", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created")
    );
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private NodeShutdownAllocationDecider decider = new NodeShutdownAllocationDecider();
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(
            decider,
            new SameShardAllocationDecider(Settings.EMPTY, clusterSettings),
            new ReplicaAfterPrimaryActiveAllocationDecider()
        )
    );
    private final AllocationService service = new AllocationService(
        allocationDeciders,
        new TestGatewayAllocator(),
        new BalancedShardsAllocator(Settings.EMPTY),
        EmptyClusterInfoService.INSTANCE,
        EmptySnapshotsInfoService.INSTANCE
    );

    private final String idxName = "test-idx";
    private final String idxUuid = "test-idx-uuid";
    private final IndexMetadata indexMetadata = IndexMetadata.builder(idxName)
        .settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                .put(IndexMetadata.SETTING_INDEX_UUID, idxUuid)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        )
        .build();

    public void testCanAllocateShardsToRestartingNode() {
        ClusterState state = prepareState(SingleNodeShutdownMetadata.Type.RESTART);
        RoutingAllocation allocation = createRoutingAllocation(state);
        RoutingNode routingNode = new RoutingNode(DATA_NODE.getId(), DATA_NODE, shard);

        Decision decision = decider.canAllocate(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + DATA_NODE.getId() + "] is preparing to restart, but will remain in the cluster")
        );
    }

    public void testCannotAllocateShardsToRemovingNode() {
        ClusterState state = prepareState(randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.REPLACE));
        RoutingAllocation allocation = createRoutingAllocation(state);
        RoutingNode routingNode = new RoutingNode(DATA_NODE.getId(), DATA_NODE, shard);

        Decision decision = decider.canAllocate(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is preparing to be removed from the cluster"));
    }

    public void testShardsCanRemainOnRestartingNode() {
        ClusterState state = prepareState(SingleNodeShutdownMetadata.Type.RESTART);
        RoutingAllocation allocation = createRoutingAllocation(state);
        RoutingNode routingNode = new RoutingNode(DATA_NODE.getId(), DATA_NODE, shard);

        Decision decision = decider.canRemain(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + DATA_NODE.getId() + "] is preparing to restart, but will remain in the cluster")
        );
    }

    public void testShardsCannotRemainOnRemovingNode() {
        ClusterState state = prepareState(randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.REPLACE));
        RoutingAllocation allocation = createRoutingAllocation(state);
        RoutingNode routingNode = new RoutingNode(DATA_NODE.getId(), DATA_NODE, shard);

        Decision decision = decider.canRemain(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is preparing to be removed from the cluster"));
    }

    public void testCanAutoExpandToRestartingNode() {
        ClusterState state = prepareState(SingleNodeShutdownMetadata.Type.RESTART);
        RoutingAllocation allocation = createRoutingAllocation(state);

        Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + DATA_NODE.getId() + "] is not preparing for removal from the cluster (is restarting)")
        );
    }

    public void testCanAutoExpandToNodeIfNoNodesShuttingDown() {
        RoutingAllocation allocation = createRoutingAllocation(ClusterState.EMPTY_STATE);

        Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is not preparing for removal from the cluster"));
    }

    public void testCanAutoExpandToNodeThatIsNotShuttingDown() {
        ClusterState state = prepareState(
            randomFrom(SingleNodeShutdownMetadata.Type.REMOVE, SingleNodeShutdownMetadata.Type.REPLACE),
            "other-node-id"
        );
        RoutingAllocation allocation = createRoutingAllocation(state);

        Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is not preparing for removal from the cluster"));
    }

    public void testCannotAutoExpandToRemovingNode() {
        ClusterState state = prepareState(SingleNodeShutdownMetadata.Type.REMOVE);
        RoutingAllocation allocation = createRoutingAllocation(state);

        Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is preparing for removal from the cluster"));
    }

    public void testAutoExpandDuringNodeReplacement() {
        ClusterState state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DATA_NODE).build())
            .metadata(Metadata.builder().put(IndexMetadata.builder(indexMetadata)))
            .build();

        // should auto-expand when no shutdown
        Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, createRoutingAllocation(state));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is not preparing for removal from the cluster"));

        // should auto-expand to source when shutdown/replacement entry is registered and node replacement has not started
        NodesShutdownMetadata shutdown = createNodesShutdownMetadata(SingleNodeShutdownMetadata.Type.REPLACE, DATA_NODE.getId());
        state = ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdown).build())
            .build();
        assertThatDecision(
            decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, createRoutingAllocation(state)),
            Decision.Type.YES,
            "node [" + DATA_NODE.getId() + "] is preparing to be removed from the cluster, but replacement is not yet present"
        );

        // should auto-expand to replacement when node replacement has started
        String replacementName = shutdown.getAllNodeMetadataMap().get(DATA_NODE.getId()).getTargetNodeName();
        DiscoveryNode replacementNode = newNode(replacementName, "node-data-1", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
        state = ClusterState.builder(state).nodes(DiscoveryNodes.builder(state.getNodes()).add(replacementNode).build()).build();

        assertThatDecision(
            decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, createRoutingAllocation(state)),
            Decision.Type.NO,
            "node [" + DATA_NODE.getId() + "] is preparing for removal from the cluster"
        );
        decision = decider.shouldAutoExpandToNode(indexMetadata, replacementNode, createRoutingAllocation(state));
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + replacementNode.getId() + "] is not preparing for removal from the cluster")
        );
    }

    private ClusterState prepareState(SingleNodeShutdownMetadata.Type shutdownType) {
        return prepareState(shutdownType, DATA_NODE.getId());
    }

    private ClusterState prepareState(SingleNodeShutdownMetadata.Type shutdownType, String nodeId) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DATA_NODE).build())
            .metadata(
                Metadata.builder()
                    .put(IndexMetadata.builder(indexMetadata))
                    .putCustom(NodesShutdownMetadata.TYPE, createNodesShutdownMetadata(shutdownType, nodeId))
            )
            .build();
    }

    private NodesShutdownMetadata createNodesShutdownMetadata(SingleNodeShutdownMetadata.Type shutdownType, String nodeId) {
        final String targetNodeName = shutdownType == SingleNodeShutdownMetadata.Type.REPLACE ? randomAlphaOfLengthBetween(10, 20) : null;
        return new NodesShutdownMetadata(new HashMap<>()).putSingleNodeMetadata(
            SingleNodeShutdownMetadata.builder()
                .setNodeId(nodeId)
                .setType(shutdownType)
                .setReason(this.getTestName())
                .setStartedAtMillis(1L)
                .setTargetNodeName(targetNodeName)
                .build()
        );
    }

    private RoutingAllocation createRoutingAllocation(ClusterState state) {
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state.getRoutingNodes(), state, null, null, 0);
        allocation.debugDecision(true);
        return allocation;
    }

    private static void assertThatDecision(Decision decision, Decision.Type type, String explanation) {
        assertThat(decision.type(), equalTo(type));
        assertThat(decision.getExplanation(), equalTo(explanation));
    }
}
