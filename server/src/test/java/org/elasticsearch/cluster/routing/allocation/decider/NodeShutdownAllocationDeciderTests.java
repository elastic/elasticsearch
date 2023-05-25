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
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.equalTo;

public class NodeShutdownAllocationDeciderTests extends ESAllocationTestCase {
    private static final DiscoveryNode DATA_NODE = newNode("node-data", Collections.singleton(DiscoveryNodeRole.DATA_ROLE));
    private final ShardRouting shard = ShardRouting.newUnassigned(
        new ShardId("myindex", "myindex", 0),
        true,
        RecoverySource.EmptyStoreRecoverySource.INSTANCE,
        new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"),
        ShardRouting.Role.DEFAULT
    );
    private final ClusterSettings clusterSettings = createBuiltInClusterSettings();
    private final NodeShutdownAllocationDecider decider = new NodeShutdownAllocationDecider();
    private final AllocationDeciders allocationDeciders = new AllocationDeciders(
        Arrays.asList(decider, new SameShardAllocationDecider(clusterSettings), new ReplicaAfterPrimaryActiveAllocationDecider())
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

    private static final List<SingleNodeShutdownMetadata.Type> REMOVE_SHUTDOWN_TYPES = List.of(
        SingleNodeShutdownMetadata.Type.REPLACE,
        SingleNodeShutdownMetadata.Type.REMOVE,
        SingleNodeShutdownMetadata.Type.SIGTERM
    );

    public void testCanAllocateShardsToRestartingNode() {
        ClusterState state = prepareState(
            service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()),
            SingleNodeShutdownMetadata.Type.RESTART
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(DATA_NODE.getId(), DATA_NODE, shard);
        allocation.debugDecision(true);

        Decision decision = decider.canAllocate(shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + DATA_NODE.getId() + "] is preparing to restart, but will remain in the cluster")
        );
    }

    public void testCannotAllocateShardsToRemovingNode() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()), type);
            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
            RoutingNode routingNode = RoutingNodesHelper.routingNode(DATA_NODE.getId(), DATA_NODE, shard);
            allocation.debugDecision(true);

            Decision decision = decider.canAllocate(shard, routingNode, allocation);
            assertThat(type.toString(), decision.type(), equalTo(Decision.Type.NO));
            assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is preparing to be removed from the cluster"));
        }
    }

    public void testShardsCanRemainOnRestartingNode() {
        ClusterState state = prepareState(
            service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()),
            SingleNodeShutdownMetadata.Type.RESTART
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        RoutingNode routingNode = RoutingNodesHelper.routingNode(DATA_NODE.getId(), DATA_NODE, shard);
        allocation.debugDecision(true);

        Decision decision = decider.canRemain(null, shard, routingNode, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + DATA_NODE.getId() + "] is preparing to restart, but will remain in the cluster")
        );
    }

    public void testShardsCannotRemainOnRemovingNode() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()), type);
            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
            RoutingNode routingNode = RoutingNodesHelper.routingNode(DATA_NODE.getId(), DATA_NODE, shard);
            allocation.debugDecision(true);

            Decision decision = decider.canRemain(null, shard, routingNode, allocation);
            assertThat(type.toString(), decision.type(), equalTo(Decision.Type.NO));
            assertThat(
                type.toString(),
                decision.getExplanation(),
                equalTo("node [" + DATA_NODE.getId() + "] is preparing to be removed from the cluster")
            );
        }
    }

    public void testCanAutoExpandToRestartingNode() {
        ClusterState state = prepareState(
            service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()),
            SingleNodeShutdownMetadata.Type.RESTART
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        allocation.debugDecision(true);

        Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo("node [" + DATA_NODE.getId() + "] is preparing to restart, but will remain in the cluster")
        );
    }

    public void testCanAutoExpandToNodeIfNoNodesShuttingDown() {
        ClusterState state = service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop());

        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
        allocation.debugDecision(true);

        Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("no nodes are shutting down"));
    }

    public void testCanAutoExpandToNodeThatIsNotShuttingDown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            ClusterState state = prepareState(
                service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()),
                type,
                "other-node-id"
            );

            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
            allocation.debugDecision(true);

            Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
            assertThat(type.toString(), decision.type(), equalTo(Decision.Type.YES));
            assertThat(type.toString(), decision.getExplanation(), equalTo("this node is not shutting down"));
        }
    }

    public void testCannotAutoExpandToRemovingNode() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            ClusterState state = prepareState(service.reroute(ClusterState.EMPTY_STATE, "initial state", ActionListener.noop()), type);
            RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, state, null, null, 0);
            allocation.debugDecision(true);

            Decision decision = decider.shouldAutoExpandToNode(indexMetadata, DATA_NODE, allocation);
            assertThat(decision.type(), equalTo(Decision.Type.NO));
            assertThat(decision.getExplanation(), equalTo("node [" + DATA_NODE.getId() + "] is preparing to be removed from the cluster"));
        }
    }

    private ClusterState prepareState(ClusterState initialState, SingleNodeShutdownMetadata.Type shutdownType) {
        return prepareState(initialState, shutdownType, DATA_NODE.getId());
    }

    private ClusterState prepareState(ClusterState initialState, SingleNodeShutdownMetadata.Type shutdownType, String nodeId) {
        final String targetNodeName = shutdownType == SingleNodeShutdownMetadata.Type.REPLACE ? randomAlphaOfLengthBetween(10, 20) : null;
        final SingleNodeShutdownMetadata nodeShutdownMetadata = SingleNodeShutdownMetadata.builder()
            .setNodeId(nodeId)
            .setType(shutdownType)
            .setReason(this.getTestName())
            .setStartedAtMillis(1L)
            .setTargetNodeName(targetNodeName)
            .setGracePeriod(
                shutdownType == SingleNodeShutdownMetadata.Type.SIGTERM
                    ? TimeValue.parseTimeValue(randomTimeValue(), this.getTestName())
                    : null
            )
            .build();
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(new HashMap<>()).putSingleNodeMetadata(
            nodeShutdownMetadata
        );
        return ClusterState.builder(initialState)
            .nodes(DiscoveryNodes.builder().add(DATA_NODE).build())
            .metadata(
                Metadata.builder().put(IndexMetadata.builder(indexMetadata)).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata)
            )
            .build();
    }
}
