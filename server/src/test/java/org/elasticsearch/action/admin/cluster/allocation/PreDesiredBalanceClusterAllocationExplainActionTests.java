/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.Explanations;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.allocator.PreDesiredBalanceShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.ShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * THESE TESTS SHOULD NOT HAVE FURTHER DEVELOPMENT!
 *
 * Tests for the {@link TransportClusterAllocationExplainAction} class using the {@link PreDesiredBalanceShardsAllocator}.
 * Parallels {@link ClusterAllocationExplainActionTests}, except contains only the tests that instantiate an allocator.
 */
public class PreDesiredBalanceClusterAllocationExplainActionTests extends ESTestCase {

    private static final AllocationDeciders NOOP_DECIDERS = new AllocationDeciders(List.of());

    private static ShardsAllocator createAllocator() {
        return new PreDesiredBalanceShardsAllocator(Settings.EMPTY);
    }

    private static class CanAllocateNotPreferredAllocationDecider extends AllocationDecider {
        @Override
        public Decision canAllocate(
            ShardRouting shardRouting,
            org.elasticsearch.cluster.routing.RoutingNode node,
            RoutingAllocation allocation
        ) {
            return Decision.single(Decision.Type.NOT_PREFERRED, "CanAllocateNotPreferredAllocationDecider", "Test not-preferred");
        }

        @Override
        public Decision canRebalance(RoutingAllocation allocation) {
            return Decision.NO;
        }
    }

    private static class CanRemainNotPreferredAllocationDecider extends AllocationDecider {
        @Override
        public Decision canRemain(
            IndexMetadata indexMetadata,
            ShardRouting shardRouting,
            org.elasticsearch.cluster.routing.RoutingNode node,
            RoutingAllocation allocation
        ) {
            return Decision.single(Decision.Type.NOT_PREFERRED, "CanRemainNotPreferredAllocationDecider", "Test not-preferred");
        }

        @Override
        public Decision canRebalance(RoutingAllocation allocation) {
            return Decision.NO;
        }
    }

    private static class CanRemainNoAllocationDecider extends AllocationDecider {
        @Override
        public Decision canRemain(
            IndexMetadata indexMetadata,
            ShardRouting shardRouting,
            org.elasticsearch.cluster.routing.RoutingNode node,
            RoutingAllocation allocation
        ) {
            return Decision.single(Decision.Type.NO, "CanRemainNoAllocationDecider", "Test no");
        }

        @Override
        public Decision canRebalance(RoutingAllocation allocation) {
            return Decision.NO;
        }
    }

    private static class CanAllocateThrottledAllocationDecider extends AllocationDecider {
        @Override
        public Decision canAllocate(
            ShardRouting shardRouting,
            org.elasticsearch.cluster.routing.RoutingNode node,
            RoutingAllocation allocation
        ) {
            return Decision.single(Decision.Type.THROTTLE, "CanAllocateThrottledAllocationDecider", "Test throttle");
        }

        @Override
        public Decision canRebalance(RoutingAllocation allocation) {
            return Decision.NO;
        }
    }

    public void testNoToNotPreferredShardAllocationExplanation() {
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();
        ShardRouting shardRouting = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        ClusterInfo clusterInfo = ClusterInfo.builder().build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            List.of(new CanRemainNoAllocationDecider(), new CanAllocateNotPreferredAllocationDecider())
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, clusterInfo, null, System.nanoTime());

        ClusterAllocationExplanation allocationExplanation = TransportClusterAllocationExplainAction.explainShard(
            shardRouting,
            allocation,
            clusterInfo,
            randomBoolean(),
            true,
            new AllocationService(
                allocationDeciders,
                new TestGatewayAllocator(),
                createAllocator(),
                EmptyClusterInfoService.INSTANCE,
                EmptySnapshotsInfoService.INSTANCE,
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            )
        );

        assertThat(allocationExplanation.getShardAllocationDecision().getMoveDecision().canRemain(), equalTo(false));
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getCanRemainDecision().type(),
            equalTo(Decision.Type.NO)
        );
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getAllocationDecision(),
            equalTo(AllocationDecision.NOT_PREFERRED)
        );
        for (var nodeDecision : allocationExplanation.getShardAllocationDecision().getMoveDecision().getNodeDecisions()) {
            assertThat(nodeDecision.getNodeDecision(), equalTo(AllocationDecision.NOT_PREFERRED));
        }
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getExplanation(),
            equalTo(Explanations.Move.NOT_PREFERRED)
        );
        assertNotNull(allocationExplanation.getShardAllocationDecision().getMoveDecision().getTargetNode());
    }

    public void testNotPreferredToNotPreferredShardAllocationExplanation() {
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();
        ShardRouting shardRouting = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        ClusterInfo clusterInfo = ClusterInfo.builder().build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            List.of(new CanRemainNotPreferredAllocationDecider(), new CanAllocateNotPreferredAllocationDecider())
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, clusterInfo, null, System.nanoTime());

        ClusterAllocationExplanation allocationExplanation = TransportClusterAllocationExplainAction.explainShard(
            shardRouting,
            allocation,
            clusterInfo,
            randomBoolean(),
            true,
            new AllocationService(
                allocationDeciders,
                new TestGatewayAllocator(),
                createAllocator(),
                EmptyClusterInfoService.INSTANCE,
                EmptySnapshotsInfoService.INSTANCE,
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            )
        );

        assertThat(allocationExplanation.getShardAllocationDecision().getMoveDecision().canRemainNotPreferred(), equalTo(true));
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getCanRemainDecision().type(),
            equalTo(Decision.Type.NOT_PREFERRED)
        );
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getAllocationDecision(),
            equalTo(AllocationDecision.NOT_PREFERRED)
        );
        for (var nodeDecision : allocationExplanation.getShardAllocationDecision().getMoveDecision().getNodeDecisions()) {
            assertThat(nodeDecision.getNodeDecision(), equalTo(AllocationDecision.NOT_PREFERRED));
        }
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getExplanation(),
            equalTo(Explanations.Move.NOT_PREFERRED_TO_NOT_PREFERRED)
        );
        assertNull(allocationExplanation.getShardAllocationDecision().getMoveDecision().getTargetNode());
    }

    public void testNotPreferredToYesShardAllocationExplanation() {
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();
        ShardRouting shardRouting = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        ClusterInfo clusterInfo = ClusterInfo.builder().build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(List.of(new CanRemainNotPreferredAllocationDecider()));
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, clusterInfo, null, System.nanoTime());

        ClusterAllocationExplanation allocationExplanation = TransportClusterAllocationExplainAction.explainShard(
            shardRouting,
            allocation,
            clusterInfo,
            randomBoolean(),
            true,
            new AllocationService(
                allocationDeciders,
                new TestGatewayAllocator(),
                createAllocator(),
                EmptyClusterInfoService.INSTANCE,
                EmptySnapshotsInfoService.INSTANCE,
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            )
        );

        assertThat(allocationExplanation.getShardAllocationDecision().getMoveDecision().canRemainNotPreferred(), equalTo(true));
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getCanRemainDecision().type(),
            equalTo(Decision.Type.NOT_PREFERRED)
        );
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getAllocationDecision(),
            equalTo(AllocationDecision.YES)
        );
        for (var nodeDecision : allocationExplanation.getShardAllocationDecision().getMoveDecision().getNodeDecisions()) {
            assertThat(nodeDecision.getNodeDecision(), equalTo(AllocationDecision.YES));
        }
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getExplanation(),
            equalTo(Explanations.Move.NOT_PREFERRED_TO_YES)
        );
        assertNotNull(allocationExplanation.getShardAllocationDecision().getMoveDecision().getTargetNode());
    }

    public void testNotPreferredToThrottleShardAllocationExplanation() {
        ClusterState clusterState = ClusterStateCreationUtils.state("idx", randomBoolean(), ShardRoutingState.STARTED);
        final ProjectId projectId = clusterState.metadata().projects().keySet().iterator().next();
        ShardRouting shardRouting = clusterState.globalRoutingTable().routingTable(projectId).index("idx").shard(0).primaryShard();
        ClusterInfo clusterInfo = ClusterInfo.builder().build();
        AllocationDeciders allocationDeciders = new AllocationDeciders(
            List.of(new CanRemainNotPreferredAllocationDecider(), new CanAllocateThrottledAllocationDecider())
        );
        RoutingAllocation allocation = new RoutingAllocation(allocationDeciders, clusterState, clusterInfo, null, System.nanoTime());

        ClusterAllocationExplanation allocationExplanation = TransportClusterAllocationExplainAction.explainShard(
            shardRouting,
            allocation,
            clusterInfo,
            randomBoolean(),
            true,
            new AllocationService(
                allocationDeciders,
                new TestGatewayAllocator(),
                createAllocator(),
                EmptyClusterInfoService.INSTANCE,
                EmptySnapshotsInfoService.INSTANCE,
                TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
            )
        );

        assertThat(allocationExplanation.getShardAllocationDecision().getMoveDecision().canRemainNotPreferred(), equalTo(true));
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getCanRemainDecision().type(),
            equalTo(Decision.Type.NOT_PREFERRED)
        );
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getAllocationDecision(),
            equalTo(AllocationDecision.THROTTLED)
        );
        for (var nodeDecision : allocationExplanation.getShardAllocationDecision().getMoveDecision().getNodeDecisions()) {
            assertThat(nodeDecision.getNodeDecision(), equalTo(AllocationDecision.THROTTLED));
        }
        assertThat(
            allocationExplanation.getShardAllocationDecision().getMoveDecision().getExplanation(),
            equalTo(Explanations.Move.NOT_PREFERRED_TO_THROTTLED)
        );
        assertNull(allocationExplanation.getShardAllocationDecision().getMoveDecision().getTargetNode());
    }
}
