/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.ShardAllocationDecision;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class EstimatedHeapUsageAllocationDeciderTests extends ESAllocationTestCase {

    static final String NODE_ID = "node-id";
    static final String OTHER_NODE_ID = "not-" + NODE_ID;
    static final String SEARCH_NODE_ID = "search-node";

    public void testYesDecisionWhenCanRemainDisabled() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, false, between(0, 100), between(0, 100), ByteSizeValue.ZERO);

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeAndShardHeap(Map.of(NODE_ID, randomLongBetween(0, 100)), shardRouting.shardId())
        );

        final Decision canRemainDecision = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            routingAllocation.routingNodes().node(NODE_ID),
            routingAllocation
        );
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
        assertThat(canRemainDecision.getExplanation(), equalTo("heap decider can remain disabled"));
    }

    public void testYesDecisionWhenDisabled() {
        final var decider = createEstimatedHeapUsageAllocationDecider(false, between(0, 100), between(0, 100));

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeAndShardHeap(Map.of(NODE_ID, randomLongBetween(0, 100)), shardRouting.shardId())
        );

        final Decision canAllocateDecision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(NODE_ID),
            routingAllocation
        );
        assertThat(canAllocateDecision.type(), equalTo(Decision.Type.YES));
        assertThat(canAllocateDecision.getExplanation(), equalTo("estimated heap allocation decider is disabled"));

        final Decision canRemainDecision = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            routingAllocation.routingNodes().node(NODE_ID),
            routingAllocation
        );
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
        assertThat(canRemainDecision.getExplanation(), equalTo("estimated heap allocation decider is disabled"));
    }

    public void testYesDecisionWhenNodeIsNotIndexNode() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, between(0, 100), between(0, 100));

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeAndShardHeap(Map.of(NODE_ID, randomLongBetween(0, 100)), shardRouting.shardId())
        );
        final var searchNode = routingAllocation.routingNodes().node(SEARCH_NODE_ID);

        final Decision canAllocateDecision = decider.canAllocate(shardRouting, searchNode, routingAllocation);
        assertThat(canAllocateDecision.type(), equalTo(Decision.Type.YES));
        assertThat(canAllocateDecision.getExplanation(), equalTo("estimated heap allocation decider is applicable only to index nodes"));

        final Decision canRemainDecision = decider.canRemain(mock(IndexMetadata.class), shardRouting, searchNode, routingAllocation);
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
        assertThat(canRemainDecision.getExplanation(), equalTo("estimated heap allocation decider is applicable only to index nodes"));
    }

    public void testYesDecisionWhenUsageMetricIsMissing() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, between(0, 100), between(0, 100));
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeAndShardHeap(Map.of(NODE_ID, randomLongBetween(0, 100)), shardRouting.shardId())
        );
        final var noHeapNode = routingAllocation.routingNodes().node(OTHER_NODE_ID);

        final Decision canAllocateDecision = decider.canAllocate(shardRouting, noHeapNode, routingAllocation);
        assertThat(canAllocateDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            canAllocateDecision.getExplanation(),
            containsString("no estimated heap estimation available for node [" + OTHER_NODE_ID + "]")
        );

        final Decision canRemainDecision = decider.canRemain(mock(IndexMetadata.class), shardRouting, noHeapNode, routingAllocation);
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            canRemainDecision.getExplanation(),
            containsString("no estimated heap estimation available for node [" + OTHER_NODE_ID + "]")
        );
    }

    public void testDecisionWhenShardHeapUsageMetricIsMissing() {
        final int watermarkPercent = between(20, 80); // represents low and high watermark
        final var decider = createEstimatedHeapUsageAllocationDecider(true, watermarkPercent, watermarkPercent);
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeHeapOnly(Map.of(NODE_ID, watermarkPercent + 1L, OTHER_NODE_ID, watermarkPercent - 1L))
        );
        final var highHeapNode = routingAllocation.routingNodes().node(NODE_ID);
        final var lowHeapNode = routingAllocation.routingNodes().node(OTHER_NODE_ID);

        // A node with heap above the low watermark should return NO regardless of an absent shard-level heap estimate.
        final Decision highHeapCanAllocateDecision = decider.canAllocate(shardRouting, highHeapNode, routingAllocation);
        assertThat(highHeapCanAllocateDecision.type(), equalTo(Decision.Type.NO));
        assertThat(
            highHeapCanAllocateDecision.getExplanation(),
            containsString("insufficient estimated heap available on node [" + NODE_ID + "]")
        );

        final Decision highHeapCanRemainDecision = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            highHeapNode,
            routingAllocation
        );
        assertThat(highHeapCanRemainDecision.type(), equalTo(Decision.Type.NO));
        assertThat(
            highHeapCanRemainDecision.getExplanation(),
            containsString("insufficient estimated heap available on node [" + NODE_ID + "]")
        );

        // A node with heap below the low watermark should return YES regardless of an absent shard-level heap estimate.
        final Decision lowHeapCanAllocateDecision = decider.canAllocate(shardRouting, lowHeapNode, routingAllocation);
        assertThat(lowHeapCanAllocateDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            lowHeapCanAllocateDecision.getExplanation(),
            containsString("sufficient estimated heap available on node [" + OTHER_NODE_ID + "]")
        );

        final Decision lowHeapCanRemainDecision = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            lowHeapNode,
            routingAllocation
        );
        assertThat(lowHeapCanRemainDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            lowHeapCanRemainDecision.getExplanation(),
            containsString("sufficient estimated heap available on node [" + OTHER_NODE_ID + "]")
        );
    }

    public void testYesDecisionWhenUsageBelowWatermark() {
        final int watermark = 85;
        final var decider = createEstimatedHeapUsageAllocationDecider(true, watermark, watermark);

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeAndShardHeap(
                Map.of(NODE_ID, randomLongBetween(0, watermark - 1 /* keep under the watermark, shard will have a very small addition */)),
                shardRouting.shardId()
            )
        );
        var routingNode = routingAllocation.routingNodes().node(NODE_ID);

        final Decision canAllocateDecision = decider.canAllocate(shardRouting, routingNode, routingAllocation);
        assertThat(canAllocateDecision.type(), equalTo(Decision.Type.YES));
        assertThat(canAllocateDecision.getExplanation(), containsString("sufficient estimated heap available on node [" + NODE_ID + "]"));

        final Decision canRemainDecision = decider.canRemain(mock(IndexMetadata.class), shardRouting, routingNode, routingAllocation);
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
        assertThat(canRemainDecision.getExplanation(), containsString("sufficient estimated heap available on node [" + NODE_ID + "]"));
    }

    public void testNoDecisionWhenUsageAboveWatermark() {
        final int lowWatermark = between(40, 80);
        final int highWatermark = between(40, 80);
        final var decider = createEstimatedHeapUsageAllocationDecider(true, lowWatermark, highWatermark);

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeAndShardHeap(
                Map.of(NODE_ID, randomLongBetween(lowWatermark + 1, 100), OTHER_NODE_ID, randomLongBetween(highWatermark + 1, 100)),
                shardRouting.shardId()
            )
        );
        var routingNode = routingAllocation.routingNodes().node(NODE_ID);
        var otherRoutingNode = routingAllocation.routingNodes().node(OTHER_NODE_ID);

        final Decision canAllocateDecision = decider.canAllocate(shardRouting, routingNode, routingAllocation);
        assertThat(canAllocateDecision.toString(), canAllocateDecision.type(), equalTo(Decision.Type.NO));
        assertThat(canAllocateDecision.getExplanation(), containsString("insufficient estimated heap available on node [" + NODE_ID + "]"));

        final Decision canRemainDecision = decider.canRemain(mock(IndexMetadata.class), shardRouting, otherRoutingNode, routingAllocation);
        assertThat(canRemainDecision.toString(), canRemainDecision.type(), equalTo(Decision.Type.NO));
        assertThat(
            canRemainDecision.getExplanation(),
            containsString("insufficient estimated heap available on node [" + OTHER_NODE_ID + "]")
        );
    }

    /**
     * When shard heap usage is available and adding the shard would push the node over the low watermark, canAllocate returns NO.
     */
    public void testCanAllocateNoWhenShardWouldExceedWatermark() {
        final int watermarkPercent = 85;
        final var decider = createEstimatedHeapUsageAllocationDecider(true, watermarkPercent, watermarkPercent);

        final ByteSizeValue totalHeap = ByteSizeValue.ofGb(10);
        final long additionalBytes = (long) (totalHeap.getBytes() * 0.10);
        final ShardRouting shardRouting = createShardRouting();

        // Set the node to 80% heap used, and add shard+index heap that would push the node to 90%. The low watermark percent is 85%.
        final ClusterInfo clusterInfo = createClusterInfoWithHeapUsage(
            Map.of(NODE_ID, createNodeHeapUsage(NODE_ID, 80, totalHeap)),
            createShardAndIndexHeapUsageMap(shardRouting.shardId(), additionalBytes)
        );

        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);
        final Decision decision = decider.canAllocate(shardRouting, routingAllocation.routingNodes().node(NODE_ID), routingAllocation);
        assertThat(decision.toString(), decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.getExplanation(), containsString("insufficient estimated heap available on node [" + NODE_ID + "]"));
    }

    /**
     * When shard heap usage is available and adding the shard would keep the node below the low watermark, canAllocate returns YES.
     */
    public void testCanAllocateYesWhenShardWouldStayBelowWatermark() {
        final int lowWatermarkPercent = 85;
        final var decider = createEstimatedHeapUsageAllocationDecider(true, lowWatermarkPercent, lowWatermarkPercent);

        final ByteSizeValue totalHeap = ByteSizeValue.ofGb(10);
        final long additionalBytes = (long) (totalHeap.getBytes() * 0.03);
        final ShardRouting shardRouting = createShardRouting();

        // Set the node to 80% heap used, and add shard+index heap that would push the node to 83%. The low watermark percent is 85%.
        final ClusterInfo clusterInfo = createClusterInfoWithHeapUsage(
            Map.of(NODE_ID, createNodeHeapUsage(NODE_ID, 80, totalHeap)),
            createShardAndIndexHeapUsageMap(shardRouting.shardId(), additionalBytes)
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);
        final Decision decision = decider.canAllocate(shardRouting, routingAllocation.routingNodes().node(NODE_ID), routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("sufficient estimated heap available on node [" + NODE_ID + "]"));
    }

    public void testYesDecisionWhenNodeHeapIsBelowMinimumThreshold() {
        final int minimumHeapSizeForEnablementInGigabytes = between(2, 32);
        final var decider = createEstimatedHeapUsageAllocationDecider(
            true,
            between(0, 100),
            between(0, 100),
            ByteSizeValue.ofGb(minimumHeapSizeForEnablementInGigabytes)
        );

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            shardRouting,
            createClusterInfoWithGenNodeAndShardHeap(
                Map.of(NODE_ID, randomLongBetween(0, 100), OTHER_NODE_ID, randomLongBetween(0, 100)),
                () -> ByteSizeValue.ofGb(between(1, minimumHeapSizeForEnablementInGigabytes - 1)),
                createShardAndIndexHeapUsageMap(shardRouting.shardId(), randomLongBetween(1, 100))
            )
        );
        var routingNode = routingAllocation.routingNodes().node(NODE_ID);
        var otherRoutingNode = routingAllocation.routingNodes().node(OTHER_NODE_ID);

        final Decision canAllocateDecision = decider.canAllocate(shardRouting, routingNode, routingAllocation);
        assertThat(canAllocateDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            canAllocateDecision.getExplanation(),
            equalTo(
                Strings.format(
                    "estimated heap decider will not intervene if heap size is below [%s]",
                    ByteSizeValue.ofGb(minimumHeapSizeForEnablementInGigabytes)
                )
            )
        );

        final Decision canRemainDecision = decider.canRemain(mock(IndexMetadata.class), shardRouting, otherRoutingNode, routingAllocation);
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            canRemainDecision.getExplanation(),
            equalTo(
                Strings.format(
                    "estimated heap decider will not intervene if heap size is below [%s]",
                    ByteSizeValue.ofGb(minimumHeapSizeForEnablementInGigabytes)
                )
            )
        );
    }

    public void testAllocationExplain() {
        final int watermark = 85;
        final var decider = createEstimatedHeapUsageAllocationDecider(true, watermark, watermark);

        final ShardRouting shardRouting = createShardRouting();

        final ClusterInfo clusterInfo = createClusterInfoWithGenNodeAndShardHeap(
            Map.of(
                NODE_ID,
                randomLongBetween(0, watermark - 1 /* keep under the watermark, shard will have a very small addition */),
                OTHER_NODE_ID,
                randomLongBetween(watermark + 1, 100)
            ),
            shardRouting.shardId()
        );
        final var routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);
        final var allocationService = createAllocationService(decider, clusterInfo);

        final var shardAllocationDecision = allocationService.explainShardAllocation(shardRouting, routingAllocation);
        final var nodeDecisions = shardAllocationDecision.getAllocateDecision().getNodeDecisions();
        assertNotNull(nodeDecisions);
        assertThat(nodeDecisions.size(), equalTo(3));

        final String explanation = Strings.collectionToDelimitedString(nodeDecisions.stream().map(Strings::toString).toList(), "\n");
        assertExplanationResult(
            explanation,
            nodeDecisions,
            Decision.Type.YES,
            "sufficient estimated heap available on node [" + NODE_ID + "]"
        );
        assertExplanationResult(
            explanation,
            nodeDecisions,
            Decision.Type.NO,
            "insufficient estimated heap available on node [" + OTHER_NODE_ID + "]"
        );
        assertExplanationResult(
            explanation,
            nodeDecisions,
            Decision.Type.YES,
            "estimated heap allocation decider is applicable only to index nodes"
        );
    }

    private static void assertExplanationResult(
        String debugExplanation,
        List<NodeAllocationResult> nodeDecisions,
        Decision.Type expectedDecision,
        String expectedMessage
    ) {
        assertTrue(
            debugExplanation,
            nodeDecisions.stream()
                .anyMatch(
                    nodeDecision -> nodeDecision.getCanAllocateDecision()
                        .getDecisions()
                        .stream()
                        .anyMatch(
                            decision -> decision.type().equals(expectedDecision) && decision.getExplanation().startsWith(expectedMessage)
                        )
                )
        );
    }

    /**
     * Tests the canRemain response of the allocation explain API for a shard that is already assigned. Makes separate
     * {@link AllocationService#explainShardAllocation} calls for: shard on node above high watermark, on node below high watermark, and on
     * a search node (decider does not apply).
     */
    public void testAllocationExplainCanRemain() {
        final int lowWatermarkPercent = 85;
        final int highWatermarkPercent = 90;
        final var decider = createEstimatedHeapUsageAllocationDecider(true, lowWatermarkPercent, highWatermarkPercent);

        // 1. Shard on index node above high watermark -> canRemain should be NO
        {
            final ClusterState clusterStateWithStartedShard = createClusterStateWithStartedShardOnNode(NODE_ID);
            final ShardRouting shardRouting = clusterStateWithStartedShard.routingTable(ProjectId.DEFAULT)
                .index("test-idx")
                .shard(0)
                .primaryShard();
            final ClusterInfo clusterInfo = createClusterInfoWithGenNodeAndShardHeap(
                Map.of(NODE_ID, highWatermarkPercent + 5L, OTHER_NODE_ID, highWatermarkPercent / 2L),
                shardRouting.shardId()
            );
            final RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(clusterStateWithStartedShard)
                .allocationDeciders(createAllocationDeciders(decider))
                .clusterInfo(clusterInfo)
                .build();
            allocation.debugDecision(true);

            final ShardAllocationDecision explainDecision = createAllocationService(decider, clusterInfo).explainShardAllocation(
                shardRouting,
                allocation
            );
            assertTrue("move decision should be taken for started shard", explainDecision.getMoveDecision().isDecisionTaken());
            final Decision canRemainDecision = explainDecision.getMoveDecision().getCanRemainDecision();
            assertThat(canRemainDecision.type(), equalTo(Decision.Type.NO));
            assertCanRemainResults(
                canRemainDecision.getDecisions().toString(),
                canRemainDecision.getDecisions(),
                Decision.Type.NO,
                "insufficient estimated heap available on node [" + NODE_ID + "]"
            );
        }

        // 2. Shard on index node below high watermark -> canRemain should be YES
        {
            final ClusterState clusterStateWithStartedShard = createClusterStateWithStartedShardOnNode(NODE_ID);
            final ShardRouting shardRouting = clusterStateWithStartedShard.routingTable(ProjectId.DEFAULT)
                .index("test-idx")
                .shard(0)
                .primaryShard();
            final ClusterInfo clusterInfo = createClusterInfoWithGenNodeAndShardHeap(
                Map.of(NODE_ID, highWatermarkPercent - 5L, OTHER_NODE_ID, highWatermarkPercent / 2L),
                shardRouting.shardId()
            );
            final RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(clusterStateWithStartedShard)
                .allocationDeciders(createAllocationDeciders(decider))
                .clusterInfo(clusterInfo)
                .build();
            allocation.debugDecision(true);

            final ShardAllocationDecision explainDecision = createAllocationService(decider, clusterInfo).explainShardAllocation(
                shardRouting,
                allocation
            );
            assertTrue("move decision should be taken for started shard", explainDecision.getMoveDecision().isDecisionTaken());
            final Decision canRemainDecision = explainDecision.getMoveDecision().getCanRemainDecision();
            assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
            assertCanRemainResults(
                canRemainDecision.getDecisions().toString(),
                canRemainDecision.getDecisions(),
                Decision.Type.YES,
                "sufficient estimated heap available on node [" + NODE_ID + "]"
            );
        }

        // 3. Shard on search node -> canRemain should be YES (decider applicable only to index nodes)
        {
            final ClusterState clusterStateSearchShardStarted = createClusterStateWithStartedShardOnNode(SEARCH_NODE_ID);
            final ShardRouting shardRouting = clusterStateSearchShardStarted.routingTable(ProjectId.DEFAULT)
                .index("test-idx")
                .shard(0)
                .primaryShard();
            final ClusterInfo clusterInfo = createClusterInfoWithGenNodeAndShardHeap(
                Map.of(
                    NODE_ID,
                    highWatermarkPercent / 2L,
                    OTHER_NODE_ID,
                    highWatermarkPercent / 2L,
                    SEARCH_NODE_ID,
                    highWatermarkPercent + 5L
                ),
                shardRouting.shardId()
            );
            final RoutingAllocation allocation = TestRoutingAllocationFactory.forClusterState(clusterStateSearchShardStarted)
                .allocationDeciders(createAllocationDeciders(decider))
                .clusterInfo(clusterInfo)
                .build();
            allocation.debugDecision(true);

            final ShardAllocationDecision explainDecision = createAllocationService(decider, clusterInfo).explainShardAllocation(
                shardRouting,
                allocation
            );
            assertTrue("move decision should be taken for started shard", explainDecision.getMoveDecision().isDecisionTaken());
            final Decision canRemainDecision = explainDecision.getMoveDecision().getCanRemainDecision();
            assertThat(canRemainDecision.getDecisions().toString(), canRemainDecision.type(), equalTo(Decision.Type.NO));
            assertCanRemainResults(
                canRemainDecision.getDecisions().toString(),
                canRemainDecision.getDecisions(),
                Decision.Type.YES,
                "estimated heap allocation decider is applicable only to index nodes"
            );
        }
    }

    private static void assertCanRemainResults(
        String debugExplanation,
        List<Decision> nodeDecisions,
        Decision.Type expectedDecision,
        String expectedMessagePrefix
    ) {
        assertTrue(
            debugExplanation,
            nodeDecisions.stream()
                .anyMatch(
                    decision -> decision.type().equals(expectedDecision) && decision.getExplanation().startsWith(expectedMessagePrefix)
                )
        );
    }

    public void testAllocationBasedOnEstimatedHeapUsage() {
        final ShardRouting shardRouting = createShardRouting();

        final int watermark = 85;
        final var decider = createEstimatedHeapUsageAllocationDecider(true, watermark, watermark);
        final ClusterState initialState = createClusterState(shardRouting);
        final var allocationService = createAllocationService(
            decider,
            createClusterInfoWithGenNodeAndShardHeap(
                // max watermark-1 percent, otherwise any extra heap usage for shards pushes the total heap usage over the limit
                // (this test uses max 100 bytes heap usage for shards and a minimum of 1GB heap (so 100 bytes < 1% * 1GB))
                Map.of(NODE_ID, randomLongBetween(0, watermark - 1), OTHER_NODE_ID, randomLongBetween(watermark + 1, 100)),
                shardRouting.shardId()
            )
        );

        final var newState = applyStartedShardsUntilNoChange(initialState, allocationService);

        assertFalse(newState.getRoutingNodes().toString(), newState.getRoutingNodes().hasUnassignedShards());
        final var indexRoutingTable = newState.routingTable(ProjectId.DEFAULT).index(shardRouting.index());
        indexRoutingTable.allShards().forEach(indexShardRoutingTable -> {
            final var primaryShard = indexShardRoutingTable.primaryShard();
            assertNotNull(primaryShard);
            assertThat(primaryShard.currentNodeId(), equalTo(NODE_ID));
        });
    }

    private static EstimatedHeapUsageAllocationDecider createEstimatedHeapUsageAllocationDecider(
        boolean enabled,
        int lowWatermarkPercent,
        int highWatermarkPercent
    ) {
        return createEstimatedHeapUsageAllocationDecider(enabled, true, lowWatermarkPercent, highWatermarkPercent, ByteSizeValue.ZERO);
    }

    private static EstimatedHeapUsageAllocationDecider createEstimatedHeapUsageAllocationDecider(
        boolean enabled,
        int lowWatermarkPercent,
        int highWatermarkPercent,
        ByteSizeValue minimumHeapSizeForEnabled
    ) {
        return createEstimatedHeapUsageAllocationDecider(
            enabled,
            true,
            lowWatermarkPercent,
            highWatermarkPercent,
            minimumHeapSizeForEnabled
        );
    }

    private static EstimatedHeapUsageAllocationDecider createEstimatedHeapUsageAllocationDecider(
        boolean enabled,
        boolean highWatermarkEnabled,
        int lowWatermarkPercent,
        int highWatermarkPercent,
        ByteSizeValue minimumHeapSizeForEnabled
    ) {
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), minimumHeapSizeForEnabled)
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), enabled)
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED.getKey(),
                    highWatermarkEnabled
                )
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK.getKey(),
                    lowWatermarkPercent + "%"
                )
                .put(
                    EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK.getKey(),
                    highWatermarkPercent + "%"
                )
                .build(),
            Set.of(
                InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
                EstimatedHeapUsageAllocationDecider.MINIMUM_LOGGING_INTERVAL,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
                EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT
            )
        );
        return new EstimatedHeapUsageAllocationDecider(clusterSettings);
    }

    private static ShardRouting createShardRouting() {
        return ShardRouting.newUnassigned(
            new ShardId(randomIdentifier(), IndexMetadata.INDEX_UUID_NA_VALUE, between(0, 2)),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            TestShardRouting.buildUnassignedInfo("auto generated for test"),
            ShardRouting.Role.INDEX_ONLY
        );
    }

    private RoutingAllocation createRoutingAllocation(AllocationDecider decider, ShardRouting shardRouting, ClusterInfo clusterInfo) {
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(createClusterState(shardRouting))
            .allocationDeciders(createAllocationDeciders(decider))
            .clusterInfo(clusterInfo)
            .build();
        routingAllocation.debugDecision(true);
        return routingAllocation;
    }

    private AllocationService createAllocationService(AllocationDecider decider, ClusterInfo clusterInfo) {
        return new AllocationService(
            createAllocationDeciders(decider),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(Settings.EMPTY),
            () -> clusterInfo,
            EmptySnapshotsInfoService.INSTANCE,
            new StatelessShardRoutingRoleStrategy()
        );
    }

    /**
     * Generates node-level heap usage stats with the given percent used, and randomly generated (per node) max total heap in GB.
     * Also generates a small heap usage, in bytes, for the given shard.
     * Returns a ClusterInfo with the generated heap usage.
     */
    private ClusterInfo createClusterInfoWithGenNodeAndShardHeap(Map<String, Long> nodeEstimatedHeapUsagePercent, ShardId shardId) {
        return createClusterInfoWithGenNodeAndShardHeap(
            nodeEstimatedHeapUsagePercent,
            () -> ByteSizeValue.ofGb(between(1, 32)),
            createShardAndIndexHeapUsageMap(shardId, randomLongBetween(1, 100) /* num bytes */)
        );
    }

    private ClusterInfo createClusterInfoWithGenNodeHeapOnly(Map<String, Long> nodeEstimatedHeapUsagePercent) {
        return createClusterInfoWithGenNodeAndShardHeap(nodeEstimatedHeapUsagePercent, () -> ByteSizeValue.ofGb(between(1, 32)), Map.of());
    }

    private ClusterInfo createClusterInfoWithGenNodeAndShardHeap(
        Map<String, Long> nodeEstimatedHeapUsagePercent,
        Supplier<ByteSizeValue> totalHeapSizeSupplier,
        Map<ShardId, ShardAndIndexHeapUsage> shardHeapUsages
    ) {
        return ClusterInfo.builder()
            .estimatedHeapUsages(
                nodeEstimatedHeapUsagePercent.entrySet()
                    .stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> createNodeHeapUsage(entry.getKey(), entry.getValue(), totalHeapSizeSupplier.get())
                        )
                    )
            )
            .estimatedShardHeapUsages(shardHeapUsages)
            .build();
    }

    private ClusterInfo createClusterInfoWithHeapUsage(
        Map<String, EstimatedHeapUsage> nodeHeapUsages,
        Map<ShardId, ShardAndIndexHeapUsage> shardHeapUsages
    ) {
        return ClusterInfo.builder().estimatedHeapUsages(nodeHeapUsages).estimatedShardHeapUsages(shardHeapUsages).build();
    }

    private static AllocationDeciders createAllocationDeciders(AllocationDecider decider) {
        return new AllocationDeciders(
            Set.of(
                decider,
                new ReplicaAfterPrimaryActiveAllocationDecider(),
                new SameShardAllocationDecider(new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)),
                new StatelessAllocationDecider()
            )
        );
    }

    private static ClusterState createClusterState(ShardRouting shardRouting) {
        final var projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(
                IndexMetadata.builder(shardRouting.getIndexName())
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(between(shardRouting.id() + 1, shardRouting.id() + 3))
                    .numberOfReplicas(0)
            )
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder())
            .putProjectMetadata(projectMetadata)
            .putRoutingTable(
                ProjectId.DEFAULT,
                RoutingTable.builder(new StatelessShardRoutingRoleStrategy())
                    .addAsNew(projectMetadata.index(shardRouting.getIndexName()))
                    .build()
            )
            .build();
    }

    /**
     * Builds a cluster state with a single index "test-idx" and one shard in STARTED state on the given node.
     *
     * @param shardCurrentNodeId the node the shard is assigned to (e.g. NODE_ID, OTHER_NODE_ID, or SEARCH_NODE_ID)
     */
    private static ClusterState createClusterStateWithStartedShardOnNode(String shardCurrentNodeId) {
        final String indexName = "test-idx";
        final IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(indexName)
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0);
        // Use Index(name, uuid) so we do not build the IndexMetadata builder yet; put() will build it once
        final Index index = new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE);
        final ShardId shardId = new ShardId(index, 0);

        final ShardRouting startedShard = TestShardRouting.newShardRouting(
            shardId,
            shardCurrentNodeId,
            true,
            ShardRoutingState.STARTED,
            ShardRouting.Role.INDEX_ONLY
        );
        final IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(index).addShard(startedShard).build();
        final RoutingTable routingTable = RoutingTable.builder(new StatelessShardRoutingRoleStrategy()).add(indexRoutingTable).build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder())
            .putProjectMetadata(ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMetadataBuilder).build())
            .putRoutingTable(ProjectId.DEFAULT, routingTable)
            .build();
    }

    private static DiscoveryNodes.Builder nodesBuilder() {
        return DiscoveryNodes.builder()
            .add(newNode(NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode(OTHER_NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode(SEARCH_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)));
    }

    private EstimatedHeapUsage createNodeHeapUsage(String nodeId, long usagePercent, ByteSizeValue totalHeapSize) {
        final var totalInBytes = totalHeapSize.getBytes();
        final var usedInBytes = (long) Math.floor(totalInBytes * usagePercent / 100.0d);
        return new EstimatedHeapUsage(nodeId, totalInBytes, usedInBytes);
    }

    private Map<ShardId, ShardAndIndexHeapUsage> createShardAndIndexHeapUsageMap(ShardId shardId, long additionalBytes) {
        return Map.of(shardId, new ShardAndIndexHeapUsage(additionalBytes / 2, additionalBytes / 2));
    }
}
