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
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.NodeHeapMetrics;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.TestRoutingAllocationFactory;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class HostedShardsPartitionHeapAllocationDeciderTests extends ESAllocationTestCase {

    static final String NODE_ID = "node-id";
    static final String OTHER_NODE_ID = "not-" + NODE_ID;
    static final String SEARCH_NODE_ID = "search-node";

    public void testYesWhenDeciderDisabled() {
        final var decider = createDecider(false, between(0, 100), between(0, 100));

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation allocation = createRoutingAllocation(
            decider,
            shardRouting,
            buildClusterInfo(NODE_ID, ByteSizeValue.ofGb(8), 90, shardRouting.shardId(), 0)
        );

        final Decision canAllocate = decider.canAllocate(shardRouting, allocation.routingNodes().node(NODE_ID), allocation);
        assertThat(canAllocate.type(), equalTo(Decision.Type.YES));
        assertThat(canAllocate.getExplanation(), equalTo("hosted shards partition heap allocation decider is disabled"));

        final Decision canRemain = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            allocation.routingNodes().node(NODE_ID),
            allocation
        );
        assertThat(canRemain.type(), equalTo(Decision.Type.YES));
        assertThat(canRemain.getExplanation(), equalTo("hosted shards partition heap allocation decider is disabled"));
    }

    public void testYesWhenNodeIsNotIndexNode() {
        final var decider = createDecider(true, between(0, 100), between(0, 100));

        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation allocation = createRoutingAllocation(
            decider,
            shardRouting,
            buildClusterInfo(NODE_ID, ByteSizeValue.ofGb(8), 50, shardRouting.shardId(), 0)
        );
        final var searchNode = allocation.routingNodes().node(SEARCH_NODE_ID);

        final Decision canAllocate = decider.canAllocate(shardRouting, searchNode, allocation);
        assertThat(canAllocate.type(), equalTo(Decision.Type.YES));
        assertThat(
            canAllocate.getExplanation(),
            equalTo("hosted shards partition heap allocation decider is applicable only to index nodes")
        );

        final Decision canRemain = decider.canRemain(mock(IndexMetadata.class), shardRouting, searchNode, allocation);
        assertThat(canRemain.type(), equalTo(Decision.Type.YES));
        assertThat(
            canRemain.getExplanation(),
            equalTo("hosted shards partition heap allocation decider is applicable only to index nodes")
        );
    }

    public void testYesWhenNodeHeapMetricsMissing() {
        final var decider = createDecider(true, between(0, 100), between(0, 100));
        final ShardRouting shardRouting = createShardRouting();
        // ClusterInfo has no entry for OTHER_NODE_ID
        final RoutingAllocation allocation = createRoutingAllocation(
            decider,
            shardRouting,
            buildClusterInfo(NODE_ID, ByteSizeValue.ofGb(8), 50, shardRouting.shardId(), 0)
        );
        final var noMetricsNode = allocation.routingNodes().node(OTHER_NODE_ID);

        final Decision canAllocate = decider.canAllocate(shardRouting, noMetricsNode, allocation);
        assertThat(canAllocate.type(), equalTo(Decision.Type.YES));
        assertThat(canAllocate.getExplanation(), containsString("no estimated heap estimation available for node [" + OTHER_NODE_ID + "]"));

        final Decision canRemain = decider.canRemain(mock(IndexMetadata.class), shardRouting, noMetricsNode, allocation);
        assertThat(canRemain.type(), equalTo(Decision.Type.YES));
        assertThat(canRemain.getExplanation(), containsString("no estimated heap estimation available for node [" + OTHER_NODE_ID + "]"));
    }

    public void testYesWhenPartitionSizeAbsent() {
        final var decider = createDecider(true, 85, 90);

        final ShardRouting shardRouting = createShardRouting();
        // ClusterInfo has nodeHeapMetrics for NODE_ID but no partition size
        final ClusterInfo clusterInfo = ClusterInfo.builder()
            .nodeHeapMetrics(Map.of(NODE_ID, new NodeHeapMetrics(NODE_ID, ByteSizeValue.ofGb(16).getBytes(), new NodeHeapEstimates(0, 0))))
            .build();
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision canAllocate = decider.canAllocate(shardRouting, allocation.routingNodes().node(NODE_ID), allocation);
        assertThat(canAllocate.type(), equalTo(Decision.Type.YES));
        assertThat(
            canAllocate.getExplanation(),
            containsString("no hosted shards partition heap capacity data available for node [" + NODE_ID + "]")
        );

        final Decision canRemain = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            allocation.routingNodes().node(NODE_ID),
            allocation
        );
        assertThat(canRemain.type(), equalTo(Decision.Type.YES));
        assertThat(
            canRemain.getExplanation(),
            containsString("no hosted shards partition heap capacity data available for node [" + NODE_ID + "]")
        );
    }

    public void testCanAllocateNoWhenPartitionUsageExceedsLowWatermark() {
        final int watermark = 85;
        final var decider = createDecider(true, watermark, watermark);

        final ByteSizeValue partitionSize = ByteSizeValue.ofGb(8);
        final ShardRouting shardRouting = createShardRouting();
        final ClusterInfo clusterInfo = buildClusterInfo(NODE_ID, partitionSize, watermark + 5, shardRouting.shardId(), 0);
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(shardRouting, allocation.routingNodes().node(NODE_ID), allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            containsString("insufficient hosted shards partition heap available on node [" + NODE_ID + "]")
        );
        assertThat(decision.getExplanation(), containsString("exceeds low watermark"));
    }

    public void testCanAllocateYesWhenPartitionUsageBelowLowWatermark() {
        final int watermark = 85;
        final var decider = createDecider(true, watermark, watermark);

        final ByteSizeValue partitionSize = ByteSizeValue.ofGb(8);
        final ShardRouting shardRouting = createShardRouting();
        final ClusterInfo clusterInfo = buildClusterInfo(NODE_ID, partitionSize, watermark - 5, shardRouting.shardId(), 0);
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(shardRouting, allocation.routingNodes().node(NODE_ID), allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            containsString("sufficient hosted shards partition heap available on node [" + NODE_ID + "]")
        );
    }

    /**
     * Partition usage is below the low watermark currently, but adding the shard's heap would push it over.
     */
    public void testCanAllocateNoWhenProjectedPartitionUsageExceedsLowWatermark() {
        final int watermark = 85;
        final var decider = createDecider(true, watermark, watermark);

        final ByteSizeValue partitionSize = ByteSizeValue.ofGb(8);
        // current: 80% of partition used; shard adds another 10% → projected 90% > 85% watermark
        final long shardBytes = (long) (partitionSize.getBytes() * 0.10);
        final ShardRouting shardRouting = createShardRouting();
        final ClusterInfo clusterInfo = buildClusterInfo(NODE_ID, partitionSize, 80, shardRouting.shardId(), shardBytes);
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(shardRouting, allocation.routingNodes().node(NODE_ID), allocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            containsString("insufficient hosted shards partition heap available on node [" + NODE_ID + "]")
        );
        assertThat(decision.getExplanation(), containsString("exceeds low watermark"));
    }

    /**
     * Partition usage is below the low watermark and the projected usage after adding the shard stays below it too.
     */
    public void testCanAllocateYesWhenProjectedPartitionUsageStaysBelowLowWatermark() {
        final int watermark = 85;
        final var decider = createDecider(true, watermark, watermark);

        final ByteSizeValue partitionSize = ByteSizeValue.ofGb(8);
        // current: 80%; shard adds 3% → projected 83% < 85%
        final long shardBytes = (long) (partitionSize.getBytes() * 0.03);
        final ShardRouting shardRouting = createShardRouting();
        final ClusterInfo clusterInfo = buildClusterInfo(NODE_ID, partitionSize, 80, shardRouting.shardId(), shardBytes);
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(shardRouting, allocation.routingNodes().node(NODE_ID), allocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            containsString("sufficient hosted shards partition heap available on node [" + NODE_ID + "]")
        );
    }

    public void testCanRemainNoWhenPartitionUsageExceedsHighWatermark() {
        final int highWatermark = 90;
        final var decider = createDecider(true, 95, highWatermark);

        final ByteSizeValue partitionSize = ByteSizeValue.ofGb(8);
        final ShardRouting shardRouting = createShardRouting();
        final ClusterInfo clusterInfo = buildClusterInfo(NODE_ID, partitionSize, highWatermark + 5, shardRouting.shardId(), 0);
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            allocation.routingNodes().node(NODE_ID),
            allocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            containsString("insufficient hosted shards partition heap available on node [" + NODE_ID + "]")
        );
        assertThat(decision.getExplanation(), containsString("exceeds high watermark"));
    }

    public void testCanRemainYesWhenPartitionUsageBelowHighWatermark() {
        final int highWatermark = 90;
        final var decider = createDecider(true, 95, highWatermark);

        final ByteSizeValue partitionSize = ByteSizeValue.ofGb(8);
        final ShardRouting shardRouting = createShardRouting();
        final ClusterInfo clusterInfo = buildClusterInfo(NODE_ID, partitionSize, highWatermark - 5, shardRouting.shardId(), 0);
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            allocation.routingNodes().node(NODE_ID),
            allocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            containsString("sufficient hosted shards partition heap available on node [" + NODE_ID + "]")
        );
    }

    public void testCanRemainYesWhenHighWatermarkDisabled() {
        final var decider = createDecider(true, false, 85, 90);

        final ShardRouting shardRouting = createShardRouting();
        // Partition usage well above high watermark — but high watermark is disabled
        final ClusterInfo clusterInfo = buildClusterInfo(NODE_ID, ByteSizeValue.ofGb(8), 95, shardRouting.shardId(), 0);
        final RoutingAllocation allocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision canRemain = decider.canRemain(
            mock(IndexMetadata.class),
            shardRouting,
            allocation.routingNodes().node(NODE_ID),
            allocation
        );
        assertThat(canRemain.type(), equalTo(Decision.Type.YES));
        assertThat(canRemain.getExplanation(), equalTo("hosted shards partition heap decider can remain disabled"));
    }

    // --- helpers ---

    private static HostedShardsPartitionHeapAllocationDecider createDecider(boolean enabled, int lowWatermark, int highWatermark) {
        return createDecider(enabled, true, lowWatermark, highWatermark);
    }

    private static HostedShardsPartitionHeapAllocationDecider createDecider(
        boolean enabled,
        boolean highWatermarkEnabled,
        int lowWatermark,
        int highWatermark
    ) {
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(HostedShardsPartitionHeapAllocationDecider.ENABLED_SETTING.getKey(), enabled)
                .put(HostedShardsPartitionHeapAllocationDecider.HIGH_WATERMARK_ENABLED_SETTING.getKey(), highWatermarkEnabled)
                .put(HostedShardsPartitionHeapAllocationDecider.LOW_WATERMARK_SETTING.getKey(), lowWatermark + "%")
                .put(HostedShardsPartitionHeapAllocationDecider.HIGH_WATERMARK_SETTING.getKey(), highWatermark + "%")
                .put(AbstractEstimatedHeapAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), ByteSizeValue.ZERO)
                .build(),
            Set.of(
                HostedShardsPartitionHeapAllocationDecider.ENABLED_SETTING,
                HostedShardsPartitionHeapAllocationDecider.HIGH_WATERMARK_ENABLED_SETTING,
                HostedShardsPartitionHeapAllocationDecider.LOW_WATERMARK_SETTING,
                HostedShardsPartitionHeapAllocationDecider.HIGH_WATERMARK_SETTING,
                AbstractEstimatedHeapAllocationDecider.MINIMUM_LOGGING_INTERVAL,
                AbstractEstimatedHeapAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT
            )
        );
        return new HostedShardsPartitionHeapAllocationDecider(clusterSettings);
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

    /**
     * Builds a ClusterInfo where {@code nodeId} has a specific hosted-shards partition usage percentage
     * and optional per-shard heap for the projection check. Total JVM heap is set large enough that the
     * total-heap guard never fires.
     */
    private static ClusterInfo buildClusterInfo(
        String nodeId,
        ByteSizeValue partitionSize,
        int partitionUsagePercent,
        ShardId shardId,
        long shardBytes
    ) {
        final long totalHeapBytes = ByteSizeValue.ofGb(16).getBytes();
        final long hostedShardsUsed = (long) (partitionSize.getBytes() * partitionUsagePercent / 100.0);
        final NodeHeapMetrics metrics = new NodeHeapMetrics(
            nodeId,
            totalHeapBytes,
            new NodeHeapEstimates(hostedShardsUsed, hostedShardsUsed)
        );
        final Map<ShardId, ShardAndIndexHeapUsage> shardUsages = shardBytes > 0
            ? Map.of(shardId, new ShardAndIndexHeapUsage(shardBytes, 0))
            : Map.of();
        return ClusterInfo.builder()
            .nodeHeapMetrics(Map.of(nodeId, metrics))
            .hostedShardsPartitionSizeByNodeId(Map.of(nodeId, partitionSize.getBytes()))
            .estimatedShardHeapUsages(shardUsages)
            .build();
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

    private static DiscoveryNodes.Builder nodesBuilder() {
        return DiscoveryNodes.builder()
            .add(newNode(NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode(OTHER_NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode(SEARCH_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)));
    }
}
