/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
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
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements.NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SharedCacheCapacityAllocationDeciderTests extends ESAllocationTestCase {

    // Every test's cluster state is built from these same three nodes (see nodesBuilder()): two search nodes and one index node. Tests
    // that need an index node use INDEX_NODE_ID directly rather than building a separate cluster state.
    static final String SEARCH_NODE_ID = "search-node";
    static final String OTHER_SEARCH_NODE_ID = "other-" + SEARCH_NODE_ID;
    static final String INDEX_NODE_ID = "index-node";

    private static final long CACHE_SIZE_IN_BYTES = 1000L;
    private static final long NO_COMMITMENT_BYTES = 0L;
    private static final int LOW_WATERMARK_PERCENT = 75;
    private static final int HIGH_WATERMARK_PERCENT = 95;
    private static final long LOW_WATERMARK_BYTES = bytesForPercent(LOW_WATERMARK_PERCENT);

    public void testYesDecisionWhenDisabled() {
        final var decider = createDecider(false, LOW_WATERMARK_PERCENT, HIGH_WATERMARK_PERCENT);
        final ShardRouting shardRouting = createShardRouting();

        // The node's cache is fully committed, but that should not matter while the decider is disabled.
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, CACHE_SIZE_IN_BYTES, NO_COMMITMENT_BYTES)),
            Map.of()
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("shared cache capacity decider is disabled"));
    }

    public void testYesDecisionWhenNodeIsNotSearchNode() {
        final var decider = createDecider(true, LOW_WATERMARK_PERCENT, HIGH_WATERMARK_PERCENT);
        final ShardRouting shardRouting = createShardRouting();

        // The index node has no cache commitment data at all. The role check should short-circuit before that data is ever consulted.
        final ClusterInfo clusterInfo = createClusterInfo(Map.of(), Map.of());
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(INDEX_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("shared cache capacity decider is applicable only to search nodes"));
    }

    public void testYesDecisionWhenNodeCacheDataMissing() {
        final var decider = createDecider(true, LOW_WATERMARK_PERCENT, HIGH_WATERMARK_PERCENT);
        final ShardRouting shardRouting = createShardRouting();

        final ClusterInfo clusterInfo = createClusterInfo(Map.of(), Map.of());
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            containsString("no cache size and commitment data available for node [" + SEARCH_NODE_ID + "]")
        );
    }

    public void testNotPreferredWhenAlreadyOverWatermark() {
        final var decider = createDecider(true, LOW_WATERMARK_PERCENT, HIGH_WATERMARK_PERCENT);
        final ShardRouting shardRouting = createShardRouting();

        // The node is already 80% committed, which exceeds the 75% low watermark.
        final long overWatermarkCommitmentBytes = bytesForPercent(80);
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, overWatermarkCommitmentBytes, NO_COMMITMENT_BYTES)),
            Map.of()
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(
            decision.getExplanation(),
            containsString(
                "node ["
                    + SEARCH_NODE_ID
                    + "] cache commitment ["
                    + overWatermarkCommitmentBytes
                    + "] bytes already exceeds the low "
                    + "watermark ["
                    + LOW_WATERMARK_BYTES
                    + "]"
            )
        );
    }

    public void testYesWhenShardRequirementMissingButBelowWatermark() {
        final var decider = createDecider(true, LOW_WATERMARK_PERCENT, HIGH_WATERMARK_PERCENT);
        final ShardRouting shardRouting = createShardRouting();

        final long belowWatermarkCommitmentBytes = bytesForPercent(50);
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(
                SEARCH_NODE_ID,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, belowWatermarkCommitmentBytes, NO_COMMITMENT_BYTES)
            ),
            Map.of()
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            containsString("no cache requirement data available for shard [" + shardRouting.shardId() + "]")
        );
    }

    public void testNotPreferredWhenShardWouldExceedWatermark() {
        final var decider = createDecider(true, LOW_WATERMARK_PERCENT, HIGH_WATERMARK_PERCENT);
        final ShardRouting shardRouting = createShardRouting();

        final long belowWatermarkCommitmentBytes = bytesForPercent(50);
        final long exceedingShardRequirementBytes = bytesForPercent(30);
        final long newCommitmentBytes = belowWatermarkCommitmentBytes + exceedingShardRequirementBytes;
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(
                SEARCH_NODE_ID,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, belowWatermarkCommitmentBytes, NO_COMMITMENT_BYTES)
            ),
            Map.of(shardRouting.shardId(), new BoostedAndUnboostedCacheRequirements(exceedingShardRequirementBytes, NO_COMMITMENT_BYTES))
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(
            decision.getExplanation(),
            containsString(
                "would raise its cache commitment from [" + belowWatermarkCommitmentBytes + "] to [" + newCommitmentBytes + "] bytes"
            )
        );
    }

    public void testYesWhenShardStaysBelowWatermark() {
        final var decider = createDecider(true, LOW_WATERMARK_PERCENT, HIGH_WATERMARK_PERCENT);
        final ShardRouting shardRouting = createShardRouting();

        final long belowWatermarkCommitmentBytes = bytesForPercent(50);
        final long safeShardRequirementBytes = bytesForPercent(10);
        final long newCommitmentBytes = belowWatermarkCommitmentBytes + safeShardRequirementBytes;
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(
                SEARCH_NODE_ID,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, belowWatermarkCommitmentBytes, NO_COMMITMENT_BYTES)
            ),
            Map.of(shardRouting.shardId(), new BoostedAndUnboostedCacheRequirements(safeShardRequirementBytes, NO_COMMITMENT_BYTES))
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            containsString(
                "would raise its cache commitment from [" + belowWatermarkCommitmentBytes + "] to [" + newCommitmentBytes + "] bytes"
            )
        );
    }

    public void testAccountingModeDivergence() {
        final ShardRouting shardRouting = createShardRouting();

        final long lowBoostedCommitmentBytes = bytesForPercent(10);
        final long highUnboostedCommitmentBytes = bytesForPercent(80);
        final long shardBoostedRequirementBytes = bytesForPercent(5);
        final long shardUnboostedRequirementBytes = bytesForPercent(5);
        final long totalCommitmentBytes = lowBoostedCommitmentBytes + highUnboostedCommitmentBytes;
        final long newBoostedCommitmentBytes = lowBoostedCommitmentBytes + shardBoostedRequirementBytes;

        // The node has a low boosted commitment but a high unboosted commitment, so the two accounting modes should disagree.
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(
                SEARCH_NODE_ID,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, lowBoostedCommitmentBytes, highUnboostedCommitmentBytes)
            ),
            Map.of(
                shardRouting.shardId(),
                new BoostedAndUnboostedCacheRequirements(shardBoostedRequirementBytes, shardUnboostedRequirementBytes)
            )
        );

        // In BOOSTED mode only the boosted bytes count, which stays well below the low watermark.
        final var boostedDecider = createDecider(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            LOW_WATERMARK_PERCENT,
            HIGH_WATERMARK_PERCENT
        );
        final RoutingAllocation boostedAllocation = createRoutingAllocation(boostedDecider, shardRouting, clusterInfo);
        final Decision boostedDecision = boostedDecider.canAllocate(
            shardRouting,
            boostedAllocation.routingNodes().node(SEARCH_NODE_ID),
            boostedAllocation
        );
        assertThat(boostedDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            boostedDecision.getExplanation(),
            containsString(
                "would raise its cache commitment from [" + lowBoostedCommitmentBytes + "] to [" + newBoostedCommitmentBytes + "] bytes"
            )
        );

        // In TOTAL mode the combined boosted and unboosted bytes already exceed the low watermark.
        final var totalDecider = createDecider(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.TOTAL,
            LOW_WATERMARK_PERCENT,
            HIGH_WATERMARK_PERCENT
        );
        final RoutingAllocation totalAllocation = createRoutingAllocation(totalDecider, shardRouting, clusterInfo);
        final Decision totalDecision = totalDecider.canAllocate(
            shardRouting,
            totalAllocation.routingNodes().node(SEARCH_NODE_ID),
            totalAllocation
        );
        assertThat(totalDecision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(
            totalDecision.getExplanation(),
            containsString(
                "node ["
                    + SEARCH_NODE_ID
                    + "] cache commitment ["
                    + totalCommitmentBytes
                    + "] bytes already exceeds the low watermark "
                    + "["
                    + LOW_WATERMARK_BYTES
                    + "]"
            )
        );
    }

    public void testSentinelRequirementTreatedAsZeroNotSkipped() {
        final var decider = createDecider(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.TOTAL,
            LOW_WATERMARK_PERCENT,
            HIGH_WATERMARK_PERCENT
        );
        final ShardRouting shardRouting = createShardRouting();

        final long unboostedCommitmentBytes = bytesForPercent(50);
        final long unboostedRequirementBytes = bytesForPercent(20);
        final long newCommitmentBytes = unboostedCommitmentBytes + unboostedRequirementBytes;

        // The shard has no boosted requirement (the sentinel value), only an unboosted requirement.
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, NO_COMMITMENT_BYTES, unboostedCommitmentBytes)),
            Map.of(
                shardRouting.shardId(),
                new BoostedAndUnboostedCacheRequirements(NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT, unboostedRequirementBytes)
            )
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        // The sentinel boosted requirement must contribute zero bytes to the total, not be treated as -1.
        assertThat(
            decision.getExplanation(),
            containsString("would raise its cache commitment from [" + unboostedCommitmentBytes + "] to [" + newCommitmentBytes + "] bytes")
        );
    }

    private static long bytesForPercent(int percent) {
        return CACHE_SIZE_IN_BYTES * percent / 100;
    }

    private static SharedCacheCapacityAllocationDecider createDecider(boolean enabled, int lowWatermarkPercent, int highWatermarkPercent) {
        return createDecider(
            enabled,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            lowWatermarkPercent,
            highWatermarkPercent
        );
    }

    private static SharedCacheCapacityAllocationDecider createDecider(
        boolean enabled,
        SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode,
        int lowWatermarkPercent,
        int highWatermarkPercent
    ) {
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(SharedCacheCapacityAllocationDecider.ENABLED_SETTING.getKey(), enabled)
                .put(SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING.getKey(), accountingMode.name())
                .put(SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING.getKey(), lowWatermarkPercent + "%")
                .put(SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING.getKey(), highWatermarkPercent + "%")
                .build(),
            Set.of(
                SharedCacheCapacityAllocationDecider.ENABLED_SETTING,
                SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING,
                SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING,
                SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING,
                SharedCacheCapacityAllocationDecider.MINIMUM_LOGGING_INTERVAL
            )
        );
        return new SharedCacheCapacityAllocationDecider(clusterSettings);
    }

    private static ShardRouting createShardRouting() {
        return ShardRouting.newUnassigned(
            new ShardId(randomIdentifier(), IndexMetadata.INDEX_UUID_NA_VALUE, between(0, 2)),
            false,
            RecoverySource.PeerRecoverySource.INSTANCE,
            TestShardRouting.buildUnassignedInfo("auto generated for test"),
            ShardRouting.Role.SEARCH_ONLY
        );
    }

    /**
     * Builds a {@link RoutingAllocation} with only {@code decider} registered. No other allocation deciders are consulted anywhere in
     * this test class: every test calls {@link SharedCacheCapacityAllocationDecider#canAllocate} directly rather than going through
     * {@link org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders} or {@code AllocationService}, so a broader decider
     * set would be unused boilerplate.
     */
    private RoutingAllocation createRoutingAllocation(AllocationDecider decider, ShardRouting shardRouting, ClusterInfo clusterInfo) {
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(createClusterState(shardRouting))
            .allocationDeciders(decider)
            .clusterInfo(clusterInfo)
            .build();
        routingAllocation.debugDecision(true);
        return routingAllocation;
    }

    private ClusterInfo createClusterInfo(
        Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments,
        Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements
    ) {
        return ClusterInfo.builder()
            .nodeCacheSizeAndCommitments(nodeCacheSizeAndCommitments)
            .shardCacheRequirements(shardCacheRequirements)
            .build();
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
            .add(newNode(SEARCH_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)))
            .add(newNode(OTHER_SEARCH_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)))
            .add(newNode(INDEX_NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)));
    }
}
