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
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
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

    static final String SEARCH_NODE_ID = "search-node";
    static final String OTHER_SEARCH_NODE_ID = "other-" + SEARCH_NODE_ID;
    static final String INDEX_NODE_ID = "index-node";

    private static final long CACHE_SIZE_IN_BYTES = 1000L;

    public void testYesDecisionWhenDisabled() {
        final var decider = createDecider(false, 75, 95);
        final ShardRouting shardRouting = createShardRouting();

        // Even a node that is already massively over-subscribed should be allowed while the decider is disabled.
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, CACHE_SIZE_IN_BYTES, 0L)),
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
        final var decider = createDecider(true, 75, 95);
        final ShardRouting shardRouting = createShardRouting();

        // The index node has no cache commitment data at all; the role check should short-circuit before that's ever consulted.
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
        final var decider = createDecider(true, 75, 95);
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
        final var decider = createDecider(true, 75, 95);
        final ShardRouting shardRouting = createShardRouting();

        // 800 boosted bytes already exceeds the 75% (750 byte) low watermark.
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, 800L, 0L)),
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
            containsString("node [" + SEARCH_NODE_ID + "] cache commitment [800] bytes already exceeds the low watermark [750]")
        );
    }

    public void testYesWhenShardRequirementMissingButBelowWatermark() {
        final var decider = createDecider(true, 75, 95);
        final ShardRouting shardRouting = createShardRouting();

        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, 500L, 0L)),
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
        final var decider = createDecider(true, 75, 95);
        final ShardRouting shardRouting = createShardRouting();

        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, 500L, 0L)),
            Map.of(shardRouting.shardId(), new BoostedAndUnboostedCacheRequirements(300L, 0L))
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertThat(decision.getExplanation(), containsString("would raise its cache commitment from [500] to [800] bytes"));
    }

    public void testYesWhenShardStaysBelowWatermark() {
        final var decider = createDecider(true, 75, 95);
        final ShardRouting shardRouting = createShardRouting();

        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, 500L, 0L)),
            Map.of(shardRouting.shardId(), new BoostedAndUnboostedCacheRequirements(100L, 0L))
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("would raise its cache commitment from [500] to [600] bytes"));
    }

    public void testAccountingModeDivergence() {
        final ShardRouting shardRouting = createShardRouting();
        // Low boosted commitment (10%), but high unboosted commitment (80%).
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, 100L, 800L)),
            Map.of(shardRouting.shardId(), new BoostedAndUnboostedCacheRequirements(50L, 50L))
        );

        // In BOOSTED mode only the 100 boosted bytes count, well below the 750 byte low watermark.
        final var boostedDecider = createDecider(true, SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED, 75, 95);
        final RoutingAllocation boostedAllocation = createRoutingAllocation(boostedDecider, shardRouting, clusterInfo);
        final Decision boostedDecision = boostedDecider.canAllocate(
            shardRouting,
            boostedAllocation.routingNodes().node(SEARCH_NODE_ID),
            boostedAllocation
        );
        assertThat(boostedDecision.type(), equalTo(Decision.Type.YES));

        // In TOTAL mode the combined 900 bytes already exceeds the 750 byte low watermark.
        final var totalDecider = createDecider(true, SharedCacheCapacityAllocationDecider.CacheAccountingMode.TOTAL, 75, 95);
        final RoutingAllocation totalAllocation = createRoutingAllocation(totalDecider, shardRouting, clusterInfo);
        final Decision totalDecision = totalDecider.canAllocate(
            shardRouting,
            totalAllocation.routingNodes().node(SEARCH_NODE_ID),
            totalAllocation
        );
        assertThat(totalDecision.type(), equalTo(Decision.Type.NOT_PREFERRED));
    }

    public void testSentinelRequirementTreatedAsZeroNotSkipped() {
        final var decider = createDecider(true, SharedCacheCapacityAllocationDecider.CacheAccountingMode.TOTAL, 75, 95);
        final ShardRouting shardRouting = createShardRouting();

        // No boosted requirement (sentinel), 200 bytes unboosted requirement.
        final ClusterInfo clusterInfo = createClusterInfo(
            Map.of(SEARCH_NODE_ID, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, 0L, 500L)),
            Map.of(shardRouting.shardId(), new BoostedAndUnboostedCacheRequirements(NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT, 200L))
        );
        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);

        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node(SEARCH_NODE_ID),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        // 500 (unboosted commitment) + 200 (unboosted requirement) = 700; the sentinel -1 boosted requirement contributes 0, not -1.
        assertThat(decision.getExplanation(), containsString("would raise its cache commitment from [500] to [700] bytes"));
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
                SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING
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

    private RoutingAllocation createRoutingAllocation(AllocationDecider decider, ShardRouting shardRouting, ClusterInfo clusterInfo) {
        final var routingAllocation = TestRoutingAllocationFactory.forClusterState(createClusterState(shardRouting))
            .allocationDeciders(createAllocationDeciders(decider))
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
            .add(newNode(SEARCH_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)))
            .add(newNode(OTHER_SEARCH_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)))
            .add(newNode(INDEX_NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)));
    }
}
