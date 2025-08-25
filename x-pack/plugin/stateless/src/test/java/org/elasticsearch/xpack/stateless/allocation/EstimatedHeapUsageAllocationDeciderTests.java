/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
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
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.EmptySnapshotsInfoService;
import org.elasticsearch.snapshots.SnapshotShardSizeInfo;
import org.elasticsearch.test.gateway.TestGatewayAllocator;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EstimatedHeapUsageAllocationDeciderTests extends ESAllocationTestCase {

    public void testYesDecisionWhenDisabled() {
        final var decider = createEstimatedHeapUsageAllocationDecider(false, between(0, 100));

        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            nodeId,
            shardRouting,
            createClusterInfo(Map.of(nodeId, randomLongBetween(0, 100)))
        );
        final Decision decision = decider.canAllocate(shardRouting, routingAllocation.routingNodes().node(nodeId), routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("estimated heap allocation decider is disabled"));
    }

    public void testYesDecisionWhenNodeIsNotIndexNode() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, between(0, 100));

        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            nodeId,
            shardRouting,
            createClusterInfo(Map.of(nodeId, randomLongBetween(0, 100)))
        );
        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node("search-node"),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), equalTo("estimated heap allocation decider is applicable only to index nodes"));
    }

    public void testYesDecisionWhenUsageMetricIsMissing() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, between(0, 100));
        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            nodeId,
            shardRouting,
            createClusterInfo(Map.of(nodeId, randomLongBetween(0, 100)))
        );
        final Decision decision = decider.canAllocate(
            shardRouting,
            routingAllocation.routingNodes().node("not-" + nodeId),
            routingAllocation
        );
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("no estimated heap estimation available for node [not-" + nodeId + "]"));
    }

    public void testYesDecisionWhenUsageBelowLowWatermark() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, 85);

        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            nodeId,
            shardRouting,
            createClusterInfo(Map.of(nodeId, randomLongBetween(0, 85)))
        );
        final Decision decision = decider.canAllocate(shardRouting, routingAllocation.routingNodes().node(nodeId), routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(decision.getExplanation(), containsString("sufficient estimated heap available on node [" + nodeId + "]"));
    }

    public void testNoDecisionWhenUsageAboveLowWatermark() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, 85);

        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            nodeId,
            shardRouting,
            createClusterInfo(Map.of(nodeId, randomLongBetween(86, 100)))
        );
        final Decision decision = decider.canAllocate(shardRouting, routingAllocation.routingNodes().node(nodeId), routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.NO));
        assertThat(decision.getExplanation(), containsString("insufficient estimated heap available on node [" + nodeId + "]"));
    }

    public void testYesDecisionWhenNodeHeapIsBelowMinimumThreshold() {
        final int minimumHeapSizeForEnablementInGigabytes = between(2, 32);
        final var decider = createEstimatedHeapUsageAllocationDecider(
            true,
            between(0, 100),
            ByteSizeValue.ofGb(minimumHeapSizeForEnablementInGigabytes)
        );

        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();
        final RoutingAllocation routingAllocation = createRoutingAllocation(
            decider,
            nodeId,
            shardRouting,
            createClusterInfo(
                Map.of(nodeId, randomLongBetween(0, 100)),
                () -> ByteSizeValue.ofGb(between(1, minimumHeapSizeForEnablementInGigabytes - 1))
            )
        );
        final Decision decision = decider.canAllocate(shardRouting, routingAllocation.routingNodes().node(nodeId), routingAllocation);
        assertThat(decision.type(), equalTo(Decision.Type.YES));
        assertThat(
            decision.getExplanation(),
            equalTo(
                Strings.format(
                    "estimated heap decider will not intervene if heap size is below [%s]",
                    ByteSizeValue.ofGb(minimumHeapSizeForEnablementInGigabytes)
                )
            )
        );
    }

    public void testAllocationExplain() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, 85);

        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();

        final ClusterInfo clusterInfo = createClusterInfo(Map.of(nodeId, randomLongBetween(0, 85), "not-" + nodeId, 100L));
        final var routingAllocation = createRoutingAllocation(decider, nodeId, shardRouting, clusterInfo);
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
            "sufficient estimated heap available on node [" + nodeId + "]"
        );
        assertExplanationResult(
            explanation,
            nodeDecisions,
            Decision.Type.NO,
            "insufficient estimated heap available on node [not-" + nodeId + "]"
        );
        assertExplanationResult(
            explanation,
            nodeDecisions,
            Decision.Type.YES,
            "estimated heap allocation decider is applicable only to index nodes"
        );
    }

    private static void assertExplanationResult(
        String explanation,
        List<NodeAllocationResult> nodeDecisions,
        Decision.Type yes,
        String message
    ) {
        assertTrue(
            explanation,
            nodeDecisions.stream()
                .anyMatch(
                    nodeDecision -> nodeDecision.getCanAllocateDecision()
                        .getDecisions()
                        .stream()
                        .anyMatch(decision -> decision.type().equals(yes) && decision.getExplanation().startsWith(message))
                )
        );
    }

    public void testAllocationBasedOnEstimatedHeapUsage() {
        final String nodeId = randomIdentifier();
        final ShardRouting shardRouting = createShardRouting();

        final var decider = createEstimatedHeapUsageAllocationDecider(true, 85);
        final ClusterState initialState = createClusterState(nodeId, shardRouting);
        final var allocationService = createAllocationService(
            decider,
            createClusterInfo(Map.of(nodeId, randomLongBetween(0, 85), "not-" + nodeId, 100L))
        );

        final var newState = applyStartedShardsUntilNoChange(initialState, allocationService);

        assertFalse(newState.getRoutingNodes().toString(), newState.getRoutingNodes().hasUnassignedShards());
        final var indexRoutingTable = newState.routingTable(ProjectId.DEFAULT).index(shardRouting.index());
        indexRoutingTable.allShards().forEach(indexShardRoutingTable -> {
            final var primaryShard = indexShardRoutingTable.primaryShard();
            assertNotNull(primaryShard);
            assertThat(primaryShard.currentNodeId(), equalTo(nodeId));
        });
    }

    private static EstimatedHeapUsageAllocationDecider createEstimatedHeapUsageAllocationDecider(boolean enabled, int percent) {
        return createEstimatedHeapUsageAllocationDecider(enabled, percent, ByteSizeValue.ZERO);
    }

    private static EstimatedHeapUsageAllocationDecider createEstimatedHeapUsageAllocationDecider(
        boolean enabled,
        int percent,
        ByteSizeValue minimumHeapSizeForEnabled
    ) {
        final var clusterSettings = new ClusterSettings(
            Settings.builder()
                .put(EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), minimumHeapSizeForEnabled)
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), enabled)
                .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK.getKey(), percent + "%")
                .build(),
            Set.of(
                InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED,
                EstimatedHeapUsageAllocationDecider.MINIMUM_LOGGING_INTERVAL,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
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

    private RoutingAllocation createRoutingAllocation(
        AllocationDecider decider,
        String nodeId,
        ShardRouting shardRouting,
        ClusterInfo clusterInfo
    ) {
        final var routingAllocation = new RoutingAllocation(
            createAllocationDeciders(decider),
            createClusterState(nodeId, shardRouting),
            clusterInfo,
            SnapshotShardSizeInfo.EMPTY,
            System.nanoTime()
        );
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

    private ClusterInfo createClusterInfo(Map<String, Long> nodeEstimatedHeapUsagePercent) {
        return createClusterInfo(nodeEstimatedHeapUsagePercent, () -> ByteSizeValue.ofGb(between(1, 32)));
    }

    private ClusterInfo createClusterInfo(Map<String, Long> nodeEstimatedHeapUsagePercent, Supplier<ByteSizeValue> totalHeapSizeSupplier) {
        final var clusterInfo = mock(ClusterInfo.class);
        when(clusterInfo.getEstimatedHeapUsages()).thenReturn(
            nodeEstimatedHeapUsagePercent.entrySet()
                .stream()
                .collect(
                    Collectors.toUnmodifiableMap(
                        Map.Entry::getKey,
                        entry -> createNodeHeapUsage(entry.getKey(), entry.getValue(), totalHeapSizeSupplier.get())
                    )
                )
        );
        return clusterInfo;
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

    private static ClusterState createClusterState(String nodeId, ShardRouting shardRouting) {
        var discoveryNodesBuilder = DiscoveryNodes.builder();
        discoveryNodesBuilder.add(newNode(nodeId, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode("not-" + nodeId, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode("search-node", Set.of(DiscoveryNodeRole.SEARCH_ROLE)));

        final var projectMetadata = ProjectMetadata.builder(ProjectId.DEFAULT)
            .put(
                IndexMetadata.builder(shardRouting.getIndexName())
                    .settings(settings(IndexVersion.current()))
                    .numberOfShards(between(shardRouting.id() + 1, shardRouting.id() + 3))
                    .numberOfReplicas(0)
            )
            .build();

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder)
            .putProjectMetadata(projectMetadata)
            .putRoutingTable(
                ProjectId.DEFAULT,
                RoutingTable.builder(new StatelessShardRoutingRoleStrategy())
                    .addAsNew(projectMetadata.index(shardRouting.getIndexName()))
                    .build()
            )
            .build();
    }

    private EstimatedHeapUsage createNodeHeapUsage(String nodeId, long usagePercent, ByteSizeValue totalHeapSize) {
        final var totalInBytes = totalHeapSize.getBytes();
        final var usedInBytes = (long) Math.floor(totalInBytes * usagePercent / 100.0d);
        return new EstimatedHeapUsage(nodeId, totalInBytes, usedInBytes);
    }
}
