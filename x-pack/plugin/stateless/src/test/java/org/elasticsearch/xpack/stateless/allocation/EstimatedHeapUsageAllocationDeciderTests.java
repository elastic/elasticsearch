/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.NodeHeapMetrics;
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
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.stateless.EstimatedHeapSettings;

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

    public void testDynamicSettings() {
        final var clusterSettings = createClusterSettings(true, true, 85, 90, ByteSizeValue.ZERO);
        final var decider = new EstimatedHeapUsageAllocationDecider(new EstimatedHeapSettings(clusterSettings), clusterSettings);
        assertTrue(decider.isEnabled());
        assertTrue(decider.isHighWatermarkEnabled());
        assertEquals(85.0, decider.getLowWatermarkPercent(), 0.0);
        assertEquals(90.0, decider.getHighWatermarkPercent(), 0.0);
        clusterSettings.applySettings(
            Settings.builder()
                .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), false)
                .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED.getKey(), false)
                .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK.getKey(), "70%")
                .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK.getKey(), "80%")
                .build()
        );
        assertFalse(decider.isEnabled());
        assertFalse(decider.isHighWatermarkEnabled());
        assertEquals(70.0, decider.getLowWatermarkPercent(), 0.0);
        assertEquals(80.0, decider.getHighWatermarkPercent(), 0.0);
    }

    public void testUsesTotalHeapAndJvmCapacity() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, 85, 90);
        final var metrics = new NodeHeapMetrics(NODE_ID, 1000, new NodeHeapEstimates(900, 100));
        final var shard = createShardRouting();
        final var allocation = createRoutingAllocation(
            decider,
            shard,
            ClusterInfo.builder().nodeHeapMetrics(Map.of(NODE_ID, metrics)).hostedShardsPartitionSizeByNodeId(Map.of(NODE_ID, 200L)).build()
        );
        assertEquals(900L, decider.getCurrentUsageBytes(metrics));
        assertEquals(Long.valueOf(1000), decider.resolveCapacityBytes(metrics, allocation.routingNodes().node(NODE_ID), allocation));
    }

    @TestLogging(
        value = "org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageAllocationDecider:DEBUG",
        reason = "verify the concrete decider logger"
    )
    public void testLogsToSubclassLogger() {
        final var decider = createEstimatedHeapUsageAllocationDecider(true, true, 85, 90, ByteSizeValue.ZERO);
        final ShardRouting shard = createShardRouting();
        final RoutingAllocation allocation = createRoutingAllocation(
            decider,
            shard,
            createClusterInfoWithGenNodeAndShardHeap(Map.of(NODE_ID, 95L), shard.shardId())
        );
        allocation.debugDecision(false);
        final var node = allocation.routingNodes().node(NODE_ID);
        try (MockLog mockLog = MockLog.capture(EstimatedHeapUsageAllocationDecider.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "allocation rejection uses the subclass logger",
                    EstimatedHeapUsageAllocationDecider.class.getCanonicalName(),
                    Level.DEBUG,
                    "insufficient estimated heap available on node *exceeds low watermark*"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "can-remain rejection uses the subclass logger",
                    EstimatedHeapUsageAllocationDecider.class.getCanonicalName(),
                    Level.DEBUG,
                    "insufficient estimated heap available on node *exceeds high watermark*"
                )
            );
            assertThat(decider.canAllocate(shard, node, allocation).type(), equalTo(Decision.Type.NO));
            assertThat(
                decider.canRemain(allocation.metadata().getProject(ProjectId.DEFAULT).index(shard.index()), shard, node, allocation).type(),
                equalTo(Decision.Type.NO)
            );
            mockLog.assertAllExpectationsMatched();
        }
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
            Map.of(NODE_ID, createNodeHeapMetrics(NODE_ID, 80, totalHeap)),
            createShardAndIndexHeapUsageMap(shardRouting.shardId(), additionalBytes)
        );

        final RoutingAllocation routingAllocation = createRoutingAllocation(decider, shardRouting, clusterInfo);
        final Decision decision = decider.canAllocate(shardRouting, routingAllocation.routingNodes().node(NODE_ID), routingAllocation);
        assertThat(decision.toString(), decision.type(), equalTo(Decision.Type.NO));
        assertThat(
            decision.getExplanation(),
            containsString("insufficient estimated heap available on node [" + NODE_ID + "/" + NODE_ID + "]")
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
            "sufficient estimated heap available on node [" + NODE_ID + "/" + NODE_ID + "]"
        );
        assertExplanationResult(
            explanation,
            nodeDecisions,
            Decision.Type.NO,
            "insufficient estimated heap available on node [" + OTHER_NODE_ID + "/" + OTHER_NODE_ID + "]"
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
                "insufficient estimated heap available on node [" + NODE_ID + "/" + NODE_ID + "]"
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
                "sufficient estimated heap available on node [" + NODE_ID + "/" + NODE_ID + "]"
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
        boolean highWatermarkEnabled,
        int lowWatermarkPercent,
        int highWatermarkPercent,
        ByteSizeValue minimumHeapSizeForEnabled
    ) {
        final var clusterSettings = createClusterSettings(
            enabled,
            highWatermarkEnabled,
            lowWatermarkPercent,
            highWatermarkPercent,
            minimumHeapSizeForEnabled
        );
        return new EstimatedHeapUsageAllocationDecider(new EstimatedHeapSettings(clusterSettings), clusterSettings);
    }

    private static ClusterSettings createClusterSettings(
        boolean enabled,
        boolean highWatermarkEnabled,
        int lowWatermarkPercent,
        int highWatermarkPercent,
        ByteSizeValue minimumHeapSizeForEnabled
    ) {
        return new ClusterSettings(
            Settings.builder()
                .put(AbstractEstimatedHeapAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), minimumHeapSizeForEnabled)
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
                AbstractEstimatedHeapAllocationDecider.MINIMUM_LOGGING_INTERVAL,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK,
                EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK_ENABLED,
                AbstractEstimatedHeapAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT
            )
        );
    }

    private static ShardRouting createShardRouting() {
        return ShardRouting.newUnassigned(
            new ShardId(randomIdentifier(), IndexMetadata.INDEX_UUID_NA_VALUE, between(0, 2)),
            true,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE,
            TestShardRouting.buildUnassignedInfo("auto generated for test"),
            ShardRouting.Role.INDEX_ONLY,
            TestShardRouting.buildRecoveryPriority(ShardRoutingState.UNASSIGNED, false)
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

    private ClusterInfo createClusterInfoWithGenNodeAndShardHeap(
        Map<String, Long> nodeEstimatedHeapUsagePercent,
        Supplier<ByteSizeValue> totalHeapSizeSupplier,
        Map<ShardId, ShardAndIndexHeapUsage> shardHeapUsages
    ) {
        return ClusterInfo.builder()
            .nodeHeapMetrics(
                nodeEstimatedHeapUsagePercent.entrySet()
                    .stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> createNodeHeapMetrics(entry.getKey(), entry.getValue(), totalHeapSizeSupplier.get())
                        )
                    )
            )
            .estimatedShardHeapUsages(shardHeapUsages)
            .build();
    }

    private ClusterInfo createClusterInfoWithHeapUsage(
        Map<String, NodeHeapMetrics> nodeHeapMetrics,
        Map<ShardId, ShardAndIndexHeapUsage> shardHeapUsages
    ) {
        return ClusterInfo.builder().nodeHeapMetrics(nodeHeapMetrics).estimatedShardHeapUsages(shardHeapUsages).build();
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
            .add(newNode(NODE_ID, NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode(OTHER_NODE_ID, OTHER_NODE_ID, Set.of(DiscoveryNodeRole.INDEX_ROLE)))
            .add(newNode(SEARCH_NODE_ID, SEARCH_NODE_ID, Set.of(DiscoveryNodeRole.SEARCH_ROLE)));
    }

    private NodeHeapMetrics createNodeHeapMetrics(String nodeId, long usagePercent, ByteSizeValue totalHeapSize) {
        final var totalInBytes = totalHeapSize.getBytes();
        final var usedInBytes = (long) Math.floor(totalInBytes * usagePercent / 100.0d);
        return new NodeHeapMetrics(nodeId, totalInBytes, new NodeHeapEstimates(usedInBytes, randomLongBetween(0, usedInBytes)));
    }

    private Map<ShardId, ShardAndIndexHeapUsage> createShardAndIndexHeapUsageMap(ShardId shardId, long additionalBytes) {
        return Map.of(shardId, new ShardAndIndexHeapUsage(additionalBytes / 2, additionalBytes / 2));
    }
}
