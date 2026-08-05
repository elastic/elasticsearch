/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeHeapEstimates;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.MetricQuality;
import org.junit.Before;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.indices.ShardLimitValidator.SETTING_CLUSTER_MAX_SHARDS_PER_NODE;
import static org.elasticsearch.xpack.stateless.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
import static org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsServiceTestUtils.getLastMaxTotalPostingsInMemoryBytes;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Tests for {@link StatelessMemoryMetricsService}, focusing on {@code getPerNodeMemoryMetrics} and {@code getShardHeapUsages}.
 */
public class StatelessMemoryMetricsServiceTests extends ESTestCase {

    private ClusterSettings clusterSettings;
    private StatelessMemoryMetricsService service;

    private static Set<Setting<?>> allSettings;

    @Before
    public void init() {
        allSettings = Stream.concat(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS.stream(),
            Stream.of(
                StatelessMemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING,
                StatelessMemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
                StatelessMemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
                StatelessMemoryMetricsService.MERGE_MEMORY_ESTIMATE_ENABLED_SETTING,
                StatelessMemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING,
                StatelessMemoryMetricsService.SELF_REPORTED_SHARD_MEMORY_OVERHEAD_ENABLED_SETTING,
                StatelessMemoryMetricsService.ADAPTIVE_SHARD_MEMORY_ESTIMATION_MIN_THRESHOLD_ENABLED_SETTING,
                SETTING_CLUSTER_MAX_SHARDS_PER_NODE
            )
        ).collect(Collectors.toSet());

        clusterSettings = new ClusterSettings(Settings.EMPTY, allSettings);
        service = new StatelessMemoryMetricsService(System::nanoTime, clusterSettings);
    }

    public void testGetShardHeapUsages() {
        // Set up shard memory metrics
        var shardMemoryMetrics1 = new StatelessMemoryMetricsService.ShardMemoryMetrics(
            // Limit the range of values for the metrics, so that adding and multiplying doesn't cause type overflow results.
            randomLongBetween(100, 10_000),
            randomIntBetween(100, 10_000),
            randomIntBetween(100, 10_000),
            randomLongBetween(100, 10_000),
            randomLongBetween(100, 10_000),
            randomLongBetween(100, 10_000),
            randomLongBetween(1, Long.MAX_VALUE),
            randomLongBetween(100, 10_000),
            MetricQuality.EXACT,
            "node-0",
            System.nanoTime()
        );
        var shardMemoryMetrics2 = new StatelessMemoryMetricsService.ShardMemoryMetrics(
            // Limit the range of values for the metrics, so that adding and multiplying doesn't cause type overflow results.
            randomLongBetween(100, 10_000),
            randomIntBetween(100, 10_000),
            randomIntBetween(100, 10_000),
            randomLongBetween(100, 10_000),
            randomLongBetween(100, 10_000),
            randomLongBetween(100, 10_000),
            randomLongBetween(1, Long.MAX_VALUE),
            randomLongBetween(100, 10_000),
            MetricQuality.EXACT,
            "node-0",
            System.nanoTime()
        );
        var shardId1 = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 2));
        var shardId2 = new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 2));

        // Add the shard memory metrics to the memory service.
        service.getShardMemoryMetrics().put(shardId1, shardMemoryMetrics1);
        service.getShardMemoryMetrics().put(shardId2, shardMemoryMetrics2);

        // Verify that the memory service correctly returns all the per shard memory metrics.
        var shardHeapUsages = service.getShardHeapUsages();
        assertThat(shardHeapUsages.get(shardId1).shardHeapUsageBytes(), equalTo(service.computeShardHeapUsage(shardMemoryMetrics1)));
        assertThat(shardHeapUsages.get(shardId1).indexHeapUsageBytes(), equalTo(service.computeIndexHeapUsage(shardMemoryMetrics1)));
        assertThat(shardHeapUsages.get(shardId2).shardHeapUsageBytes(), equalTo(service.computeShardHeapUsage(shardMemoryMetrics2)));
        assertThat(shardHeapUsages.get(shardId2).indexHeapUsageBytes(), equalTo(service.computeIndexHeapUsage(shardMemoryMetrics2)));
    }

    public void testShardHeapUsageIncludesPointsMemory() {
        // these settings force us to use the adaptive memory calculation which will count the points memory in the heap usage
        clusterSettings.applySettings(
            Settings.builder()
                .put(StatelessMemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING.getKey(), "-1b")
                .put(StatelessMemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING.getKey(), "0%")
                .build()
        );
        final ClusterState clusterState = randomInitialTwoNodeClusterState(4);
        final DiscoveryNode node0 = clusterState.nodes().get("node_0");
        service.clusterChanged(new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE));

        final Map<ShardId, ShardMappingSize> metricsWithoutPoints = createShardMappingMetricsWithPointsInMemory(
            randomMemoryMetrics(node0, clusterState),
            0L
        );
        service.updateShardsMappingSize(new HeapMemoryUsage(1, metricsWithoutPoints));
        final NodeHeapEstimates node0EstimateWithoutPoints = service.getPerNodeMemoryMetrics(clusterState).get(node0.getId());

        final long pointsInMemoryBytes = randomLongBetween(1, 100);
        final Map<ShardId, ShardMappingSize> metricsWithPoints = createShardMappingMetricsWithPointsInMemory(
            metricsWithoutPoints,
            pointsInMemoryBytes
        );
        service.updateShardsMappingSize(new HeapMemoryUsage(2, metricsWithPoints));
        final NodeHeapEstimates node0EstimateWithPoints = service.getPerNodeMemoryMetrics(clusterState).get(node0.getId());

        final long expectedDelta = pointsInMemoryBytes * metricsWithPoints.size();
        assertThat(node0EstimateWithPoints.totalHeapUsage() - node0EstimateWithoutPoints.totalHeapUsage(), equalTo(expectedDelta));
        // Points memory is attributed entirely to the hosted shards, nothing else changed between the two updates
        assertThat(
            node0EstimateWithPoints.hostedShardsHeapUsage() - node0EstimateWithoutPoints.hostedShardsHeapUsage(),
            equalTo(expectedDelta)
        );
    }

    /**
     * Verifies that {@link StatelessMemoryMetricsService#computeIndexHeapUsage} and
     * {@link StatelessMemoryMetricsService#computeShardHeapUsage} do not diverge from what is used internally in the
     * {@link StatelessMemoryMetricsService}'s node-level heap usage calculations (routing placement, same rules as
     * {@link StatelessMemoryMetricsService#getPerNodeMemoryMetrics(ClusterState)}).
     */
    private void compareAgainstSumOfIndividualShards(StatelessMemoryMetricsService service, ClusterState clusterState) {
        final Map<String, NodeHeapEstimates> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState);
        final Map<String, Long> perNodeOnlyIndexAndShardMemoryUsage = new HashMap<>(perNodeMemoryMetrics.size());
        final Map<String, Long> perNodeHostedShardsHeapUsage = new HashMap<>(perNodeMemoryMetrics.size());
        final Map<String, Set<String>> perNodeSeenIndices = new HashMap<>(perNodeMemoryMetrics.size());

        final long nowNanos = 0L;

        for (RoutingNode routingNode : clusterState.getRoutingNodes()) {
            final String nodeId = routingNode.nodeId();
            final DiscoveryNode discoveryNode = clusterState.nodes().get(nodeId);
            if (discoveryNode == null || discoveryNode.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE) == false) {
                continue;
            }

            for (ShardRouting shard : routingNode) {
                if (shard.primary() == false) {
                    continue;
                }
                if (shard.active() == false && shard.initializing() == false) {
                    continue;
                }
                final ShardId shardId = shard.shardId();
                StatelessMemoryMetricsService.ShardMemoryMetrics shardMemoryMetrics = service.getShardMemoryMetrics().get(shardId);
                if (shardMemoryMetrics == null) {
                    shardMemoryMetrics = service.newUninitialisedShardMemoryMetrics(nowNanos);
                }
                final long shardHeap = service.computeShardHeapUsage(shardMemoryMetrics);
                final var seenIndices = perNodeSeenIndices.computeIfAbsent(nodeId, key -> new HashSet<>());

                long indexHeap = 0L;
                if (seenIndices.add(shardId.getIndexName())) {
                    indexHeap = service.computeIndexHeapUsage(shardMemoryMetrics);
                }

                var perShardUsages = service.getShardHeapUsages();
                if (perShardUsages.containsKey(shardId)) {
                    assertThat(perShardUsages.get(shardId).shardHeapUsageBytes(), equalTo(shardHeap));
                    assertThat(
                        perShardUsages.get(shardId).indexHeapUsageBytes(),
                        equalTo(service.computeIndexHeapUsage(shardMemoryMetrics))
                    );
                }

                perNodeOnlyIndexAndShardMemoryUsage.merge(nodeId, shardHeap + indexHeap, Long::sum);
                perNodeHostedShardsHeapUsage.merge(nodeId, shardHeap, Long::sum);
            }
        }

        for (var nodeMetrics : perNodeMemoryMetrics.entrySet()) {
            final long mergeMemoryEstimate = service.mergeMemoryEstimation();
            final long minimumRequiredHeapForHandlingLargeIndexingOps = service.minimumRequiredHeapForAcceptingLargeIndexingOps();
            final long indicesAndWorkloadOverheads = service.getNodeBaseHeapEstimateInBytes();
            final long miscNodeUsage = mergeMemoryEstimate + minimumRequiredHeapForHandlingLargeIndexingOps + indicesAndWorkloadOverheads;
            final long indexAndShardOnly = perNodeOnlyIndexAndShardMemoryUsage.getOrDefault(nodeMetrics.getKey(), 0L);
            final long hostedShardsOnly = perNodeHostedShardsHeapUsage.getOrDefault(nodeMetrics.getKey(), 0L);
            assertThat(
                "Heap usage for node "
                    + nodeMetrics.getKey()
                    + " is "
                    + nodeMetrics.getValue()
                    + "; misc heap usage for the node is "
                    + miscNodeUsage
                    + "; summed index and shard heap usage for the node is: "
                    + indexAndShardOnly
                    + "; postings overhead per node is: "
                    + getLastMaxTotalPostingsInMemoryBytes(service),
                nodeMetrics.getValue().totalHeapUsage(),
                allOf(
                    greaterThanOrEqualTo(indexAndShardOnly + miscNodeUsage),
                    // The reported total postings per node is actually the max across all nodes, so there is no way to account for that
                    // in the sum of shards+indices per node heap calculation. Therefore, here we ensure the two calculated values are
                    // within a difference of the max total postings per node.
                    lessThanOrEqualTo(indexAndShardOnly + miscNodeUsage + getLastMaxTotalPostingsInMemoryBytes(service))
                )
            );
            // The hosted-shards-only estimate excludes index-level mapping size and the node-base/merge/indexing-ops overheads,
            // see EstimatedHeapUsageBuilder#getHeapEstimate.
            assertThat(
                "Hosted shards heap usage for node "
                    + nodeMetrics.getKey()
                    + " is "
                    + nodeMetrics.getValue().hostedShardsHeapUsage()
                    + "; summed hosted-shards-only heap usage for the node is: "
                    + hostedShardsOnly
                    + "; postings overhead per node is: "
                    + getLastMaxTotalPostingsInMemoryBytes(service),
                nodeMetrics.getValue().hostedShardsHeapUsage(),
                // The reported total postings per node is actually the max across all nodes, so there is no way to account for that
                // in the sum of shards+indices per node heap calculation. Therefore, here we ensure the two calculated values are
                // within a difference of the max total postings per node.
                allOf(
                    greaterThanOrEqualTo(hostedShardsOnly),
                    lessThanOrEqualTo(hostedShardsOnly + getLastMaxTotalPostingsInMemoryBytes(service))
                )
            );
        }
    }

    /**
     * Per-node heap follows primary routing, not {@link StatelessMemoryMetricsService.ShardMemoryMetrics#getMetricShardNodeId()}.
     */
    public void testPerNodeMemoryMetricsUsesPrimaryRoutingNotMetricReporterNode() {
        final String indexName = randomIdentifier();

        ClusterState clusterState = ClusterStateCreationUtils.state(indexName, 2, 1);
        final ShardRouting onlyShard = clusterState.globalRoutingTable()
            .routingTable(ProjectId.DEFAULT)
            .index(indexName)
            .shard(0)
            .primaryShard();

        final DiscoveryNode nodeWithoutShard = clusterState.nodes()
            .stream()
            .filter(n -> n.getId().equals(onlyShard.currentNodeId()) == false)
            .findFirst()
            .orElseThrow();

        service.clusterChanged(new ClusterChangedEvent("init", clusterState, ClusterState.EMPTY_STATE));
        final StatelessMemoryMetricsService.ShardMemoryMetrics metricsWithWrongReporter = randomShardMemoryMetrics(
            nodeWithoutShard.getId()
        );
        service.getShardMemoryMetrics().put(onlyShard.shardId(), metricsWithWrongReporter);

        final Map<String, NodeHeapEstimates> perNode = service.getPerNodeMemoryMetrics(clusterState);
        final long deltaForShard = service.computeShardHeapUsage(metricsWithWrongReporter) + service.computeIndexHeapUsage(
            metricsWithWrongReporter
        ) - metricsWithWrongReporter.getPostingsInMemoryBytes();
        assertThat(
            perNode.get(onlyShard.currentNodeId()).totalHeapUsage() - perNode.get(nodeWithoutShard.getId()).totalHeapUsage(),
            equalTo(deltaForShard)
        );
        // The difference for the hosted-shards estimate should be just the size of the shard (no index metadata)
        assertThat(
            perNode.get(onlyShard.currentNodeId()).hostedShardsHeapUsage() - perNode.get(nodeWithoutShard.getId()).hostedShardsHeapUsage(),
            equalTo(service.computeShardHeapUsage(metricsWithWrongReporter))
        );
    }

    /**
     * While a primary is relocating, {@link StatelessMemoryMetricsService#getPerNodeMemoryMetrics} only counts the heap usage
     * on the source node. The simulator will simulate the successful completion of the relocation which will deduct
     * from the source and add to the target.
     */
    public void testPerNodeMemoryMetricsCountsRelocatingPrimaryOnlyOnSource() {
        final String indexName = randomIdentifier();
        final ClusterState state0 = ClusterStateCreationUtils.state(indexName, 2, 1);
        // ensure that the local node is master so we don't clear the shardMemoryMetrics on cluster state updates
        final ClusterState startedState = ClusterState.builder(state0)
            .nodes(DiscoveryNodes.builder(state0.nodes()).masterNodeId(state0.nodes().getLocalNodeId()).build())
            .build();
        final ShardRouting onlyShard = startedState.globalRoutingTable()
            .routingTable(ProjectId.DEFAULT)
            .index(indexName)
            .shard(0)
            .primaryShard();
        assert onlyShard.started() : "Expect shard to be started";
        final String originalNodeId = onlyShard.currentNodeId();
        final DiscoveryNode otherNode = startedState.nodes()
            .stream()
            .filter(n -> n.getId().equals(originalNodeId) == false)
            .findFirst()
            .orElseThrow();

        service.clusterChanged(new ClusterChangedEvent("init", startedState, ClusterState.EMPTY_STATE));
        final StatelessMemoryMetricsService.ShardMemoryMetrics metrics = randomShardMemoryMetrics(randomIdentifier()); // metricShardNodeId
                                                                                                                       // doesn't
        // matter
        service.getShardMemoryMetrics().put(onlyShard.shardId(), metrics);

        final Map<String, NodeHeapEstimates> perNodeStarted = service.getPerNodeMemoryMetrics(startedState);

        // Relocate the shard
        final RoutingNodes routingNodes = startedState.getRoutingNodes().mutableCopy();
        routingNodes.relocateShard(onlyShard, otherNode.getId(), randomNegativeLong(), "relocate", RoutingChangesObserver.NOOP);
        final GlobalRoutingTable globalRoutingTable = startedState.globalRoutingTable().rebuild(routingNodes, startedState.metadata());
        final ClusterState relocatingState = ClusterState.builder(startedState).routingTable(globalRoutingTable).incrementVersion().build();

        service.clusterChanged(new ClusterChangedEvent("relocate", relocatingState, startedState));
        final Map<String, NodeHeapEstimates> perNodeRelocating = service.getPerNodeMemoryMetrics(relocatingState);

        // The heap usage of the target node should remain the same, the original node should be unchanged
        assertThat(perNodeRelocating.get(otherNode.getId()), equalTo(perNodeStarted.get(otherNode.getId())));
        assertThat(perNodeRelocating.get(originalNodeId), equalTo(perNodeStarted.get(originalNodeId)));
    }

    private static StatelessMemoryMetricsService.ShardMemoryMetrics randomShardMemoryMetrics(String metricShardNodeId) {
        return new StatelessMemoryMetricsService.ShardMemoryMetrics(
            randomLongBetween(80_000, 120_000),
            randomIntBetween(2, 6),
            randomIntBetween(8, 20),
            randomLongBetween(5_000, 15_000),
            randomLongBetween(100, 500),
            randomLongBetween(5_000, 15_000),
            UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
            randomNonNegativeLong(),
            randomFrom(MetricQuality.values()),
            metricShardNodeId,
            System.nanoTime()
        );
    }

    /**
     * Indexing nodes with no assigned active/initializing primaries still receive a per-node estimate (base, merge, indexing-ops buffer,
     * and global max postings), matching {@link StatelessMemoryMetricsService#getPerNodeMemoryMetrics}.
     */
    public void testPerNodeMemoryMetricsIncludesIndexingNodesWithNoAssignedPrimaries() {
        final String indexName = randomIdentifier();
        final ClusterState clusterState = ClusterStateCreationUtils.state(indexName, 3, 1);
        final ShardRouting onlyShard = clusterState.globalRoutingTable()
            .routingTable(ProjectId.DEFAULT)
            .index(indexName)
            .shard(0)
            .primaryShard();
        final Set<String> nodesWithNoShards = clusterState.nodes()
            .stream()
            .filter(n -> n.getId().equals(onlyShard.currentNodeId()) == false)
            .map(DiscoveryNode::getId)
            .collect(Collectors.toSet());

        service.clusterChanged(new ClusterChangedEvent("init", clusterState, ClusterState.EMPTY_STATE));
        final Map<String, NodeHeapEstimates> perNode = service.getPerNodeMemoryMetrics(clusterState);

        assertThat(perNode.size(), equalTo(3));
        assertThat(nodesWithNoShards, hasSize(2));
        // All nodes have an estimate, empty nodes have a smaller estimate than the host node
        for (String emptyNodeId : nodesWithNoShards) {
            assertThat(perNode.get(onlyShard.currentNodeId()).totalHeapUsage(), greaterThan(perNode.get(emptyNodeId).totalHeapUsage()));
            // Empty nodes host no shards, so their hosted-shards-only estimate is zero, unlike the host node's
            assertThat(perNode.get(emptyNodeId).hostedShardsHeapUsage(), equalTo(0L));
            assertThat(perNode.get(onlyShard.currentNodeId()).hostedShardsHeapUsage(), greaterThan(0L));
        }
    }

    public void testEstimatedHeapMemoryCalculations() {
        ClusterState clusterState1 = randomInitialTwoNodeClusterState(4);
        var discoveryNodes = clusterState1.getNodes();
        var node0 = discoveryNodes.get("node_0");
        var node1 = discoveryNodes.get("node_1");
        service.clusterChanged(new ClusterChangedEvent("test", clusterState1, ClusterState.EMPTY_STATE));

        final long node0EstimateBeforeUpdate;
        final long node1EstimateBeforeUpdate;
        final long node0HostedShardsBeforeUpdate;
        final long node1HostedShardsBeforeUpdate;
        // Record the baseline heap usage for node 0 and 1, before any additional information is received
        {
            Map<String, NodeHeapEstimates> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            node0EstimateBeforeUpdate = perNodeMemoryMetrics.get(node0.getId()).totalHeapUsage();
            node1EstimateBeforeUpdate = perNodeMemoryMetrics.get(node1.getId()).totalHeapUsage();
            node0HostedShardsBeforeUpdate = perNodeMemoryMetrics.get(node0.getId()).hostedShardsHeapUsage();
            node1HostedShardsBeforeUpdate = perNodeMemoryMetrics.get(node1.getId()).hostedShardsHeapUsage();
        }

        // We receive a shard mappings update from node 0
        final var node0MetricsUpdate = randomMemoryMetrics(node0, clusterState1);
        final var node0PostingsSize = node0MetricsUpdate.values().stream().mapToLong(ShardMappingSize::postingsInMemoryBytes).sum();
        service.updateShardsMappingSize(new HeapMemoryUsage(2, node0MetricsUpdate));

        // Node 0 heap estimate should have increased
        // Note that hollow shards can reduce the initial estimate, but we don't test this here
        long node0EstimateAfterUpdate;
        {
            final Map<String, NodeHeapEstimates> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            node0EstimateAfterUpdate = perNodeMemoryMetrics.get(node0.getId()).totalHeapUsage();
            assertThat(node0EstimateAfterUpdate, greaterThan(node0EstimateBeforeUpdate));
            // PostingsMemorySize is the max across all nodes, so node1's estimate should have increased by that amount
            assertThat(perNodeMemoryMetrics.get(node1.getId()).totalHeapUsage(), equalTo(node1EstimateBeforeUpdate + node0PostingsSize));
            assertThat(
                perNodeMemoryMetrics.get(node0.getId()).hostedShardsHeapUsage(),
                equalTo(node0HostedShardsBeforeUpdate + node0PostingsSize)
            );
            // hosted shards estimate doesn't take the maximized postings value, it should be unchanged on node 1
            assertThat(perNodeMemoryMetrics.get(node1.getId()).hostedShardsHeapUsage(), equalTo(node1HostedShardsBeforeUpdate));
        }

        // We receive a shard mappings update from node 1
        final var node1MetricsUpdate = randomMemoryMetrics(node1, clusterState1);
        final var node1PostingsSize = node1MetricsUpdate.values().stream().mapToLong(ShardMappingSize::postingsInMemoryBytes).sum();
        service.updateShardsMappingSize(new HeapMemoryUsage(1, node1MetricsUpdate));

        // Node 1 heap estimate should have increased
        final long node1EstimateAfterUpdate;
        final long node0HostedShardsAfterBothUpdates;
        final long node1HostedShardsAfterBothUpdates;
        {
            final Map<String, NodeHeapEstimates> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            // PostingsMemorySize is the max across all nodes so that node0's estimate can increase if node1 has larger postings size
            if (node0PostingsSize < node1PostingsSize) {
                node0EstimateAfterUpdate += node1PostingsSize - node0PostingsSize;
            }
            assertThat(perNodeMemoryMetrics.get(node0.getId()).totalHeapUsage(), equalTo(node0EstimateAfterUpdate));
            node1EstimateAfterUpdate = perNodeMemoryMetrics.get(node1.getId()).totalHeapUsage();
            assertThat(node1EstimateAfterUpdate, greaterThan(node1EstimateBeforeUpdate));
            node0HostedShardsAfterBothUpdates = perNodeMemoryMetrics.get(node0.getId()).hostedShardsHeapUsage();
            node1HostedShardsAfterBothUpdates = perNodeMemoryMetrics.get(node1.getId()).hostedShardsHeapUsage();
            assertThat(node0HostedShardsAfterBothUpdates, greaterThan(node0HostedShardsBeforeUpdate));
            assertThat(node1HostedShardsAfterBothUpdates, greaterThan(node1HostedShardsBeforeUpdate));
        }

        // we receive a merge estimate from node 0
        final long node0MergeEstimate = randomLongBetween(10_000, 100_000);
        service.updateMergeMemoryEstimate(
            new StatelessMemoryMetricsService.ShardMergeMemoryEstimatePublication(
                randomLongBetween(100, 1000),
                node0.getEphemeralId(),
                new StatelessMemoryMetricsService.ShardMergeMemoryEstimate(randomIdentifier(), node0MergeEstimate)
            )
        );

        // All heap estimates should have increased, but hostedShardsHeapUsage is unaffected: merge memory is not a hosted-shard component
        final long node0EstimateAfterMergeEstimate, node1EstimateAfterMergeEstimate;
        {
            final Map<String, NodeHeapEstimates> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));

            node0EstimateAfterMergeEstimate = perNodeMemoryMetrics.get(node0.getId()).totalHeapUsage();
            assertThat(node0EstimateAfterMergeEstimate - node0EstimateAfterUpdate, equalTo(node0MergeEstimate));

            node1EstimateAfterMergeEstimate = perNodeMemoryMetrics.get(node1.getId()).totalHeapUsage();
            assertThat(node1EstimateAfterMergeEstimate - node1EstimateAfterUpdate, equalTo(node0MergeEstimate));

            assertThat(perNodeMemoryMetrics.get(node0.getId()).hostedShardsHeapUsage(), equalTo(node0HostedShardsAfterBothUpdates));
            assertThat(perNodeMemoryMetrics.get(node1.getId()).hostedShardsHeapUsage(), equalTo(node1HostedShardsAfterBothUpdates));
        }

        // update indexing operations heap memory requirement
        final long indexingOperationsHeapMemoryRequirements = randomLongBetween(1_000, 100_000);
        service.updateIndexingOperationsHeapMemoryRequirements(indexingOperationsHeapMemoryRequirements);

        // All nodes' heap estimate should have increased, but hostedShardsHeapUsage is unaffected: indexing-ops overhead is not a
        // hosted-shard component
        {
            final Map<String, NodeHeapEstimates> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            assertThat(
                perNodeMemoryMetrics.get(node0.getId()).totalHeapUsage() - node0EstimateAfterMergeEstimate,
                equalTo(indexingOperationsHeapMemoryRequirements)
            );
            assertThat(
                perNodeMemoryMetrics.get(node1.getId()).totalHeapUsage() - node1EstimateAfterMergeEstimate,
                equalTo(indexingOperationsHeapMemoryRequirements)
            );
            assertThat(perNodeMemoryMetrics.get(node0.getId()).hostedShardsHeapUsage(), equalTo(node0HostedShardsAfterBothUpdates));
            assertThat(perNodeMemoryMetrics.get(node1.getId()).hostedShardsHeapUsage(), equalTo(node1HostedShardsAfterBothUpdates));
        }
    }

    private ClusterState randomInitialTwoNodeClusterState(int numberOfIndices) {
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node_0"))
            .add(DiscoveryNodeUtils.create("node_1"))
            .localNodeId("node_0")
            .masterNodeId("node_0")
            .build();
        String[] indices = IntStream.range(0, numberOfIndices).mapToObj(i -> randomIdentifier()).toArray(String[]::new);
        Tuple<ProjectMetadata.Builder, RoutingTable.Builder> projectAndRt = ClusterStateCreationUtils
            .projectWithAssignedPrimariesAndReplicas(ProjectId.DEFAULT, indices, 2, 0, discoveryNodes);
        return ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .routingTable(GlobalRoutingTable.builder().put(ProjectId.DEFAULT, projectAndRt.v2()).build())
            .metadata(Metadata.builder().put(projectAndRt.v1()))
            .build();
    }

    private Map<ShardId, ShardMappingSize> randomMemoryMetrics(DiscoveryNode node, ClusterState clusterState) {
        Map<ShardId, ShardMappingSize> result = new HashMap<>();
        Map<Index, Long> indexMappingSizes = new HashMap<>();
        clusterState.getRoutingNodes().node(node.getId()).forEach(r -> {
            long mappingSize = indexMappingSizes.computeIfAbsent(
                r.shardId().getIndex(),
                i -> ByteSizeValue.ofKb(randomLongBetween(1, 200)).getBytes()
            );
            result.put(
                r.shardId(),
                new ShardMappingSize(
                    mappingSize,
                    randomIntBetween(1, 1_000),
                    randomIntBetween(1, 100),
                    randomLongBetween(1, 100),
                    randomIntBetween(1, 100),
                    randomIntBetween(1, 100),
                    UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                    node.getId()
                )
            );
        });
        return result;
    }

    private static Map<ShardId, ShardMappingSize> createShardMappingMetricsWithPointsInMemory(
        Map<ShardId, ShardMappingSize> metrics,
        long pointsInMemoryBytes
    ) {
        final Map<ShardId, ShardMappingSize> result = new HashMap<>();
        metrics.forEach(
            (shardId, shardMappingSize) -> result.put(
                shardId,
                new ShardMappingSize(
                    shardMappingSize.mappingSizeInBytes(),
                    shardMappingSize.numSegments(),
                    shardMappingSize.totalFields(),
                    shardMappingSize.postingsInMemoryBytes(),
                    shardMappingSize.liveDocsBytes(),
                    pointsInMemoryBytes,
                    shardMappingSize.shardMemoryOverheadBytes(),
                    shardMappingSize.nodeId()
                )
            )
        );
        return result;
    }

}
