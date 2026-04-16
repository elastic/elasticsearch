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

package org.elasticsearch.xpack.stateless.memory;

import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
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

    /**
     * Verifies that {@link StatelessMemoryMetricsService#computeIndexHeapUsage} and
     * {@link StatelessMemoryMetricsService#computeShardHeapUsage} do not diverge from what is used internally in the
     * {@link StatelessMemoryMetricsService}'s node-level heap usage calculations (routing placement, same rules as
     * {@link StatelessMemoryMetricsService#getPerNodeMemoryMetrics(ClusterState)}).
     */
    private void compareAgainstSumOfIndividualShards(StatelessMemoryMetricsService service, ClusterState clusterState) {
        final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState);
        final Map<String, Long> perNodeOnlyIndexAndShardMemoryUsage = new HashMap<>(perNodeMemoryMetrics.size());
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
            }
        }

        for (var nodeMetrics : perNodeMemoryMetrics.entrySet()) {
            final long mergeMemoryEstimate = service.mergeMemoryEstimation();
            final long minimumRequiredHeapForHandlingLargeIndexingOps = service.minimumRequiredHeapForAcceptingLargeIndexingOps();
            final long indicesAndWorkloadOverheads = service.getNodeBaseHeapEstimateInBytes();
            final long miscNodeUsage = mergeMemoryEstimate + minimumRequiredHeapForHandlingLargeIndexingOps + indicesAndWorkloadOverheads;
            final long indexAndShardOnly = perNodeOnlyIndexAndShardMemoryUsage.getOrDefault(nodeMetrics.getKey(), 0L);
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
                nodeMetrics.getValue(),
                allOf(
                    greaterThanOrEqualTo(indexAndShardOnly + miscNodeUsage),
                    // The reported total postings per node is actually the max across all nodes, so there is no way to account for that
                    // in the sum of shards+indices per node heap calculation. Therefore, here we ensure the two calculated values are
                    // within a difference of the max total postings per node.
                    lessThanOrEqualTo(indexAndShardOnly + miscNodeUsage + getLastMaxTotalPostingsInMemoryBytes(service))
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

        final Map<String, Long> perNode = service.getPerNodeMemoryMetrics(clusterState);
        final long deltaForShard = service.computeShardHeapUsage(metricsWithWrongReporter) + service.computeIndexHeapUsage(
            metricsWithWrongReporter
        ) - metricsWithWrongReporter.getPostingsInMemoryBytes();
        assertThat(perNode.get(onlyShard.currentNodeId()) - perNode.get(nodeWithoutShard.getId()), equalTo(deltaForShard));
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

        final Map<String, Long> perNodeStarted = service.getPerNodeMemoryMetrics(startedState);

        // Relocate the shard
        final RoutingNodes routingNodes = startedState.getRoutingNodes().mutableCopy();
        routingNodes.relocateShard(onlyShard, otherNode.getId(), randomNegativeLong(), "relocate", RoutingChangesObserver.NOOP);
        final GlobalRoutingTable globalRoutingTable = startedState.globalRoutingTable().rebuild(routingNodes, startedState.metadata());
        final ClusterState relocatingState = ClusterState.builder(startedState).routingTable(globalRoutingTable).incrementVersion().build();

        service.clusterChanged(new ClusterChangedEvent("relocate", relocatingState, startedState));
        final Map<String, Long> perNodeRelocating = service.getPerNodeMemoryMetrics(relocatingState);

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
        final Map<String, Long> perNode = service.getPerNodeMemoryMetrics(clusterState);

        assertThat(perNode.size(), equalTo(3));
        assertThat(nodesWithNoShards, hasSize(2));
        // All nodes have an estimate, empty nodes have a smaller estimate than the host node
        for (String emptyNodeId : nodesWithNoShards) {
            assertThat(perNode.get(onlyShard.currentNodeId()), greaterThan(perNode.get(emptyNodeId)));
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
        // Record the baseline heap usage for node 0 and 1, before any additional information is received
        {
            Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            node0EstimateBeforeUpdate = perNodeMemoryMetrics.get(node0.getId());
            node1EstimateBeforeUpdate = perNodeMemoryMetrics.get(node1.getId());
        }

        // We receive a shard mappings update from node 0
        final var node0MetricsUpdate = randomMemoryMetrics(node0, clusterState1);
        final var node0PostingsSize = node0MetricsUpdate.values().stream().mapToLong(ShardMappingSize::postingsInMemoryBytes).sum();
        service.updateShardsMappingSize(new HeapMemoryUsage(2, node0MetricsUpdate));

        // Node 0 heap estimate should have increased
        // Note that hollow shards can reduce the initial estimate, but we don't test this here
        long node0EstimateAfterUpdate;
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            node0EstimateAfterUpdate = perNodeMemoryMetrics.get(node0.getId());
            assertThat(node0EstimateAfterUpdate, greaterThan(node0EstimateBeforeUpdate));
            // PostingsMemorySize is the max across all nodes, so node1's estimate should have increased by that amount
            assertThat(perNodeMemoryMetrics.get(node1.getId()), equalTo(node1EstimateBeforeUpdate + node0PostingsSize));
        }

        // We receive a shard mappings update from node 1
        final var node1MetricsUpdate = randomMemoryMetrics(node1, clusterState1);
        final var node1PostingsSize = node1MetricsUpdate.values().stream().mapToLong(ShardMappingSize::postingsInMemoryBytes).sum();
        service.updateShardsMappingSize(new HeapMemoryUsage(1, node1MetricsUpdate));

        // Node 1 heap estimate should have increased
        final long node1EstimateAfterUpdate;
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            // PostingsMemorySize is the max across all nodes so that node0's estimate can increase if node1 has larger postings size
            if (node0PostingsSize < node1PostingsSize) {
                node0EstimateAfterUpdate += node1PostingsSize - node0PostingsSize;
            }
            assertThat(perNodeMemoryMetrics.get(node0.getId()), equalTo(node0EstimateAfterUpdate));
            node1EstimateAfterUpdate = perNodeMemoryMetrics.get(node1.getId());
            assertThat(node1EstimateAfterUpdate, greaterThan(node1EstimateBeforeUpdate));
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

        // All heap estimates should have increased
        final long node0EstimateAfterMergeEstimate, node1EstimateAfterMergeEstimate;
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));

            node0EstimateAfterMergeEstimate = perNodeMemoryMetrics.get(node0.getId());
            assertThat(node0EstimateAfterMergeEstimate - node0EstimateAfterUpdate, equalTo(node0MergeEstimate));

            node1EstimateAfterMergeEstimate = perNodeMemoryMetrics.get(node1.getId());
            assertThat(node1EstimateAfterMergeEstimate - node1EstimateAfterUpdate, equalTo(node0MergeEstimate));
        }

        // update indexing operations heap memory requirement
        final long indexingOperationsHeapMemoryRequirements = randomLongBetween(1_000, 100_000);
        service.updateIndexingOperationsHeapMemoryRequirements(indexingOperationsHeapMemoryRequirements);

        // All nodes' heap estimate should have increased
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1);
            compareAgainstSumOfIndividualShards(service, clusterState1);
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            assertThat(
                perNodeMemoryMetrics.get(node0.getId()) - node0EstimateAfterMergeEstimate,
                equalTo(indexingOperationsHeapMemoryRequirements)
            );
            assertThat(
                perNodeMemoryMetrics.get(node1.getId()) - node1EstimateAfterMergeEstimate,
                equalTo(indexingOperationsHeapMemoryRequirements)
            );
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
                    UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                    node.getId()
                )
            );
        });
        return result;
    }
}
