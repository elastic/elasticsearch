/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import com.carrotsearch.hppc.ObjectIntHashMap;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

/**
 * Unit tests for {@link IndexBalanceMetrics}.
 */
public class IndexBalanceMetricsTests extends ESTestCase {

    public void testComputeHistogramCounts() {
        final int numIndices = randomCount();
        final int numIndexNodes = randomNodeCount();
        final int numSearchNodes = randomNodeCount();
        final int maxShards = between(0, 40);

        final var indexNames = randomIndexNames(numIndices);
        final var primaries = randomFrequencyMap("index_", numIndexNodes, maxShards, 1);

        final var state = buildState(indexNames, primaries, numSearchNodes);
        final var result = new IndexBalanceMetrics().compute(state);

        assertThat(sum(result.primaryBalanceHistogram()), equalTo(numIndices));
        assertThat(sum(result.replicaBalanceHistogram()), equalTo(numIndices));
    }

    public void testUnassignedShardsSkipped() {
        final var indexName = randomIndexName();
        final var index = new Index(indexName, "_na_");
        final int numAssigned = between(1, 5);
        final int numUnassigned = between(1, 5);
        final int totalShards = numAssigned + numUnassigned;

        final var routingBuilder = IndexRoutingTable.builder(index);
        for (int i = 0; i < numAssigned; i++) {
            routingBuilder.addShard(newShardRouting(new ShardId(index, i), "idx1", true, ShardRoutingState.STARTED));
        }
        for (int i = numAssigned; i < totalShards; i++) {
            routingBuilder.addShard(newShardRouting(new ShardId(index, i), null, true, ShardRoutingState.UNASSIGNED));
        }

        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.builder("idx1").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build()))
            .metadata(
                Metadata.builder().put(IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), totalShards, 0)))
            )
            .routingTable(RoutingTable.builder().add(routingBuilder))
            .build();

        final var result = new IndexBalanceMetrics().compute(state);

        assertThat("one index counted in primary histogram", sum(result.primaryBalanceHistogram()), equalTo(1));
    }

    public void testNonEligibleNodesSkipped() {
        final int numIndexNodes = randomNodeCount();
        final int numSearchNodes = randomNodeCount();
        final int numMLNodes = randomNodeCount();

        final var nodesBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < numIndexNodes; i++) {
            nodesBuilder.add(DiscoveryNodeUtils.builder("index_" + i).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        }
        for (int i = 0; i < numSearchNodes; i++) {
            nodesBuilder.add(DiscoveryNodeUtils.builder("search_" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
        }
        for (int i = 0; i < numMLNodes; i++) {
            nodesBuilder.add(DiscoveryNodeUtils.builder("ml_" + i).roles(Set.of(DiscoveryNodeRole.ML_ROLE)).build());
        }

        final var state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodesBuilder).build();
        final var eligibleMap = IndexBalanceMetrics.buildEligibleNodeMap(
            state.nodes(),
            state.metadata().nodeShutdowns(),
            DiscoveryNodeRole.INDEX_ROLE
        );

        assertThat(eligibleMap.size(), equalTo(numIndexNodes));
        for (int i = 0; i < numMLNodes; i++) {
            assertFalse(eligibleMap.containsKey("ml_" + i));
        }
        for (int i = 0; i < numSearchNodes; i++) {
            assertFalse(eligibleMap.containsKey("search_" + i));
        }
    }

    public void testShuttingDownNodesExcluded() {
        final int numHealthyNodes = randomNodeCount();
        final int numShuttingDown = randomNodeCount();

        final var nodesBuilder = DiscoveryNodes.builder();
        final var shutdownEntries = new HashMap<String, SingleNodeShutdownMetadata>();
        for (int i = 0; i < numHealthyNodes; i++) {
            nodesBuilder.add(DiscoveryNodeUtils.builder("idx_" + i).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        }
        for (int i = 0; i < numShuttingDown; i++) {
            final var nodeId = "shutdown_" + i;
            nodesBuilder.add(DiscoveryNodeUtils.builder(nodeId).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
            shutdownEntries.put(
                nodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(nodeId)
                    .setNodeEphemeralId(nodeId)
                    .setType(SingleNodeShutdownMetadata.Type.SIGTERM)
                    .setReason("test")
                    .setStartedAtMillis(1L)
                    .setGracePeriod(TimeValue.timeValueMinutes(5))
                    .build()
            );
        }

        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, new NodesShutdownMetadata(shutdownEntries)))
            .build();
        final var eligibleMap = IndexBalanceMetrics.buildEligibleNodeMap(
            state.nodes(),
            state.metadata().nodeShutdowns(),
            DiscoveryNodeRole.INDEX_ROLE
        );

        assertThat(eligibleMap.size(), equalTo(numHealthyNodes));
        for (int i = 0; i < numShuttingDown; i++) {
            assertFalse(eligibleMap.containsKey("shutdown_" + i));
        }
    }

    public void testShardsImbalanceRatioEdgeCases() {
        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(new ObjectIntHashMap<>()), equalTo(0.0));
        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(mapOf(0, 0, 0)), equalTo(0.0));
        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(mapOf(5)), equalTo(0.0));
        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(mapOf(2, 1)), equalTo(0.0));
        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(mapOf(10, 11, 11)), equalTo(0.0));
        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(mapOf(1, 0, 0, 0)), equalTo(0.0));
        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(mapOf(3, 0, 0, 0)), closeTo(2.0 / 3.0, 1e-9));
    }

    public void testShardsImbalanceRatio() {
        final int numNodes = between(2, rarely() ? 100 : 5);
        // light nodes: below average after redistribution; heavy nodes: above average
        final int numLight = between(1, numNodes - 1);
        final int avgShards = between(1, 40);
        final int totalShards = numNodes * avgShards;
        final var ratio = between(0, 19) * 0.05;
        final int offBalance = (int) Math.floor(ratio * totalShards);
        assumeTrue("light nodes must cover offBalance", offBalance <= numLight * avgShards);

        final var map = buildBalancedMap(numNodes, avgShards);
        subtractFromLightNodes(map, numLight, offBalance, avgShards);
        addToHeavyNodes(map, numLight, numNodes, offBalance);

        assertThat(IndexBalanceMetrics.shardsImbalanceRatio(map), closeTo(totalShards > 0 ? (double) offBalance / totalShards : 0.0, 1e-9));
    }

    public void testBucketIndex() {
        assertThat(IndexBalanceMetrics.bucketIndex(0.0), equalTo(0));
        assertThat(IndexBalanceMetrics.bucketIndex(0.05), equalTo(1));
        assertThat(IndexBalanceMetrics.bucketIndex(0.1), equalTo(1));
        assertThat(IndexBalanceMetrics.bucketIndex(0.19), equalTo(1));
        assertThat(IndexBalanceMetrics.bucketIndex(0.2), equalTo(2));
        assertThat(IndexBalanceMetrics.bucketIndex(0.3), equalTo(2));
        assertThat(IndexBalanceMetrics.bucketIndex(0.49), equalTo(2));
        assertThat(IndexBalanceMetrics.bucketIndex(0.5), equalTo(3));
        assertThat(IndexBalanceMetrics.bucketIndex(0.75), equalTo(3));
        assertThat(IndexBalanceMetrics.bucketIndex(1.0), equalTo(3));
    }

    public void testLastStateRetained() {
        final var state = buildState(
            List.of(randomIndexName()),
            randomFrequencyMap("index_", randomNodeCount(), between(0, 40), 1),
            randomNodeCount()
        );
        final var metrics = new IndexBalanceMetrics();

        assertThat(metrics.getLastState(), equalTo(IndexBalanceMetrics.IndexBalanceState.EMPTY));

        final var result = metrics.compute(state);
        assertThat(metrics.getLastState(), equalTo(result));
    }

    public void testResetState() {
        final var state = buildState(
            List.of(randomIndexName()),
            randomFrequencyMap("index_", randomNodeCount(), between(0, 40), 1),
            randomNodeCount()
        );
        final var metrics = new IndexBalanceMetrics();
        metrics.compute(state);
        assertThat(metrics.getLastState(), not(equalTo(IndexBalanceMetrics.IndexBalanceState.EMPTY)));

        metrics.resetState();
        assertThat(metrics.getLastState(), equalTo(IndexBalanceMetrics.IndexBalanceState.EMPTY));
    }

    // -- helpers --

    private static ClusterState buildState(List<String> indexNames, Map<String, Integer> primariesPerNode, int numSearchNodes) {
        final var nodesBuilder = DiscoveryNodes.builder();
        for (var nodeId : primariesPerNode.keySet()) {
            nodesBuilder.add(DiscoveryNodeUtils.builder(nodeId).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build());
        }
        for (int i = 0; i < numSearchNodes; i++) {
            nodesBuilder.add(DiscoveryNodeUtils.builder("search_" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build());
        }

        final int totalPrimaries = primariesPerNode.values().stream().mapToInt(Integer::intValue).sum();
        final var metadataBuilder = Metadata.builder();
        final var routingTableBuilder = RoutingTable.builder();

        for (var indexName : indexNames) {
            final var index = new Index(indexName, "_na_");
            metadataBuilder.put(
                IndexMetadata.builder(indexName).settings(indexSettings(IndexVersion.current(), totalPrimaries, numSearchNodes))
            );

            final var indexRouting = IndexRoutingTable.builder(index);
            int shardId = 0;
            for (var entry : primariesPerNode.entrySet()) {
                for (int i = 0; i < entry.getValue(); i++) {
                    indexRouting.addShard(newShardRouting(new ShardId(index, shardId), entry.getKey(), true, ShardRoutingState.STARTED));
                    for (int s = 0; s < numSearchNodes; s++) {
                        indexRouting.addShard(
                            newShardRouting(new ShardId(index, shardId), "search_" + s, false, ShardRoutingState.STARTED)
                        );
                    }
                    shardId++;
                }
            }
            routingTableBuilder.add(indexRouting);
        }

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder)
            .metadata(metadataBuilder)
            .routingTable(routingTableBuilder)
            .build();
    }

    private static int randomCount() {
        return between(1, rarely() ? 1000 : 10);
    }

    private static int randomNodeCount() {
        return between(1, rarely() ? 100 : 5);
    }

    private static List<String> randomIndexNames(int count) {
        final var names = new ArrayList<String>(count);
        for (int i = 0; i < count; i++) {
            names.add(randomIndexName() + "-" + i);
        }
        return names;
    }

    /**
     * Per-node random shard counts in {@code [0, max]}. If the total falls below
     * {@code minTotal}, the first node is bumped to satisfy it.
     */
    private static Map<String, Integer> randomFrequencyMap(String prefix, int nodeCount, int maxShardsPerNode, int minTotal) {
        final var map = new HashMap<String, Integer>();
        int total = 0;
        for (int i = 0; i < nodeCount; i++) {
            final int count = between(0, maxShardsPerNode);
            map.put(prefix + i, count);
            total += count;
        }
        if (total < minTotal && nodeCount > 0) {
            final var firstKey = prefix + "0";
            map.put(firstKey, map.get(firstKey) + (minTotal - total));
        }
        return map;
    }

    private static int sum(int[] histogram) {
        int total = 0;
        for (int value : histogram) {
            total += value;
        }
        return total;
    }

    private static ObjectIntHashMap<String> mapOf(int... values) {
        final var map = new ObjectIntHashMap<String>();
        for (int i = 0; i < values.length; i++) {
            map.put("node_" + i, values[i]);
        }
        return map;
    }

    private static ObjectIntHashMap<String> buildBalancedMap(int numNodes, int avgShards) {
        final var map = new ObjectIntHashMap<String>();
        for (int i = 0; i < numNodes; i++) {
            map.put("node_" + i, avgShards);
        }
        return map;
    }

    private static void subtractFromLightNodes(ObjectIntHashMap<String> map, int numLight, int offBalance, int avgShards) {
        int remaining = offBalance;
        for (int i = 0; i < numLight; i++) {
            final var key = "node_" + i;
            final int take = (i < numLight - 1) ? between(0, Math.min(remaining, avgShards)) : remaining;
            map.addTo(key, -take);
            remaining -= take;
        }
    }

    private static void addToHeavyNodes(ObjectIntHashMap<String> map, int numLight, int numNodes, int offBalance) {
        int remaining = offBalance;
        for (int i = numLight; i < numNodes; i++) {
            final var key = "node_" + i;
            final int give = (i < numNodes - 1) ? between(0, remaining) : remaining;
            map.addTo(key, give);
            remaining -= give;
        }
    }
}
