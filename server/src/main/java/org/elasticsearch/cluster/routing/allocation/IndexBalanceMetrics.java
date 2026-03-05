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
import com.carrotsearch.hppc.ObjectIntMap;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.Arrays;
import java.util.stream.Stream;

/**
 * Computes index balance metrics from cluster state.
 * Used by {@link IndexBalanceMetricsTask} which periodically triggers computation and
 * publishes the result via MeterRegistry.
 *
 * <p>Balance is computed per sub-group (primaries on {@link DiscoveryNodeRole#INDEX_ROLE} nodes,
 * replicas on {@link DiscoveryNodeRole#SEARCH_ROLE} nodes). Results are aggregated into a
 * histogram of balance ranges rather than retained per-index.
 */
public final class IndexBalanceMetrics {

    static final double[] BUCKET_UPPER_BOUNDS = { 0.0, 0.2, 0.5, 1.0 };
    static final int BUCKET_COUNT = BUCKET_UPPER_BOUNDS.length;

    /**
     * Histogram of index balance values for primary and replica sub-groups.
     * Each histogram has {@link #BUCKET_COUNT} buckets; see {@link #compute(ClusterState)} for the bucket scheme.
     */
    public record IndexBalanceState(int[] primaryBalanceHistogram, int[] replicaBalanceHistogram) {

        public static final IndexBalanceState EMPTY = new IndexBalanceState(new int[BUCKET_COUNT], new int[BUCKET_COUNT]);
    }

    /**
     * Compute index balance from the current cluster state.
     * Builds frequency maps of shard counts per eligible node, computes an imbalance ratio
     * per index via {@link #shardsImbalanceRatio}, and buckets the results into histograms.
     *
     * <p>Bucket scheme:
     * <table>
     * <caption>Imbalance ratio histogram buckets</caption>
     * <tr><th>Index</th><th>Label</th><th>Meaning</th><th>Example</th></tr>
     * <tr><td>0</td><td>{@code 0}</td><td>Perfect balance (ratio == 0.0)</td><td>3 shards on 3 nodes: [1,1,1]</td></tr>
     * <tr><td>1</td><td>{@code (0.0,0.2)}</td><td>Mild imbalance</td><td>10 shards on 3 nodes: [2,4,4]</td></tr>
     * <tr><td>2</td><td>{@code [0.2,0.5)}</td><td>Moderate imbalance</td><td>10 shards on 3 nodes: [1,4,5]</td></tr>
     * <tr><td>3</td><td>{@code [0.5,1.0]}</td><td>Severe imbalance</td><td>6 shards on 3 nodes: [0,0,6]</td></tr>
     * </table>
     */
    public IndexBalanceState compute(ClusterState state) {
        final var nodes = state.nodes();
        final var shutdowns = state.metadata().nodeShutdowns();
        final var indexNodeMap = buildEligibleNodeMap(nodes, shutdowns, DiscoveryNodeRole.INDEX_ROLE);
        final var searchNodeMap = buildEligibleNodeMap(nodes, shutdowns, DiscoveryNodeRole.SEARCH_ROLE);
        final var primaryHist = new int[BUCKET_COUNT];
        final var replicaHist = new int[BUCKET_COUNT];

        for (var indexRoutingTable : state.routingTable()) {
            Arrays.fill(indexNodeMap.values, 0);
            Arrays.fill(searchNodeMap.values, 0);

            fillFrequencyMap(indexRoutingTable.allActivePrimaries(), indexNodeMap);
            fillFrequencyMap(indexRoutingTable.allActiveReplicas(), searchNodeMap);

            primaryHist[bucketIndex(shardsImbalanceRatio(indexNodeMap))]++;
            replicaHist[bucketIndex(shardsImbalanceRatio(searchNodeMap))]++;
        }

        return new IndexBalanceState(primaryHist, replicaHist);
    }

    /**
     * Build a frequency map pre-populated with zero counts for all eligible nodes having the given role
     * and not marked for shutdown.
     */
    static ObjectIntHashMap<String> buildEligibleNodeMap(DiscoveryNodes nodes, NodesShutdownMetadata shutdowns, DiscoveryNodeRole role) {
        final var map = new ObjectIntHashMap<String>();
        for (var node : nodes.getDataNodes().values()) {
            if (node.getRoles().contains(role) && shutdowns.contains(node.getId()) == false) {
                map.put(node.getId(), 0);
            }
        }
        return map;
    }

    private static void fillFrequencyMap(Stream<ShardRouting> shards, ObjectIntHashMap<String> frequencyMap) {
        shards.forEach(shard -> {
            final var nodeId = shard.currentNodeId();
            if (frequencyMap.containsKey(nodeId)) {
                frequencyMap.addTo(nodeId, 1);
            }
        });
    }

    /**
     * Calculate ratio of shards out of balance. The map should include all available nodes for given shards.
     * Nodes without shards should have zero value but be present in the map.
     *
     * @param nodeShardsFreqMap allocated shard counts per node, including all eligible nodes
     * @return ratio of how many shards out of total need to be moved to achieve balanced state
     */
    static double shardsImbalanceRatio(ObjectIntMap<String> nodeShardsFreqMap) {
        if (nodeShardsFreqMap.isEmpty()) {
            return 0.0;
        }

        int totalShards = 0;
        int minShards = Integer.MAX_VALUE;
        int maxShards = 0;
        for (var cursor : nodeShardsFreqMap) {
            totalShards += cursor.value;
            minShards = Math.min(minShards, cursor.value);
            maxShards = Math.max(maxShards, cursor.value);
        }

        if (totalShards == 0 || maxShards - minShards <= 1) {
            return 0.0;
        }

        final int nodesForBalance = Math.min(totalShards, nodeShardsFreqMap.size());
        final double avg = ((double) totalShards) / nodesForBalance;

        double offBalanceShards = 0;
        for (var cursor : nodeShardsFreqMap) {
            if (cursor.value > avg) {
                offBalanceShards += cursor.value - avg;
            }
        }

        offBalanceShards = Math.floor(offBalanceShards);
        return offBalanceShards / totalShards;
    }

    static int bucketIndex(double balance) {
        if (balance == 0.0) {
            return 0;
        }
        for (int i = 1; i < BUCKET_UPPER_BOUNDS.length; i++) {
            if (balance < BUCKET_UPPER_BOUNDS[i]) {
                return i;
            }
        }
        return BUCKET_COUNT - 1;
    }
}
