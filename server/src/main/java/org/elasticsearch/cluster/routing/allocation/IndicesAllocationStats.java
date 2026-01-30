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

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;

public class IndicesAllocationStats {

    static Stream<ShardRouting> indexPrimaries(IndexRoutingTable indexRt) {
        return indexRt.allShards().map(IndexShardRoutingTable::primaryShard);
    }

    static Stream<ShardRouting> indexReplicas(IndexRoutingTable indexRt) {
        return indexRt.allShards().map(IndexShardRoutingTable::replicaShards).flatMap(List::stream);
    }

    /**
     * Creates frequency map of zeros for filtered nodes.
     */
    static ObjectIntHashMap<String> emptyFreqMap(DiscoveryNodes nodes, Predicate<DiscoveryNode> nodeFilter) {
        final var freqMap = new ObjectIntHashMap<String>();
        nodes.stream().filter(nodeFilter).forEach(node -> freqMap.put(node.getId(), 0));
        return freqMap;
    }

    static ObjectIntHashMap<String> indexTierEmptyFreqMap(DiscoveryNodes nodes) {
        return emptyFreqMap(nodes, node -> node.hasRole(INDEX_ROLE.roleName()));
    }

    static ObjectIntHashMap<String> searchTierEmptyFreqMap(DiscoveryNodes nodes) {
        return emptyFreqMap(nodes, node -> node.hasRole(SEARCH_ROLE.roleName()));
    }

    /**
     * Keys(NodeIds) in map stay the same across many, possibly thousands of indices.
     * Rather than constructing new map every time we can wipe values and reuse map.
     */
    static void resetFreqMap(ObjectIntHashMap<String> freqMap) {
        Arrays.fill(freqMap.values, 0);
    }

    /**
     * Given stream of shards and frequency map with populated keys(NodeIds) fill shards counts.
     */
    static void populateFreqMap(Stream<ShardRouting> shards, ObjectIntMap<String> emptyMapWithNodes) {
        shards.forEach(sr -> {
            final var nodeId = sr.currentNodeId();
            if (nodeId != null) {
                assert emptyMapWithNodes.containsKey(nodeId);
                emptyMapWithNodes.addTo(nodeId, 1);
            }
        });
    }

    /**
     * Calculate ratio of shards out of balance. A caller must provide a frequency map
     * of shards per node. The map should include all available nodes for given shards.
     * Node that does not have a shard should have zero value, but exists in map.
     *
     * @param nodeShardsFreqMap a map of allocated shards counts per node, map should include all nodes
     *                          available for shards allocation.
     * @return ratio of how many shards out of total need to be moved to achieve balanced state.
     */
    static float shardsImbalanceRatio(ObjectIntMap<String> nodeShardsFreqMap) {
        int totalShards = 0;
        int minShards = 0;
        int maxShards = 0;
        for (var cursor : nodeShardsFreqMap) {
            totalShards += cursor.value;
            minShards = Math.min(minShards, cursor.value);
            maxShards = Math.max(maxShards, cursor.value);
        }

        // Quick balance test, min and max at most 1 shard apart.
        // For example, [1,1], [2,1], [1,0,0,0], [10,11,11]
        if (maxShards - minShards <= 1) {
            return 0;
        }

        // Number of nodes required to achieve balance.
        // When total number of shards is less than available nodes
        // then we need less nodes to achieve balance.
        // For example, [3,0,0,0] needs only 3 nodes - [1,1,1].
        int nodesForBalance = Math.min(totalShards, nodeShardsFreqMap.size());

        // A total number of shards we need move to achieve perfect balance.
        // Sum all positive differences from average to see how many shards out of balance
        float offBalanceShards = 0;
        float avg = ((float) totalShards) / nodesForBalance;
        for (var cursor : nodeShardsFreqMap) {
            if (cursor.value > avg) {
                offBalanceShards += avg - cursor.value;
            }
        }

        // Only whole shard can be moved, a fractional part here indicates normal inequality that
        // cannot be further improved. For example, [3,3,1] has avg=2.33.. and sum of positive
        // diffs = (3-2.33..) + (3 - 2.33..) ~= 1.34, at least one shard. Moving one shard would
        // achieve balance [3,2,2].
        offBalanceShards = (float) Math.floor(offBalanceShards);

        return offBalanceShards / totalShards;
    }

    /**
     * A wrapper around {@link #shardsImbalanceRatio(ObjectIntMap)} to fill and reset frequency map.
     */
    static float shardsImbalanceRatioReuseMap(Stream<ShardRouting> shards, ObjectIntHashMap<String> freqMap) {
        populateFreqMap(shards, freqMap);
        final var imbalance = shardsImbalanceRatio(freqMap);
        resetFreqMap(freqMap);
        return imbalance;
    }

    /**
     * Calculates imbalance ratios for index and search tiers in Serverless. Assuming primaries
     * should be assigned across nodes with {@link org.elasticsearch.cluster.node.DiscoveryNodeRole#INDEX_ROLE}
     * and replicas across nodes with {@link org.elasticsearch.cluster.node.DiscoveryNodeRole#SEARCH_ROLE}.
     */
    static ServerlessIndexImbalance calculateServerlessIndexImbalance(
        IndexRoutingTable indexRt,
        ObjectIntHashMap<String> indexFreqMap,
        ObjectIntHashMap<String> searchFreqMap
    ) {
        final var primaries = indexPrimaries(indexRt);
        final var indexImbalance = shardsImbalanceRatioReuseMap(primaries, indexFreqMap);
        final var replicas = indexReplicas(indexRt);
        final var searchImbalance = shardsImbalanceRatioReuseMap(replicas, searchFreqMap);
        return new ServerlessIndexImbalance(indexImbalance, searchImbalance);
    }

    /**
     * Calculate a histogram of indices and their imbalance ratio for entire cluster. A histogram
     * starts at 0 and ends at 1, with 0.1 step. For example, [0,0,1,2,1,0,0,0,0,0] means one index
     * with 0.2-0.3, two indices with 0.3-0.4,and one index with 0.4-0.5 imbalance ratio.
     */
    public static ServerlessIndicesImbalanceHistogram calculateServerlessIndicesImbalanceHistogram(RoutingTable rt, DiscoveryNodes nodes) {
        final var indexFreqMap = indexTierEmptyFreqMap(nodes);
        final var searchFreqMap = searchTierEmptyFreqMap(nodes);
        final var indexHistogram = new int[10];
        final var searchHistogram = new int[10];
        for (var irt : rt) {
            final var imbalance = calculateServerlessIndexImbalance(irt, indexFreqMap, searchFreqMap);
            indexHistogram[Math.round(imbalance.index * 10)] += 1;
            searchHistogram[Math.round(imbalance.search * 10)] += 1;
        }
        return new ServerlessIndicesImbalanceHistogram(indexHistogram, searchHistogram);
    }

    public record ServerlessIndicesImbalanceHistogram(int[] index, int[] search) {}

    record ServerlessIndexImbalance(float index, float search) {}

}
