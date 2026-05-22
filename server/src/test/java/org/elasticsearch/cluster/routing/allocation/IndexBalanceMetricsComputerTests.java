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
import com.carrotsearch.hppc.procedures.ObjectProcedure;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.node.DiscoveryNodeRole.INDEX_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.ML_ROLE;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.SEARCH_ROLE;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link IndexBalanceMetricsComputer}.
 */
public class IndexBalanceMetricsComputerTests extends ESTestCase {

    /**
     * Two assigned shards perfectly balanced across two index/search nodes (one STARTED, one RELOCATING),
     * plus one unassigned shard. If unassigned shards were counted, or if RELOCATING shards counted on
     * the destination instead of the source, the imbalance ratio would be non-zero; instead both
     * histograms must report perfect balance (bucket 0).
     */
    public void testUnassignedShardsSkipped() {
        final var index = new Index("test-index", "_na_");
        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.builder("index_0").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("index_1").roles(Set.of(INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search_0").roles(Set.of(SEARCH_ROLE)).build())
            .add(DiscoveryNodeUtils.builder("search_1").roles(Set.of(SEARCH_ROLE)).build());

        final var routing = IndexRoutingTable.builder(index)
            .addShard(newShardRouting(new ShardId(index, 0), "index_0", true, ShardRoutingState.STARTED))
            .addShard(newShardRouting(new ShardId(index, 0), "search_0", false, ShardRoutingState.STARTED))
            // Shard 1 is mid-relocation: counted on the source node (index_1 / search_1), not the target.
            .addShard(newShardRouting(new ShardId(index, 1), "index_1", "index_0", true, ShardRoutingState.RELOCATING))
            .addShard(newShardRouting(new ShardId(index, 1), "search_1", "search_0", false, ShardRoutingState.RELOCATING))
            .addShard(newShardRouting(new ShardId(index, 2), null, true, ShardRoutingState.UNASSIGNED))
            .addShard(newShardRouting(new ShardId(index, 2), null, false, ShardRoutingState.UNASSIGNED));

        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(Metadata.builder().put(IndexMetadata.builder(index.getName()).settings(indexSettings(IndexVersion.current(), 3, 1))))
            .routingTable(RoutingTable.builder().add(routing))
            .build();

        final var result = IndexBalanceMetricsComputer.compute(state);

        assertThat("primary shards should yield perfect balance", result.primaryBalanceHistogram()[0], equalTo(1));
        assertThat("replica shards should yield perfect balance", result.replicaBalanceHistogram()[0], equalTo(1));
    }

    static Set<String> randomNodeIds(DiscoveryNodeRole nodeRole, int count) {
        return IntStream.range(0, count).mapToObj(i -> nodeRole.roleName() + "_" + i).collect(Collectors.toSet());
    }

    static DiscoveryNode discoveryNodeFromRole(DiscoveryNodeRole role, String nodeId) {
        return DiscoveryNodeUtils.builder(nodeId).roles(Set.of(role)).build();
    }

    static Set<String> hppcMapKeySet(ObjectIntHashMap<String> hppcMap) {
        final var keySet = new HashSet<String>();
        hppcMap.keys().forEach((ObjectProcedure<? super String>) keySet::add);
        return keySet;
    }

    public void testNonEligibleNodesSkipped() {
        final var indexNodeIds = randomNodeIds(INDEX_ROLE, randomNodeCount());
        final var searchNodeIds = randomNodeIds(SEARCH_ROLE, randomNodeCount());
        final var mlNodeIds = randomNodeIds(ML_ROLE, randomNodeCount());

        final var nodesBuilder = DiscoveryNodes.builder();
        indexNodeIds.stream().map(id -> discoveryNodeFromRole(INDEX_ROLE, id)).forEach(nodesBuilder::add);
        searchNodeIds.stream().map(id -> discoveryNodeFromRole(SEARCH_ROLE, id)).forEach(nodesBuilder::add);
        mlNodeIds.stream().map(id -> discoveryNodeFromRole(ML_ROLE, id)).forEach(nodesBuilder::add);

        final var state = ClusterState.builder(ClusterName.DEFAULT).nodes(nodesBuilder).build();
        final var shutdowns = state.metadata().nodeShutdowns();

        final var indexEligible = IndexBalanceMetricsComputer.buildEligibleNodeMap(state.nodes(), shutdowns, INDEX_ROLE);
        final var searchEligible = IndexBalanceMetricsComputer.buildEligibleNodeMap(state.nodes(), shutdowns, SEARCH_ROLE);
        final var mlEligible = IndexBalanceMetricsComputer.buildEligibleNodeMap(state.nodes(), shutdowns, DiscoveryNodeRole.ML_ROLE);

        assertEquals(indexNodeIds, hppcMapKeySet(indexEligible));
        assertEquals(searchNodeIds, hppcMapKeySet(searchEligible));
        assertEquals(Set.of(), hppcMapKeySet(mlEligible));
    }

    public void testShuttingDownNodesExcluded() {
        final int numHealthyNodes = randomNodeCount();
        final int numShuttingDown = randomNodeCount();

        final var nodesBuilder = DiscoveryNodes.builder();
        final var shutdownEntries = new HashMap<String, SingleNodeShutdownMetadata>();
        for (int i = 0; i < numHealthyNodes; i++) {
            nodesBuilder.add(DiscoveryNodeUtils.builder("index_" + i).roles(Set.of(INDEX_ROLE)).build());
        }
        for (int i = 0; i < numShuttingDown; i++) {
            final var nodeId = "shutdown_" + i;
            nodesBuilder.add(DiscoveryNodeUtils.builder(nodeId).roles(Set.of(INDEX_ROLE)).build());
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
        final var eligibleMap = IndexBalanceMetricsComputer.buildEligibleNodeMap(
            state.nodes(),
            state.metadata().nodeShutdowns(),
            INDEX_ROLE
        );

        assertThat(eligibleMap.size(), equalTo(numHealthyNodes));
        for (int i = 0; i < numShuttingDown; i++) {
            assertFalse(eligibleMap.containsKey("shutdown_" + i));
        }
    }

    public void testShardsImbalanceRatioEdgeCases() {
        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(new ObjectIntHashMap<>()), equalTo(0.0));
        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(mapOf(0, 0, 0)), equalTo(0.0));
        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(mapOf(5)), equalTo(0.0));
        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(mapOf(2, 1)), equalTo(0.0));
        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(mapOf(10, 11, 11)), equalTo(0.0));
        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(mapOf(1, 0, 0, 0)), equalTo(0.0));
        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(mapOf(3, 0, 0, 0)), closeTo(2.0 / 3.0, 1e-9));
    }

    /**
     * Fully randomized test that reverse-engineers a shard allocation from a target imbalance ratio.
     *
     * <ol>
     *   <li>Pick a random number of nodes and a random average shard count, giving a perfectly balanced starting map.</li>
     *   <li>Choose a target imbalance ratio (0.0 to 0.95 in 0.05 steps) and compute how many shards
     *       ({@code offBalance}) must be moved to produce that ratio.</li>
     *   <li>Subtract {@code offBalance} shards from randomly chosen "light" nodes (below average)
     *       and add them to "heavy" nodes (above average).</li>
     *   <li>Assert that {@link IndexBalanceMetricsComputer#shardsImbalanceRatio} returns the expected ratio.</li>
     * </ol>
     */
    public void testShardsImbalanceRatio() {
        final int numNodes = between(2, rarely() ? 100 : 5);
        final int numLight = between(1, numNodes - 1);
        final int avgShards = between(1, 40);
        final int totalShards = numNodes * avgShards;
        // Cap the ratio so that the light nodes can absorb all `offBalance` deductions without going negative.
        final double maxRatio = Math.min(0.95, (double) numLight / numNodes);
        final var ratio = between(0, (int) Math.floor(maxRatio / 0.05)) * 0.05;
        final int offBalance = (int) Math.floor(ratio * totalShards);

        final var map = buildBalancedMap(numNodes, avgShards);
        subtractFromLightNodes(map, numLight, offBalance, avgShards);
        addToHeavyNodes(map, numLight, numNodes, offBalance);

        assertThat(IndexBalanceMetricsComputer.shardsImbalanceRatio(map), closeTo((double) offBalance / totalShards, 1e-9));
    }

    public void testBucketIndex() {
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.0), equalTo(0));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.05), equalTo(1));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.1), equalTo(1));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.19), equalTo(1));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.2), equalTo(2));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.3), equalTo(2));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.49), equalTo(2));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.5), equalTo(3));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(0.75), equalTo(3));
        assertThat(IndexBalanceMetricsComputer.bucketIndex(1.0), equalTo(3));
    }

    // -- helpers --

    private static int randomNodeCount() {
        return between(1, rarely() ? 100 : 5);
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

    /**
     * Removes {@code offBalance} shards from "light" nodes (indices {@code [0, numLight)}).
     * Each non-last light node gives up a random portion of the remaining deficit (capped at {@code avgShards}
     * so counts stay non-negative); the last light node absorbs whatever is left.
     */
    private static void subtractFromLightNodes(ObjectIntHashMap<String> map, int numLight, int offBalance, int avgShards) {
        int remaining = offBalance;
        for (int i = 0; i < numLight; i++) {
            final var key = "node_" + i;
            final int take = (i < numLight - 1) ? between(0, Math.min(remaining, avgShards)) : remaining;
            map.addTo(key, -take);
            remaining -= take;
        }
    }

    /**
     * Adds {@code offBalance} shards to "heavy" nodes (indices {@code [numLight, numNodes)}).
     * Each non-last heavy node receives a random portion of the remaining surplus;
     * the last heavy node absorbs whatever is left.
     */
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
