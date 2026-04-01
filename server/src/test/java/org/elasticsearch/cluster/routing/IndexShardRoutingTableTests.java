/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.ResponseCollectorService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IndexShardRoutingTableTests extends ESTestCase {

    /**
     * Stat-less nodes receive no rank entry from {@code rankNodes} and sort last via nullsLast.
     * They receive traffic through ε-greedy exploration, not through rank manipulation.
     */
    public void testRankNodesExcludesStatlessNodes() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            ResponseCollectorService collector = new ResponseCollectorService(clusterService);
            collector.addNodeStatistics("node1", 1, 100_000, 50_000);
            collector.addNodeStatistics("node2", 5, 500_000, 200_000);

            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("node1", collector.getNodeStatistics("node1"));
            nodeStats.put("node2", collector.getNodeStatistics("node2"));
            nodeStats.put("node3", Optional.empty());

            Map<String, Long> searchCounts = new HashMap<>();
            searchCounts.put("node1", 5L);
            searchCounts.put("node2", 3L);

            double r1 = nodeStats.get("node1").get().rank(searchCounts.get("node1"));
            double r2 = nodeStats.get("node2").get().rank(searchCounts.get("node2"));

            Map<String, Double> ranks = IndexShardRoutingTable.rankNodes(nodeStats, searchCounts);
            assertEquals(r1, ranks.get("node1"), 0.0);
            assertEquals(r2, ranks.get("node2"), 0.0);
            // stat-less node gets no rank entry
            assertNull(ranks.get("node3"));
        } finally {
            terminate(threadPool);
        }
    }

    public void testFindExplorationCandidateStatlessNode() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ResponseCollectorService collector = newCollector(threadPool);
            // node1 is fully warmed (past the 30-response threshold)
            for (int i = 0; i < 50; i++) {
                collector.addNodeStatistics("node1", 10, 100_000, 50_000);
            }
            // node2 has no stats — stat-less

            List<ShardRouting> shards = List.of(startedShard(true, "node1"), startedShard(false, "node2"));
            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("node1", collector.getNodeStatistics("node1"));
            nodeStats.put("node2", Optional.empty());

            // stat-less node is a candidate
            ShardRouting candidate = IndexShardRoutingTable.findExplorationCandidate(shards, nodeStats, collector, null, 0, 30);
            assertNotNull(candidate);
            assertEquals("node2", candidate.currentNodeId());

            // backstop: at the in-flight cap → not a candidate
            candidate = IndexShardRoutingTable.findExplorationCandidate(shards, nodeStats, collector, Map.of("node2", 8L), 8, 30);
            assertNull(candidate);

            // below the in-flight cap → still a candidate
            candidate = IndexShardRoutingTable.findExplorationCandidate(shards, nodeStats, collector, Map.of("node2", 7L), 8, 30);
            assertNotNull(candidate);
            assertEquals("node2", candidate.currentNodeId());
        } finally {
            terminate(threadPool);
        }
    }

    public void testFindExplorationCandidateWarmingNode() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ResponseCollectorService collector = newCollector(threadPool);
            // node1: many responses (warmed up)
            for (int i = 0; i < 50; i++) {
                collector.addNodeStatistics("node1", 10, 100_000, 50_000);
            }
            // node2: 5 responses (still warming)
            for (int i = 0; i < 5; i++) {
                collector.addNodeStatistics("node2", 2, 200_000, 100_000);
            }

            int warmupThreshold = 30;
            List<ShardRouting> shards = List.of(startedShard(true, "node1"), startedShard(false, "node2"));
            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("node1", collector.getNodeStatistics("node1"));
            nodeStats.put("node2", collector.getNodeStatistics("node2"));

            ShardRouting candidate = IndexShardRoutingTable.findExplorationCandidate(
                shards,
                nodeStats,
                collector,
                null,
                0,
                warmupThreshold
            );
            assertNotNull("node with responses < warmup threshold should be a candidate", candidate);
            assertEquals("node2", candidate.currentNodeId());

            // push node2 past the warmup threshold
            for (int i = 0; i < 25; i++) {
                collector.addNodeStatistics("node2", 50, 80_000, 30_000);
            }
            nodeStats.put("node2", collector.getNodeStatistics("node2"));

            candidate = IndexShardRoutingTable.findExplorationCandidate(shards, nodeStats, collector, null, 0, warmupThreshold);
            assertNull("node with responses >= warmup threshold should not be a candidate", candidate);
        } finally {
            terminate(threadPool);
        }
    }

    public void testFindExplorationCandidateAllWarmedUp() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ResponseCollectorService collector = newCollector(threadPool);
            for (int i = 0; i < 50; i++) {
                collector.addNodeStatistics("node1", 50, 100_000, 50_000);
                collector.addNodeStatistics("node2", 50, 80_000, 30_000);
            }

            List<ShardRouting> shards = List.of(startedShard(true, "node1"), startedShard(false, "node2"));
            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("node1", collector.getNodeStatistics("node1"));
            nodeStats.put("node2", collector.getNodeStatistics("node2"));

            ShardRouting candidate = IndexShardRoutingTable.findExplorationCandidate(shards, nodeStats, collector, null, 0, 30);
            assertNull("no candidates when all nodes are warmed up", candidate);
        } finally {
            terminate(threadPool);
        }
    }

    private static ResponseCollectorService newCollector(TestThreadPool threadPool) {
        return new ResponseCollectorService(
            new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            )
        );
    }

    private static ShardRouting startedShard(boolean primary, String nodeId) {
        ShardId shardId = new ShardId(new Index("test", "uuid"), 0);
        RecoverySource recovery = primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE;
        ShardRouting shard = ShardRouting.newUnassigned(
            shardId,
            primary,
            recovery,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, null),
            ShardRouting.Role.DEFAULT
        );
        return shard.initialize(nodeId, null, 0).moveToStarted(ShardRouting.UNAVAILABLE_EXPECTED_SHARD_SIZE);
    }

    public void testEqualsAttributesKey() {
        List<String> attr1 = Arrays.asList("a");
        List<String> attr2 = Arrays.asList("b");
        IndexShardRoutingTable.AttributesKey attributesKey1 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey2 = new IndexShardRoutingTable.AttributesKey(attr1);
        IndexShardRoutingTable.AttributesKey attributesKey3 = new IndexShardRoutingTable.AttributesKey(attr2);
        String s = "Some random other object";
        assertEquals(attributesKey1, attributesKey1);
        assertEquals(attributesKey1, attributesKey2);
        assertNotEquals(attributesKey1, null);
        assertNotEquals(attributesKey1, s);
        assertNotEquals(attributesKey1, attributesKey3);
    }

    public void testEquals() {
        Index index = new Index("a", "b");
        ShardId shardId = new ShardId(index, 1);
        ShardId shardId2 = new ShardId(index, 2);
        ShardRouting shardRouting = TestShardRouting.newShardRouting(shardId, null, true, ShardRoutingState.UNASSIGNED);
        IndexShardRoutingTable table1 = new IndexShardRoutingTable(shardId, List.of(shardRouting));
        IndexShardRoutingTable table2 = new IndexShardRoutingTable(shardId, List.of(shardRouting));
        IndexShardRoutingTable table3 = new IndexShardRoutingTable(
            shardId2,
            List.of(TestShardRouting.newShardRouting(shardId2, null, true, ShardRoutingState.UNASSIGNED))
        );
        String s = "Some other random object";
        assertEquals(table1, table1);
        assertEquals(table1, table2);
        assertNotEquals(table1, null);
        assertNotEquals(table1, s);
        assertNotEquals(table1, table3);
    }
}
