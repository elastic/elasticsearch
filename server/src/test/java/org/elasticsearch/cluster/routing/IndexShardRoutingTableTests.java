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
import java.util.Random;

public class IndexShardRoutingTableTests extends ESTestCase {

    /**
     * Replicas without statistics either have no synthetic rank (null — sorts last) or, with
     * probability {@code unknown/total}, one such replica is assigned {@code nextDown(bestRank)} for
     * exploration.
     */
    public void testRankNodesWithoutStatsExploresOrLeavesNull() {
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
            double bestRank = Math.min(r1, r2);
            double expectedExplore = Math.nextDown(bestRank);

            for (int seed = 0; seed < 100; seed++) {
                Map<String, Double> ranks = IndexShardRoutingTable.rankNodes(nodeStats, searchCounts, new Random(seed));
                assertEquals(r1, ranks.get("node1"), 0.0);
                assertEquals(r2, ranks.get("node2"), 0.0);
                Double r3 = ranks.get("node3");
                if (r3 != null) {
                    assertEquals(expectedExplore, r3, 0.0);
                }
            }
        } finally {
            terminate(threadPool);
        }
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
