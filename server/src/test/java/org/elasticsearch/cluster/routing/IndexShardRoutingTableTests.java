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

public class IndexShardRoutingTableTests extends ESTestCase {

    /**
     * Verify that when a new node joins with no ARS stats, it is not ranked first (i.e. it does not
     * receive a disproportionate flood of requests). Nodes without stats should be assigned the max
     * known rank so they sort alongside the worst-ranked known node rather than before all others.
     */
    public void testAdaptiveReplicaSelectionDoesNotFloodNewNode() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            Index index = new Index("test", "test-uuid");
            ShardId shardId = new ShardId(index, 0);

            // Three active shards on three different nodes
            ShardRouting shard1 = TestShardRouting.newShardRouting(shardId, "node1", true, ShardRoutingState.STARTED);
            ShardRouting shard2 = TestShardRouting.newShardRouting(shardId, "node2", false, ShardRoutingState.STARTED);
            ShardRouting shard3 = TestShardRouting.newShardRouting(shardId, "node3", false, ShardRoutingState.STARTED);

            IndexShardRoutingTable table = new IndexShardRoutingTable(shardId, List.of(shard1, shard2, shard3));

            // Set up a ResponseCollectorService with stats only for node1 and node2 (node3 is "new")
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            ResponseCollectorService collector = new ResponseCollectorService(clusterService);
            // node1: low queue, fast response/service → good rank
            collector.addNodeStatistics("node1", 1, 100_000, 50_000);
            // node2: higher queue, slower → worse rank
            collector.addNodeStatistics("node2", 5, 500_000, 200_000);
            // node3: no stats at all — simulates a newly joined node

            Map<String, Long> searchCounts = new HashMap<>();
            searchCounts.put("node1", 5L);
            searchCounts.put("node2", 3L);

            // Perform ranked iteration and count how often each node is first
            Map<String, Integer> firstCounts = new HashMap<>();
            int iterations = 10;
            for (int i = 0; i < iterations; i++) {
                ShardIterator it = table.activeInitializingShardsRankedIt(collector, searchCounts);
                ShardRouting first = it.nextOrNull();
                assertNotNull(first);
                firstCounts.merge(first.currentNodeId(), 1, Integer::sum);
            }

            // node3 (no stats) must NOT be ranked first — its default rank equals the max
            // known rank, so nodes with better stats should always be preferred
            assertEquals("node with no stats should not be ranked first", 0, firstCounts.getOrDefault("node3", 0).intValue());
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
