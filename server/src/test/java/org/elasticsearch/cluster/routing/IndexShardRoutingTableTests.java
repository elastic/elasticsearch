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
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class IndexShardRoutingTableTests extends ESTestCase {

    /**
     * The probe cap checks the live in-flight map, not the snapshot. Stat-less nodes below the
     * live cap are probed (assigned {@code nextDown(bestRank)}). Nodes at or above the live cap
     * get no rank entry and sort last via nullsLast.
     */
    public void testRankNodesProbeCapUsesLiveInflightCounts() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            ResponseCollectorService collector = new ResponseCollectorService(clusterService, MeterRegistry.NOOP);
            collector.addNodeStatistics("node1", 1, 100_000, 50_000);
            collector.addNodeStatistics("node2", 5, 500_000, 200_000);

            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("node1", collector.getNodeStatistics("node1"));
            nodeStats.put("node2", collector.getNodeStatistics("node2"));
            nodeStats.put("node3", Optional.empty());

            // Snapshot used by ARS formula — node3 has 0 inflight here
            Map<String, Long> snapshot = new HashMap<>();
            snapshot.put("node1", 5L);
            snapshot.put("node2", 3L);

            double r1 = nodeStats.get("node1").get().rank(snapshot.get("node1"));
            double r2 = nodeStats.get("node2").get().rank(snapshot.get("node2"));
            double bestRank = Math.min(r1, r2);
            double expectedProbe = Math.nextDown(bestRank);

            long cap = 3;

            // Live map shows node3 at 0 inflight → probed
            Map<String, Long> live = new HashMap<>();
            Map<String, Double> ranks = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, cap, 0);
            assertEquals(r1, ranks.get("node1"), 0.0);
            assertEquals(r2, ranks.get("node2"), 0.0);
            assertEquals(expectedProbe, ranks.get("node3"), 0.0);

            // Live map shows node3 below cap → still probed
            live.put("node3", cap - 1);
            ranks = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, cap, 0);
            assertEquals(expectedProbe, ranks.get("node3"), 0.0);

            // Live map shows node3 at cap → NOT probed, even though snapshot still shows 0
            live.put("node3", cap);
            ranks = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, cap, 0);
            assertNull(ranks.get("node3"));
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * A warming-up peer (observationCount below the warmup threshold) whose sparse stats make it
     * look fastest gets clamped up to the lowest warm peer's rank, ties with the best warm peer,
     * and shares traffic via the comparator's tie-break. Once its observation count crosses the
     * threshold the clamp no longer applies and it ranks on standard C3 terms.
     */
    public void testRankNodesClampsWarmingUpPeerToLowestWarmPeer() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            ResponseCollectorService collector = new ResponseCollectorService(clusterService, MeterRegistry.NOOP);
            // warmA is the fastest, well-established peer. warmB is slightly busier. Both run well
            // past the warmup threshold so they're considered warm.
            collector.addNodeStatistics("warmA", 2, 100_000, 100_000);
            collector.addNodeStatistics("warmB", 4, 100_000, 100_000);
            for (int i = 0; i < 50; i++) {
                collector.addNodeStatistics("warmA", 2, 100_000, 100_000);
                collector.addNodeStatistics("warmB", 4, 100_000, 100_000);
            }
            // freshX has the same raw stats as warmA but only one observation — without the clamp
            // it'd share warmA's rank from sparse data and start flooding.
            collector.addNodeStatistics("freshX", 2, 100_000, 100_000);

            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("warmA", collector.getNodeStatistics("warmA"));
            nodeStats.put("warmB", collector.getNodeStatistics("warmB"));
            nodeStats.put("freshX", collector.getNodeStatistics("freshX"));

            Map<String, Long> snapshot = Map.of("warmA", 0L, "warmB", 0L, "freshX", 0L);
            Map<String, Long> live = Map.of();

            // Clamp disabled (warmupSamples=0): freshX and warmA share rank, warmB is worse.
            Map<String, Double> bareRanks = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, 0L, 0);
            double bareWarmA = bareRanks.get("warmA");
            double bareWarmB = bareRanks.get("warmB");
            double bareFreshX = bareRanks.get("freshX");
            assertEquals("freshX should share warmA's bare rank when their stats match", bareWarmA, bareFreshX, 0.0);
            assertTrue("warmA should beat warmB on bare ranks", bareWarmA < bareWarmB);

            // Clamp enabled with threshold higher than freshX's observation count: freshX is
            // warming up. Its bare rank equals warmA's, so the clamp is a no-op for it (it isn't
            // below bestWarmRank). Warm peers are untouched.
            Map<String, Double> clampedTie = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, 0L, 10);
            assertEquals(bareWarmA, clampedTie.get("warmA"), 0.0);
            assertEquals(bareWarmB, clampedTie.get("warmB"), 0.0);
            assertEquals(bareWarmA, clampedTie.get("freshX"), 0.0);

            // Now drop warmA's queue so its bare rank drops below the would-be freshX rank, then
            // re-add freshX with a value that makes it look fastest of all. The clamp should peg
            // freshX up to warmA's bare rank instead of letting it surge ahead.
            ResponseCollectorService floodCollector = new ResponseCollectorService(clusterService, MeterRegistry.NOOP);
            floodCollector.addNodeStatistics("warmA", 3, 100_000, 100_000);
            for (int i = 0; i < 50; i++) {
                floodCollector.addNodeStatistics("warmA", 3, 100_000, 100_000);
            }
            // freshX has only one observation but happens to look extremely fast.
            floodCollector.addNodeStatistics("freshX", 1, 50_000, 50_000);

            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> floodStats = new HashMap<>();
            floodStats.put("warmA", floodCollector.getNodeStatistics("warmA"));
            floodStats.put("freshX", floodCollector.getNodeStatistics("freshX"));
            Map<String, Long> floodSnap = Map.of("warmA", 0L, "freshX", 0L);
            Map<String, Long> floodLive = Map.of();

            Map<String, Double> floodBare = IndexShardRoutingTable.rankNodes(floodStats, floodSnap, floodLive, 0L, 0);
            assertTrue("freshX should look fastest on bare ranks", floodBare.get("freshX") < floodBare.get("warmA"));

            Map<String, Double> floodClamped = IndexShardRoutingTable.rankNodes(floodStats, floodSnap, floodLive, 0L, 10);
            // Post-clamp: freshX is pegged to warmA's bare rank.
            assertEquals(floodBare.get("warmA"), floodClamped.get("freshX"), 0.0);
            assertEquals(floodBare.get("warmA"), floodClamped.get("warmA"), 0.0);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * A warming-up peer whose bare rank is already worse than the best warm peer is left alone —
     * the clamp only raises ranks that would otherwise sort below all warm peers.
     */
    public void testRankNodesDoesNotBoostWarmingUpPeerWithWorseBareRank() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            ResponseCollectorService collector = new ResponseCollectorService(clusterService, MeterRegistry.NOOP);
            collector.addNodeStatistics("warmA", 1, 100_000, 50_000);
            for (int i = 0; i < 50; i++) {
                collector.addNodeStatistics("warmA", 1, 100_000, 50_000);
            }
            // freshY has high queue + service time → bare rank far above warmA's.
            collector.addNodeStatistics("freshY", 8, 800_000, 400_000);

            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("warmA", collector.getNodeStatistics("warmA"));
            nodeStats.put("freshY", collector.getNodeStatistics("freshY"));
            Map<String, Long> snapshot = Map.of("warmA", 0L, "freshY", 0L);
            Map<String, Long> live = Map.of();

            Map<String, Double> bare = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, 0L, 0);
            assertTrue(bare.get("freshY") > bare.get("warmA"));

            Map<String, Double> clamped = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, 0L, 10);
            // freshY's bare rank is already above warmA's — the clamp leaves it alone.
            assertEquals(bare.get("freshY"), clamped.get("freshY"), 0.0);
            assertEquals(bare.get("warmA"), clamped.get("warmA"), 0.0);
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * Once a peer's observation count reaches the warmup threshold it's considered warm and the
     * clamp no longer applies — even if its bare rank dips below the previous best warm peer's,
     * it's allowed to win on its own merits.
     */
    public void testRankNodesClampReleasesWhenObservationCountReachesThreshold() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            ResponseCollectorService collector = new ResponseCollectorService(clusterService, MeterRegistry.NOOP);
            collector.addNodeStatistics("warmA", 3, 100_000, 100_000);
            for (int i = 0; i < 50; i++) {
                collector.addNodeStatistics("warmA", 3, 100_000, 100_000);
            }
            // X has been observed enough times to cross the threshold and it really is faster.
            collector.addNodeStatistics("X", 1, 50_000, 50_000);
            for (int i = 0; i < 9; i++) {
                collector.addNodeStatistics("X", 1, 50_000, 50_000);
            }

            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("warmA", collector.getNodeStatistics("warmA"));
            nodeStats.put("X", collector.getNodeStatistics("X"));
            Map<String, Long> snapshot = Map.of("warmA", 0L, "X", 0L);
            Map<String, Long> live = Map.of();

            // X.observationCount == 10 (reaches threshold) → considered warm → no clamp → wins on merit.
            Map<String, Double> ranks = IndexShardRoutingTable.rankNodes(nodeStats, snapshot, live, 0L, 10);
            assertTrue("X should beat warmA after crossing the warmup threshold", ranks.get("X") < ranks.get("warmA"));
        } finally {
            terminate(threadPool);
        }
    }

    /**
     * The inflight cap that gates stat-less probing also applies to warming-up peers. A
     * warming-up peer at-or-above the cap is dropped from the rank map entirely so it sorts last
     * via {@code nullsLast} — this hard-caps the burst load it can absorb before it graduates.
     */
    public void testRankNodesAppliesInflightCapToWarmingUpPeers() {
        TestThreadPool threadPool = new TestThreadPool("test");
        try {
            ClusterService clusterService = new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            );
            ResponseCollectorService collector = new ResponseCollectorService(clusterService, MeterRegistry.NOOP);
            // warmA is well-warmed; freshX has only one observation.
            collector.addNodeStatistics("warmA", 2, 100_000, 100_000);
            for (int i = 0; i < 50; i++) {
                collector.addNodeStatistics("warmA", 2, 100_000, 100_000);
            }
            collector.addNodeStatistics("freshX", 2, 100_000, 100_000);

            Map<String, Optional<ResponseCollectorService.ComputedNodeStats>> nodeStats = new HashMap<>();
            nodeStats.put("warmA", collector.getNodeStatistics("warmA"));
            nodeStats.put("freshX", collector.getNodeStatistics("freshX"));

            long cap = 4;
            // Below cap on both snapshot and live maps: freshX participates in ranking.
            Map<String, Long> snapshotBelow = new HashMap<>(Map.of("warmA", 0L, "freshX", cap - 1));
            Map<String, Long> liveBelow = Map.of("freshX", cap - 1);
            Map<String, Double> ranksBelow = IndexShardRoutingTable.rankNodes(nodeStats, snapshotBelow, liveBelow, cap, 10);
            assertNotNull("freshX should still have a rank below cap", ranksBelow.get("freshX"));
            assertNotNull(ranksBelow.get("warmA"));

            // Snapshot at cap → freshX gets no rank entry, sorts last.
            Map<String, Long> snapshotAt = new HashMap<>(Map.of("warmA", 0L, "freshX", cap));
            Map<String, Long> liveAt = Map.of();
            Map<String, Double> ranksSnapAt = IndexShardRoutingTable.rankNodes(nodeStats, snapshotAt, liveAt, cap, 10);
            assertNull("freshX should be removed from ranks when snapshot inflight reaches cap", ranksSnapAt.get("freshX"));
            assertNotNull("warmA still ranked", ranksSnapAt.get("warmA"));

            // Live at cap → also dropped.
            Map<String, Long> snapshotZero = new HashMap<>(Map.of("warmA", 0L, "freshX", 0L));
            Map<String, Long> liveAtCap = Map.of("freshX", cap);
            Map<String, Double> ranksLiveAt = IndexShardRoutingTable.rankNodes(nodeStats, snapshotZero, liveAtCap, cap, 10);
            assertNull("freshX should be removed from ranks when live inflight reaches cap", ranksLiveAt.get("freshX"));

            // The warm peer is never gated by the cap, even at the same inflight count.
            Map<String, Long> snapshotWarmAtCap = new HashMap<>(Map.of("warmA", cap, "freshX", 0L));
            Map<String, Double> ranksWarmAtCap = IndexShardRoutingTable.rankNodes(nodeStats, snapshotWarmAtCap, Map.of(), cap, 10);
            assertNotNull("warmA must keep its rank regardless of inflight cap", ranksWarmAtCap.get("warmA"));
            assertNotNull(ranksWarmAtCap.get("freshX"));
        } finally {
            terminate(threadPool);
        }
    }

    public void testEqualsAttributesKey() {
        List<String> attr1 = List.of("a");
        List<String> attr2 = List.of("b");
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
