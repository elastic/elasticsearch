/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.allocator.BalancingRoundSummary.CombinedBalancingRoundSummary;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class BalancingRoundSummaryTests extends ESTestCase {

    /**
     * Tests the {@link BalancingRoundSummary.CombinedBalancingRoundSummary#combine(List)} method.
     */
    public void testCombine() {
        final String NODE_1 = "node1";
        final String NODE_2 = "node2";
        final var node1BaseWeights = new DesiredBalanceMetrics.NodeWeightStats(10, 20, 30, 40);
        final var node2BaseWeights = new DesiredBalanceMetrics.NodeWeightStats(100, 200, 300, 400);
        final var commonDiff = new BalancingRoundSummary.NodeWeightsDiff(1, 2, 3, 4);
        final var shardMoveCounts1 = new DesiredBalance.ShardMoveCounts(50, 4, 6, 10);
        final var shardMoveCounts2 = new DesiredBalance.ShardMoveCounts(150, 6, 6, 0);

        // Set up a summaries list with two summary entries for a two node cluster
        List<BalancingRoundSummary> summaries = new ArrayList<>();
        summaries.add(
            new BalancingRoundSummary(
                Map.of(
                    NODE_1,
                    new BalancingRoundSummary.NodesWeightsChanges(node1BaseWeights, commonDiff),
                    NODE_2,
                    new BalancingRoundSummary.NodesWeightsChanges(node2BaseWeights, commonDiff)
                ),
                shardMoveCounts1.movements(),
                shardMoveCounts1.newlyAssigned(),
                0,
                0,
                shardMoveCounts1.newlyUnassigned(),
                shardMoveCounts1.shutdownInducedMoves()
            )
        );
        summaries.add(
            new BalancingRoundSummary(
                Map.of(
                    NODE_1,
                    new BalancingRoundSummary.NodesWeightsChanges(
                        // The base weights for the next BalancingRoundSummary will be the previous BalancingRoundSummary's base + diff
                        // weights.
                        new DesiredBalanceMetrics.NodeWeightStats(
                            node1BaseWeights.shardCount() + commonDiff.shardCountDiff(),
                            node1BaseWeights.diskUsageInBytes() + commonDiff.diskUsageInBytesDiff(),
                            node1BaseWeights.writeLoad() + commonDiff.writeLoadDiff(),
                            node1BaseWeights.nodeWeight() + commonDiff.totalWeightDiff()
                        ),
                        commonDiff
                    ),
                    NODE_2,
                    new BalancingRoundSummary.NodesWeightsChanges(
                        new DesiredBalanceMetrics.NodeWeightStats(
                            node2BaseWeights.shardCount() + commonDiff.shardCountDiff(),
                            node2BaseWeights.diskUsageInBytes() + commonDiff.diskUsageInBytesDiff(),
                            node2BaseWeights.writeLoad() + commonDiff.writeLoadDiff(),
                            node2BaseWeights.nodeWeight() + commonDiff.totalWeightDiff()
                        ),
                        commonDiff
                    )
                ),
                shardMoveCounts2.movements(),
                shardMoveCounts2.newlyAssigned(),
                0,
                0,
                shardMoveCounts2.newlyUnassigned(),
                shardMoveCounts2.shutdownInducedMoves()
            )
        );

        // Combine the summaries.
        CombinedBalancingRoundSummary combined = BalancingRoundSummary.CombinedBalancingRoundSummary.combine(summaries);

        assertEquals(2, combined.numberOfBalancingRounds());
        assertEquals(shardMoveCounts1.movements() + shardMoveCounts2.movements(), combined.numberOfShardMoves());
        assertEquals(shardMoveCounts1.newlyAssigned() + shardMoveCounts2.newlyAssigned(), combined.numNewlyAssignedShards());
        assertEquals(shardMoveCounts1.newlyUnassigned() + shardMoveCounts2.newlyUnassigned(), combined.numNewlyUnassignedShards());
        assertEquals(
            shardMoveCounts1.shutdownInducedMoves() + shardMoveCounts2.shutdownInducedMoves(),
            combined.numShuttingDownMovedShards()
        );
        assertEquals(2, combined.nodeNameToWeightChanges().size());

        var combinedNode1WeightsChanges = combined.nodeNameToWeightChanges().get(NODE_1);
        var combinedNode2WeightsChanges = combined.nodeNameToWeightChanges().get(NODE_2);

        // The base weights for each node should match the first BalancingRoundSummary's base weight values. The diff weights will be summed
        // across all BalancingRoundSummary entries (in this case, there are two BalancingRoundSummary entries).

        assertEquals(node1BaseWeights.shardCount(), combinedNode1WeightsChanges.baseWeights().shardCount());
        assertDoublesEqual(node1BaseWeights.diskUsageInBytes(), combinedNode1WeightsChanges.baseWeights().diskUsageInBytes());
        assertDoublesEqual(node1BaseWeights.writeLoad(), combinedNode1WeightsChanges.baseWeights().writeLoad());
        assertDoublesEqual(node1BaseWeights.nodeWeight(), combinedNode1WeightsChanges.baseWeights().nodeWeight());
        assertEquals(2 * commonDiff.shardCountDiff(), combinedNode1WeightsChanges.weightsDiff().shardCountDiff());
        assertDoublesEqual(2 * commonDiff.diskUsageInBytesDiff(), combinedNode1WeightsChanges.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(2 * commonDiff.writeLoadDiff(), combinedNode1WeightsChanges.weightsDiff().writeLoadDiff());
        assertDoublesEqual(2 * commonDiff.totalWeightDiff(), combinedNode1WeightsChanges.weightsDiff().totalWeightDiff());

        assertEquals(node2BaseWeights.shardCount(), combinedNode2WeightsChanges.baseWeights().shardCount());
        assertDoublesEqual(node2BaseWeights.diskUsageInBytes(), combinedNode2WeightsChanges.baseWeights().diskUsageInBytes());
        assertDoublesEqual(node2BaseWeights.writeLoad(), combinedNode2WeightsChanges.baseWeights().writeLoad());
        assertDoublesEqual(node2BaseWeights.nodeWeight(), combinedNode2WeightsChanges.baseWeights().nodeWeight());
        assertEquals(2 * commonDiff.shardCountDiff(), combinedNode2WeightsChanges.weightsDiff().shardCountDiff());
        assertDoublesEqual(2 * commonDiff.diskUsageInBytesDiff(), combinedNode2WeightsChanges.weightsDiff().diskUsageInBytesDiff());
        assertDoublesEqual(2 * commonDiff.writeLoadDiff(), combinedNode2WeightsChanges.weightsDiff().writeLoadDiff());
        assertDoublesEqual(2 * commonDiff.totalWeightDiff(), combinedNode2WeightsChanges.weightsDiff().totalWeightDiff());
    }

    /**
     * Helper for double type inputs. assertEquals on double type inputs require a delta.
     */
    private void assertDoublesEqual(double expected, double actual) {
        assertEquals(expected, actual, 0.00001);
    }

}
