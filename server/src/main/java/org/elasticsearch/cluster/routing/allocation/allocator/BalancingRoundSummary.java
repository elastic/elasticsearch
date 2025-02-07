/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Summarizes the impact to the cluster as a result of a rebalancing round.
 *
 * @param nodeNameToWeightChanges The shard balance weight changes for each node (by name), comparing a previous DesiredBalance shard
 *                                allocation to a new DesiredBalance allocation.
 * @param numberOfShardsToMove The number of shard moves required to move from the previous desired balance to the new one. Does not include
 *                             new (index creation) or removed (index deletion) shard assignements.
 */
public record BalancingRoundSummary(Map<String, NodesWeightsChanges> nodeNameToWeightChanges, long numberOfShardsToMove) {

    /**
     * Represents the change in weights for a node going from an old DesiredBalance to a new DesiredBalance
     * Saves the node weights of an old DesiredBalance, along with a diff against a newer DesiredBalance.
     *
     * @param weights The starting {@link DesiredBalanceMetrics.NodeWeightStats} of a previous DesiredBalance.
     * @param nextWeightsDiff The difference between a previous DesiredBalance and a new DesiredBalance.
     */
    record NodesWeightsChanges(DesiredBalanceMetrics.NodeWeightStats weights, NodeWeightsDiff nextWeightsDiff) {}

    /**
     * Represents the change of shard balance weights for a node, comparing an older DesiredBalance with the latest DesiredBalance.
     *
     * @param shardCountDiff How many more, or less, shards are assigned to the node in the latest DesiredBalance.
     * @param diskUsageInBytesDiff How much more, or less, disk is used by shards assigned to the node in the latest DesiredBalance.
     * @param writeLoadDiff How much more, or less, write load is estimated for shards assigned to the node in the latest DesiredBalance.
     * @param totalWeightDiff How much more, or less, the total weight is of shards assigned to the node in the latest DesiredBalance.
     */
    record NodeWeightsDiff(long shardCountDiff, double diskUsageInBytesDiff, double writeLoadDiff, double totalWeightDiff) {
        public static NodeWeightsDiff create(DesiredBalanceMetrics.NodeWeightStats base, DesiredBalanceMetrics.NodeWeightStats next) {
            return new NodeWeightsDiff(
                next.shardCount() - base.shardCount(),
                next.diskUsageInBytes() - base.diskUsageInBytes(),
                next.writeLoad() - base.writeLoad(),
                next.nodeWeight() - base.nodeWeight()
            );
        }
    }

    @Override
    public String toString() {
        return "BalancingRoundSummary{"
            + "nodeNameToWeightChanges"
            + nodeNameToWeightChanges
            + ", numberOfShardsToMove="
            + numberOfShardsToMove
            + '}';
    }

    /**
     * Holds combined {@link BalancingRoundSummary} results. Essentially holds a list of the balancing events and the summed up changes
     * across all those events: what allocation work was done across some period of time.
     * TODO: WIP ES-10341
     *
     * Note that each balancing round summary is the difference between, at the time, latest desired balance and the previous desired balance.
     * Each summary represents a step towards the next desired balance, which is based on presuming the previous desired balance is reached. So
     * combining them is roughly the difference between the first summary's previous desired balance and the last summary's latest desired
     * balance.
     *
     * @param numberOfBalancingRounds How many balancing round summaries are combined in this report.
     * @param nodeNameToWeightChanges
     * @param numberOfShardMoves The sum of shard moves for each balancing round being combined into a single summary.
     */
    public record CombinedBalancingRoundSummary(
        int numberOfBalancingRounds,
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges,
        long numberOfShardMoves
    ) {

        public static final CombinedBalancingRoundSummary EMPTY_RESULTS = new CombinedBalancingRoundSummary(0, new HashMap<>(), 0);

        public static CombinedBalancingRoundSummary combine(List<BalancingRoundSummary> summaries) {
            if (summaries.isEmpty()) {
                return EMPTY_RESULTS;
            }

            // Initialize the combined weight changes with the oldest changes. We can then build the combined changes by adding the diffs of
            // newer weight changes. If a new node gets added in a later summary, then we will initialize its weights starting there.
            // Similarly, a node may be removed in a later summary: in this case we will keep that nodes work, up until it was removed.
            var iterator = summaries.iterator();
            assert iterator.hasNext();
            var firstSummary = iterator.next();
            Map<String, NodesWeightsChanges> combinedNodeNameToWeightChanges = new HashMap<>(firstSummary.nodeNameToWeightChanges);

            // Number of shards moves are simply summed across summaries. Each new balancing round is built upon the last one, so it is
            // possible that a shard is reassigned back to a node before it even moves away, and that will still be counted as 2 moves here.
            long numberOfShardMoves = firstSummary.numberOfShardsToMove;

            // Initialize with 1 because we've already begun to iterate the summaries.
            int numSummaries = 1;

            // Iterate any remaining summaries (after the first one).
            while (iterator.hasNext()) {
                var summary = iterator.next();
                for (var nodeNameAndWeights : summary.nodeNameToWeightChanges.entrySet()) {
                    var combined = combinedNodeNameToWeightChanges.get(nodeNameAndWeights.getKey());
                    if (combined == null) {
                        // Encountered a new node in a later summary. Add the new node initializing it with the base weights from that
                        // summary.
                        combinedNodeNameToWeightChanges.put(nodeNameAndWeights.getKey(), nodeNameAndWeights.getValue());
                    } else {
                        var newCombinedDiff = new NodeWeightsDiff(
                            combined.nextWeightsDiff.shardCountDiff + nodeNameAndWeights.getValue().nextWeightsDiff.shardCountDiff,
                            combined.nextWeightsDiff.diskUsageInBytesDiff + nodeNameAndWeights
                                .getValue().nextWeightsDiff.diskUsageInBytesDiff,
                            combined.nextWeightsDiff.writeLoadDiff + nodeNameAndWeights.getValue().nextWeightsDiff.writeLoadDiff,
                            combined.nextWeightsDiff.totalWeightDiff + nodeNameAndWeights.getValue().nextWeightsDiff.totalWeightDiff
                        );
                        var newCombinedChanges = new NodesWeightsChanges(combined.weights, newCombinedDiff);
                        combinedNodeNameToWeightChanges.compute(nodeNameAndWeights.getKey(), (k, weightChanges) -> newCombinedChanges);
                    }
                }

                ++numSummaries;
                numberOfShardMoves += summary.numberOfShardsToMove();
            }

            return new CombinedBalancingRoundSummary(numSummaries, combinedNodeNameToWeightChanges, numberOfShardMoves);
        }

    }

}
