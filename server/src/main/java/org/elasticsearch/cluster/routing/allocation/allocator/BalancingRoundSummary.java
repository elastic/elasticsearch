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
 *                             newly assigned or unassigned shard copies.
 * @param numNewlyAssignedShards The number of shard copies that are newly assigned to a data node, not moving from another node. New
 *                               assignment is typically the result of creating an index or increasing the replica count.
 * @param numDeciderMovedShards TODO
 * @param numBalancingMovedShards TODO
 * @param numNewlyUnassignedShards The number of shard copies that are newly assigned. Unassignment could occur from deletion of an index or
 *                                 unexpected loss of a data node (and failure to reassign elsewhere).
 * @param numShuttingDownMovedShards A subset of {@link #numberOfShardsToMove} to surface the shard allocation cost of shutting down nodes.
 */
public record BalancingRoundSummary(
    Map<String, NodesWeightsChanges> nodeNameToWeightChanges,
    long numberOfShardsToMove,
    long numNewlyAssignedShards,
    long numDeciderMovedShards,
    long numBalancingMovedShards,
    long numNewlyUnassignedShards,
    long numShuttingDownMovedShards
) {

    /**
     * Represents the change in weights for a node going from an old DesiredBalance to a new DesiredBalance
     * Saves the node weights of an old DesiredBalance, along with a diff against a newer DesiredBalance.
     *
     * @param baseWeights The starting {@link DesiredBalanceMetrics.NodeWeightStats} of a previous DesiredBalance.
     * @param weightsDiff The difference between the {@code baseWeights} and a new DesiredBalance.
     */
    record NodesWeightsChanges(DesiredBalanceMetrics.NodeWeightStats baseWeights, NodeWeightsDiff weightsDiff) {}

    /**
     * Represents the change of shard balance weights for a node, comparing an older DesiredBalance with the latest DesiredBalance.
     *
     * @param shardCountDiff How many more, or less, shards are assigned to the node in the latest DesiredBalance.
     * @param diskUsageInBytesDiff How much more, or less, disk is used by shards assigned to the node in the latest DesiredBalance.
     * @param writeLoadDiff How much more, or less, write load is estimated for shards assigned to the node in the latest DesiredBalance.
     * @param totalWeightDiff How much more, or less, the total weight is of shards assigned to the node in the latest DesiredBalance.
     */
    record NodeWeightsDiff(long shardCountDiff, double diskUsageInBytesDiff, double writeLoadDiff, double totalWeightDiff) {

        /**
         * Creates a diff where the {@code base} weights will be subtracted from the {@code next} weights, to show the changes made to reach
         * the {@code next} weights.
         *
         * @param base has the original weights
         * @param next has the new weights
         * @return The diff of ({@code next} - {@code base})
         */
        public static NodeWeightsDiff create(DesiredBalanceMetrics.NodeWeightStats base, DesiredBalanceMetrics.NodeWeightStats next) {
            return new NodeWeightsDiff(
                next.shardCount() - base.shardCount(),
                next.diskUsageInBytes() - base.diskUsageInBytes(),
                next.writeLoad() - base.writeLoad(),
                next.nodeWeight() - base.nodeWeight()
            );
        }

        /**
         * Creates a new {@link NodeWeightsDiff} summing this instance's values with {@code otherDiff}'s values.
         */
        public NodeWeightsDiff combine(NodeWeightsDiff otherDiff) {
            return new NodeWeightsDiff(
                this.shardCountDiff + otherDiff.shardCountDiff,
                this.diskUsageInBytesDiff + otherDiff.diskUsageInBytesDiff,
                this.writeLoadDiff + otherDiff.writeLoadDiff,
                this.totalWeightDiff + otherDiff.totalWeightDiff
            );
        }
    }

    @Override
    public String toString() {
        return "BalancingRoundSummary{"
            + "nodeNameToWeightChanges="
            + nodeNameToWeightChanges
            + ", numberOfShardsToMove="
            + numberOfShardsToMove
            + ", numNewlyAssignedShards="
            + numNewlyAssignedShards
            + ", numDeciderMovedShards="
            + numDeciderMovedShards
            + ", numBalancingMovedShards="
            + numBalancingMovedShards
            + ", numNewlyUnassignedShards="
            + numNewlyUnassignedShards
            + ", numShuttingDownMovedShards="
            + numShuttingDownMovedShards
            + '}';
    }

    /**
     * Holds combined {@link BalancingRoundSummary} results. Essentially holds a list of the balancing events and the summed up changes
     * across all those events: what allocation work was done across some period of time.
     * TODO: WIP ES-10341
     *
     * Note that each balancing round summary is the difference between, at the time, latest desired balance and the previous desired
     * balance. Each summary represents a step towards the next desired balance, which is based on presuming the previous desired balance is
     * reached. So combining them is roughly the difference between the first summary's previous desired balance and the last summary's
     * latest desired balance.
     *
     * @param numberOfBalancingRounds How many balancing round summaries are combined in this report.
     * @param nodeNameToWeightChanges The weight changes per node across the combined summaries.
     * @param numberOfShardMoves The sum of shard moves for each balancing round being combined into a single summary.
     */
    public record CombinedBalancingRoundSummary(
        int numberOfBalancingRounds,
        Map<String, NodesWeightsChanges> nodeNameToWeightChanges,
        long numberOfShardMoves,
        long numNewlyAssignedShards,
        long numDeciderMovedShards,
        long numBalancingMovedShards,
        long numNewlyUnassignedShards,
        long numShuttingDownMovedShards
    ) {

        public static final CombinedBalancingRoundSummary EMPTY_RESULTS = new CombinedBalancingRoundSummary(
            0,
            new HashMap<>(),
            0,
            0,
            0,
            0,
            0,
            0
        );

        /**
         * Merges multiple {@link BalancingRoundSummary} summaries into a single {@link CombinedBalancingRoundSummary}.
         */
        public static CombinedBalancingRoundSummary combine(List<BalancingRoundSummary> summaries) {
            if (summaries.isEmpty()) {
                return EMPTY_RESULTS;
            }

            // We will loop through the summaries and sum the weight diffs for each node entry.
            Map<String, NodesWeightsChanges> combinedNodeNameToWeightChanges = new HashMap<>();

            // Number of shards moves are simply summed across summaries. Each new balancing round is built upon the last one, so it is
            // possible that a shard is reassigned back to a node before it even moves away, and that will still be counted as 2 moves here.
            long numberOfShardMoves = 0;
            long numNewlyAssignedShards = 0;
            long numDeciderMovedShards = 0;
            long numBalancingMovedShards = 0;
            long numNewlyUnassignedShards = 0;
            long numShuttingDownMovedShards = 0;

            // Total number of summaries that are being combined.
            int numSummaries = 0;

            var iterator = summaries.iterator();
            while (iterator.hasNext()) {
                var summary = iterator.next();

                // We'll build the weight changes by keeping the node weight base from the first summary in which a node appears and then
                // summing the weight diffs in each summary to get total weight diffs across summaries.
                for (var nodeNameAndWeights : summary.nodeNameToWeightChanges.entrySet()) {
                    var combined = combinedNodeNameToWeightChanges.get(nodeNameAndWeights.getKey());
                    if (combined == null) {
                        // Either this is the first summary, and combinedNodeNameToWeightChanges hasn't been initialized yet for this node;
                        // or a later balancing round had a new node. Either way, initialize the node entry with the weight changes from the
                        // first summary in which it appears.
                        combinedNodeNameToWeightChanges.put(nodeNameAndWeights.getKey(), nodeNameAndWeights.getValue());
                    } else {
                        // We have at least two summaries containing this node, so let's combine them.
                        var newCombinedChanges = new NodesWeightsChanges(
                            combined.baseWeights,
                            combined.weightsDiff.combine(nodeNameAndWeights.getValue().weightsDiff())
                        );
                        combinedNodeNameToWeightChanges.put(nodeNameAndWeights.getKey(), newCombinedChanges);
                    }
                }

                ++numSummaries;
                numberOfShardMoves += summary.numberOfShardsToMove();
                numNewlyAssignedShards += summary.numNewlyAssignedShards();
                numDeciderMovedShards += summary.numDeciderMovedShards();
                numBalancingMovedShards += summary.numBalancingMovedShards();
                numNewlyUnassignedShards += summary.numNewlyUnassignedShards();
                numShuttingDownMovedShards += summary.numShuttingDownMovedShards();
            }

            return new CombinedBalancingRoundSummary(
                numSummaries,
                combinedNodeNameToWeightChanges,
                numberOfShardMoves,
                numNewlyAssignedShards,
                numDeciderMovedShards,
                numBalancingMovedShards,
                numNewlyUnassignedShards,
                numShuttingDownMovedShards
            );
        }

        @Override
        public String toString() {
            return "BalancingRoundSummary{"
                + "nodeNameToWeightChanges="
                + nodeNameToWeightChanges
                + ", numberOfShardMoves="
                + numberOfShardMoves
                + ", numNewlyAssignedShards="
                + numNewlyAssignedShards
                + ", numDeciderMovedShards="
                + numDeciderMovedShards
                + ", numBalancingMovedShards="
                + numBalancingMovedShards
                + ", numNewlyUnassignedShards="
                + numNewlyUnassignedShards
                + ", numShuttingDownMovedShards="
                + numShuttingDownMovedShards
                + '}';
        }
    }

}
