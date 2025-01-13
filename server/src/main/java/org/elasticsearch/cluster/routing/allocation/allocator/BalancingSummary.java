/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.core.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Data structures defining the results of allocation balancing rounds.
 */
public class BalancingSummary {

    /**
     * Holds combined {@link BalancingRoundSummary} results. Essentially holds a list of the balancing events and the summed up changes
     * across all those events: what allocation work was done across some period of time.
     *
     * @param events A list of all the cluster events that started the balancing rounds and time duration for computation + reconciliation
     *               of each event.
     * @param shardAssignments The sum of all shard movements across all combined balancing rounds.
     * @param nodeChanges The total change stats per node in the cluster from the earliest balancing round to the latest one.
     */
    record CombinedClusterBalancingRoundSummary(
        List<Tuple<Long, ClusterRebalancingEvent>> events,
        ClusterShardAssignments shardAssignments,
        Map<String, IndividualNodeRebalancingChangeStats> nodeChanges
    ) {};

    /**
     * Summarizes the impact to the cluster as a result of a rebalancing round.
     *
     * @param eventStartTime Time at which the desired balance calculation began due to a cluster event.
     * @param eventEndTime Time at which the new desired balance calculation was finished.
     * @param event Reports what provoked the rebalancing round. The rebalancer only runs when requested, not on a periodic basis.
     * @param computationFinishReason Whether the balancing round converged to a final allocation, or exiting early for some reason.
     * @param shardMovements Lists the total number of shard moves, and breaks down the total into number shards moved by category,
     *                       like node shutdown
     * @param nodeChanges A Map of node name to {@link IndividualNodeRebalancingChangeStats} to describe what each node gained and how much
     *                    work each node performed for the balancing round.
     */
    record BalancingRoundSummary(
        long eventStartTime,
        long eventEndTime,
        ClusterRebalancingEvent event,
        DesiredBalance.ComputationFinishReason computationFinishReason,
        ClusterShardAssignments shardMovements,
        Map<String, IndividualNodeRebalancingChangeStats> nodeChanges
    ) {
        @Override
        public String toString() {
            return "BalancingRoundSummary{"
                + "ClusterRebalancingEvent="
                + event
                + ", ClusterShardMovements="
                + shardMovements
                + ", NodeChangeStats={"
                + nodeChanges
                + "}"
                + '}';
        }
    };

    /**
     * General cost-benefit information on the node-level. Describes how each node was improved by a balancing round, and how much work that
     * node did to achieve the shard rebalancing.
     *
     * @param nodeWeightBeforeRebalancing
     * @param nodeWeightAfterRebalancing
     * @param dataMovedToNodeInMB
     * @param dataMovedAwayFromNodeInMB
     */
    record IndividualNodeRebalancingChangeStats(
        float nodeWeightBeforeRebalancing,
        float nodeWeightAfterRebalancing,
        long dataMovedToNodeInMB,
        long dataMovedAwayFromNodeInMB
    ) {
        @Override
        public String toString() {
            return "IndividualNodeRebalancingChangeStats{"
                + "nodeWeightBeforeRebalancing="
                + nodeWeightBeforeRebalancing
                + ", nodeWeightAfterRebalancing="
                + nodeWeightAfterRebalancing
                + ", dataMovedToNodeInMB="
                + dataMovedToNodeInMB
                + ", dataMovedAwayFromNodeInMB="
                + dataMovedAwayFromNodeInMB
                + '}';
        }
    };

    /**
     * Tracks and summarizes the more granular reasons why shards are moved between nodes.
     *
     * @param numShardsMoved total number of shard moves between nodes
     * @param numAllocationDeciderForcedShardMoves total number of shards that must be moved because they violate an AllocationDecider rule
     * @param numRebalancingShardMoves total number of shards moved to improve cluster balance and are not otherwise required to move
     * @param numShutdownForcedShardMoves total number of shards that must move off of a node because it is shutting down
     * @param numNewlyAssignedShardsNotMoved
     * @param numStuckShards total number of shards violating an AllocationDecider on their current node and on every other cluster node
     */
    public record ClusterShardAssignments(
        long numShardsMoved,
        long numAllocationDeciderForcedShardMoves,
        long numRebalancingShardMoves,
        long numShutdownForcedShardMoves,
        long numNewlyAssignedShardsNotMoved,
        long numStuckShards
    ) {
        @Override
        public String toString() {
            return "ClusterShardMovements{"
                + "numShardsMoved="
                + numShardsMoved
                + ", numAllocationDeciderForcedShardMoves="
                + numAllocationDeciderForcedShardMoves
                + ", numRebalancingShardMoves="
                + numRebalancingShardMoves
                + ", numShutdownForcedShardMoves="
                + numShutdownForcedShardMoves
                + ", numStuckShards="
                + numStuckShards
                + '}';
        }
    };

    /**
     * The cluster event that initiated a rebalancing round. This will tell us what initiated the balancer doing some amount of rebalancing
     * work.
     */
    enum ClusterRebalancingEvent {
        // TODO (Dianna): go through the reroute methods and identify the causes -- many reroute methods accept a 'reason' string -- and
        // replace them with this enum to be saved later in a balancing summary.
        RerouteCommand,
        IndexCreation,
        IndexDeletion,
        NodeShutdownAndRemoval,
        NewNodeAdded
    }

}
