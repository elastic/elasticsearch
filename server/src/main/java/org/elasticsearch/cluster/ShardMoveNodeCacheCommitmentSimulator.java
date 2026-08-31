/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements.NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT;

/**
 * Simulates the impact to each node's cache commitment in response to the movement of individual
 * shards around the cluster. Deltas from shard movements are accumulated first and applied to the
 * initial commitments (with clamping to 0) only when the simulated result is read, since {@link ClusterInfo}
 * snapshots can contain contradictory shard placement information that would otherwise make an
 * intermediate commitment go negative, even though the net effect of all movements would not. Clamping is
 * therefore performed once at the end, rather than after every single shard movement.
 */
public class ShardMoveNodeCacheCommitmentSimulator {

    private static final Logger logger = LogManager.getLogger(ShardMoveNodeCacheCommitmentSimulator.class);

    private final Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements;
    private final Map<String, NodeCacheSizeAndCommitments> initialNodeCacheSizeAndCommitments;
    // hppc's primitive-valued map avoids wrapping each delta in a Long object for every shard movement
    // accumulated here, across what can be many thousands of calls to simulateShardStarted() in a single
    // desired balance computation.
    private final ObjectLongMap<String> boostedCommitmentDeltaByNode;
    private final ObjectLongMap<String> unboostedCommitmentDeltaByNode;

    public ShardMoveNodeCacheCommitmentSimulator(ClusterInfo clusterInfo) {
        this.shardCacheRequirements = Map.copyOf(clusterInfo.getShardCacheRequirements());
        this.initialNodeCacheSizeAndCommitments = Map.copyOf(clusterInfo.getNodeCacheSizeAndCommitments());
        this.boostedCommitmentDeltaByNode = new ObjectLongHashMap<>();
        this.unboostedCommitmentDeltaByNode = new ObjectLongHashMap<>();
    }

    public void simulateShardStarted(ShardRouting shard) {
        var requirement = shardCacheRequirements.get(shard.shardId());
        if (requirement == null) {
            logger.trace("no cache requirement recorded for shard [{}], skipping cache commitment simulation", shard.shardId());
            return;
        }

        modifyNodeCacheCommitment(shard.currentNodeId(), requirement, Modification.ADD);

        if (shard.relocatingNodeId() != null) {
            modifyNodeCacheCommitment(shard.relocatingNodeId(), requirement, Modification.REMOVE);
        }
    }

    private enum Modification {
        ADD(1),
        REMOVE(-1);

        private final int sign;

        Modification(int sign) {
            this.sign = sign;
        }
    }

    private void modifyNodeCacheCommitment(String nodeId, BoostedAndUnboostedCacheRequirements requirement, Modification modification) {
        if (initialNodeCacheSizeAndCommitments.containsKey(nodeId) == false) {
            logger.trace("no cache size/commitment recorded for node [{}], skipping cache commitment simulation", nodeId);
            return;
        }

        if (requirement.boostedCacheRequirementInBytes() != NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT) {
            long delta = modification.sign * requirement.boostedCacheRequirementInBytes();
            boostedCommitmentDeltaByNode.put(nodeId, Math.addExact(boostedCommitmentDeltaByNode.getOrDefault(nodeId, 0L), delta));
        }
        if (requirement.unboostedCacheRequirementInBytes() != NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT) {
            long delta = modification.sign * requirement.unboostedCacheRequirementInBytes();
            unboostedCommitmentDeltaByNode.put(nodeId, Math.addExact(unboostedCommitmentDeltaByNode.getOrDefault(nodeId, 0L), delta));
        }
    }

    /**
     * Applies the accumulated deltas from simulated shard movements to the initial commitments, clamping
     * boosted and/or unboosted commitment to 0 to avoid producing a negative commitment.
     */
    public Map<String, NodeCacheSizeAndCommitments> getSimulatedNodeCacheSizeAndCommitments() {
        if (boostedCommitmentDeltaByNode.isEmpty() && unboostedCommitmentDeltaByNode.isEmpty()) {
            return Collections.unmodifiableMap(initialNodeCacheSizeAndCommitments);
        }
        return initialNodeCacheSizeAndCommitments.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, entry -> {
            var nodeId = entry.getKey();
            if (boostedCommitmentDeltaByNode.containsKey(nodeId) == false && unboostedCommitmentDeltaByNode.containsKey(nodeId) == false) {
                return entry.getValue();
            }
            var initial = entry.getValue();
            long updatedBoostedCommitment = Math.max(
                0,
                Math.addExact(initial.boostedCacheCommitmentInBytes(), boostedCommitmentDeltaByNode.get(nodeId))
            );
            long updatedUnboostedCommitment = Math.max(
                0,
                Math.addExact(initial.unboostedCacheCommitmentInBytes(), unboostedCommitmentDeltaByNode.get(nodeId))
            );
            return new NodeCacheSizeAndCommitments(initial.cacheSizeInBytes(), updatedBoostedCommitment, updatedUnboostedCommitment);
        }));
    }

    /**
     * The shard cache requirements used for this simulation, unaffected by shard movement.
     */
    public Map<ShardId, BoostedAndUnboostedCacheRequirements> getShardCacheRequirements() {
        return shardCacheRequirements;
    }
}
