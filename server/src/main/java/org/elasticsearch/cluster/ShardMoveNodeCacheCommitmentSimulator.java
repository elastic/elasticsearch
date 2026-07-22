/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements.NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT;

/**
 * Simulates the impact to each node's cache commitment in response to the movement of individual
 * shards around the cluster.
 */
public class ShardMoveNodeCacheCommitmentSimulator {

    private static final Logger logger = LogManager.getLogger(ShardMoveNodeCacheCommitmentSimulator.class);

    private final Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements;
    private final Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments;

    public ShardMoveNodeCacheCommitmentSimulator(ClusterInfo clusterInfo) {
        this.shardCacheRequirements = new HashMap<>(clusterInfo.getShardCacheRequirements());
        this.nodeCacheSizeAndCommitments = new HashMap<>(clusterInfo.getNodeCacheSizeAndCommitments());
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

        public long applyDelta(long currentValue, long shardRequirement) {
            if (shardRequirement == NO_BOOSTED_OR_UNBOOSTED_CACHE_REQUIREMENT) {
                return currentValue;
            }
            return Math.addExact(currentValue, sign * shardRequirement);
        }
    }

    private void modifyNodeCacheCommitment(String nodeId, BoostedAndUnboostedCacheRequirements requirement, Modification modification) {
        var current = nodeCacheSizeAndCommitments.get(nodeId);
        if (current == null) {
            logger.trace("no cache size/commitment recorded for node [{}], skipping cache commitment simulation", nodeId);
            return;
        }

        long updatedBoostedCommitment = modification.applyDelta(
            current.boostedCacheCommitmentInBytes(),
            requirement.boostedCacheRequirementInBytes()
        );
        long updatedUnboostedCommitment = modification.applyDelta(
            current.unboostedCacheCommitmentInBytes(),
            requirement.unboostedCacheRequirementInBytes()
        );
        // ClusterInfo gives us a consistent snapshot of node commitments and shard requirements, so a shard's requirement should
        // never be subtracted from a node that didn't have it counted in the first place.
        assert updatedBoostedCommitment >= 0
            : "boosted cache commitment for node [" + nodeId + "] went negative: " + updatedBoostedCommitment;
        assert updatedUnboostedCommitment >= 0
            : "unboosted cache commitment for node [" + nodeId + "] went negative: " + updatedUnboostedCommitment;

        nodeCacheSizeAndCommitments.put(
            nodeId,
            new NodeCacheSizeAndCommitments(current.cacheSizeInBytes(), updatedBoostedCommitment, updatedUnboostedCommitment)
        );
    }

    public Map<String, NodeCacheSizeAndCommitments> getSimulatedNodeCacheSizeAndCommitments() {
        return Collections.unmodifiableMap(nodeCacheSizeAndCommitments);
    }

    /**
     * The shard cache requirements used for this simulation, unaffected by shard movement.
     */
    public Map<ShardId, BoostedAndUnboostedCacheRequirements> getShardCacheRequirements() {
        return Collections.unmodifiableMap(shardCacheRequirements);
    }
}
