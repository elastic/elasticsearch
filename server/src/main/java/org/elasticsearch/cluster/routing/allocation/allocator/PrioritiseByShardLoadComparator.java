/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.index.shard.ShardId;

import java.util.Comparator;
import java.util.Map;

/**
 * Sorts shards on one node by desirability to move, using a per-shard load map.
 * Sort order goes:
 * <ol>
 *     <li>Shards with load in <i>{@link #threshold}</i> &rarr; {@link #maxLoadOnNode} (exclusive)</li>
 *     <li>Shards with load in <i>{@link #threshold}</i> &rarr; 0</li>
 *     <li>Shards with load == {@link #maxLoadOnNode}</li>
 *     <li>Shards with missing load</li>
 * </ol>
 *
 * e.g., for any two <code>ShardRouting</code>s, <code>r1</code> and <code>r2</code>,
 * <ul>
 *     <li><code>compare(r1, r2) &gt; 0</code> when <code>r2</code> is more desirable to move</li>
 *     <li><code>compare(r1, r2) == 0</code> when the two shards are equally desirable to move</li>
 *     <li><code>compare(r1, r2) &lt; 0</code> when <code>r1</code> is more desirable to move</li>
 * </ul>
 * An empty load map compares every shard as equal.
 */
public class PrioritiseByShardLoadComparator implements Comparator<ShardRouting> {

    /**
     * This is the threshold over which we consider shards to have a "high" load represented
     * as a ratio of the maximum load present on the node.
     * <p>
     * We prefer to move shards that have a load close to <b>this value</b> x {@link #maxLoadOnNode}.
     */
    public static final double THRESHOLD_RATIO = 0.5;
    private static final double MISSING_LOAD = -1;
    private final Map<ShardId, Double> shardLoads;
    private final double maxLoadOnNode;
    private final double threshold;
    private final String nodeId;

    public PrioritiseByShardLoadComparator(Map<ShardId, Double> shardLoads, RoutingNode routingNode) {
        this.shardLoads = shardLoads;
        double maxLoadOnNode = MISSING_LOAD;
        for (ShardRouting shardRouting : routingNode) {
            maxLoadOnNode = Math.max(maxLoadOnNode, shardLoads.getOrDefault(shardRouting.shardId(), MISSING_LOAD));
        }
        this.maxLoadOnNode = maxLoadOnNode;
        threshold = maxLoadOnNode * THRESHOLD_RATIO;
        nodeId = routingNode.nodeId();
    }

    @Override
    public int compare(ShardRouting lhs, ShardRouting rhs) {
        assert nodeId.equals(lhs.currentNodeId()) && nodeId.equals(rhs.currentNodeId())
            : this.getClass().getSimpleName()
                + " is node-specific. comparator="
                + nodeId
                + ", lhs="
                + lhs.currentNodeId()
                + ", rhs="
                + rhs.currentNodeId();

        // If we have no shard load data, shortcut
        if (maxLoadOnNode == MISSING_LOAD) {
            return 0;
        }

        final double lhsLoad = shardLoads.getOrDefault(lhs.shardId(), MISSING_LOAD);
        final double rhsLoad = shardLoads.getOrDefault(rhs.shardId(), MISSING_LOAD);

        // prefer any known load over any unknown load
        final var rhsIsMissing = rhsLoad == MISSING_LOAD;
        final var lhsIsMissing = lhsLoad == MISSING_LOAD;
        if (rhsIsMissing && lhsIsMissing) {
            return 0;
        }
        if (rhsIsMissing ^ lhsIsMissing) {
            return lhsIsMissing ? 1 : -1;
        }

        if (lhsLoad < maxLoadOnNode && rhsLoad < maxLoadOnNode) {
            final var lhsOverThreshold = lhsLoad >= threshold;
            final var rhsOverThreshold = rhsLoad >= threshold;
            if (lhsOverThreshold && rhsOverThreshold) {
                // Both values between threshold and maximum, prefer lowest
                return Double.compare(lhsLoad, rhsLoad);
            } else if (lhsOverThreshold) {
                // lhs between threshold and maximum, rhs below threshold, prefer lhs
                return -1;
            } else if (rhsOverThreshold) {
                // lhs below threshold, rhs between threshold and maximum, prefer rhs
                return 1;
            }
            // Both values below the threshold, prefer highest
            return Double.compare(rhsLoad, lhsLoad);
        }

        // prefer the non-max load if there is one
        return Double.compare(lhsLoad, rhsLoad);
    }

    /**
     * Ranks shards by write load.
     */
    public static class PrioritiseByShardWriteLoadComparator extends PrioritiseByShardLoadComparator {

        public PrioritiseByShardWriteLoadComparator(ClusterInfo clusterInfo, RoutingNode routingNode) {
            super(clusterInfo.getShardWriteLoads(), routingNode);
        }
    }

    // TODO - PrioritiseByShardSearchLoadComparator?
}
