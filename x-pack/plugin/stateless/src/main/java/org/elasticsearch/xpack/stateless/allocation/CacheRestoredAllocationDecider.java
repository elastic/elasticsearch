/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;

/**
 * Handles allocation decisions for cache-restored replacement nodes.
 *
 * <p>Two responsibilities, both scoped to the active replacement window (while the source node
 * remains in the cluster):
 * <ol>
 *   <li><b>Audit trail</b> — emits a labelled {@code Decision.YES} for (shard, node) pairs where
 *       the replacement node's cache was restored from the shard's current home, visible in
 *       {@code GET /_cluster/allocation/explain}.</li>
 *   <li><b>Parallel-clone isolation</b> — when multiple replacement nodes are in the cluster
 *       simultaneously (operator is cloning several nodes at once), returns {@code Decision.NO}
 *       for any (shard, node) pair where the node was cloned from a <em>different</em> source,
 *       preventing shards from cross-assigning to the wrong warm cache. The {@code NO} is lifted
 *       to {@code YES} when the correct replacement is absent from the cluster so that shard
 *       allocation is never indefinitely stalled.</li>
 * </ol>
 *
 * <p>Once the source node has left the cluster the attribute on the replacement is stale and
 * both responsibilities cease: the decider returns {@code Decision.YES} unconditionally and
 * {@link StatelessBalancingWeightsFactory}'s weight boost no longer applies.
 *
 * <p>Preference ranking is handled by {@link StatelessBalancingWeightsFactory}.
 */
public class CacheRestoredAllocationDecider extends AllocationDecider {

    public static final String NAME = "cache_restored";

    /** The node attribute key set on a replacement node to advertise the source node ID. */
    public static final String CACHE_RESTORED_FROM_ATTR = "es_cache_restored_from_node";

    /**
     * Returns {@code true} when {@code node} carries {@link #CACHE_RESTORED_FROM_ATTR} and the
     * named source node is still a member of the cluster — the active replacement window shared
     * by this decider and {@link StatelessBalancingWeightsFactory}'s weight boost.
     */
    static boolean isReplacementWindowActive(DiscoveryNode node, DiscoveryNodes nodes) {
        String restoredFrom = node.getAttributes().get(CACHE_RESTORED_FROM_ATTR);
        if (restoredFrom == null) {
            return false;
        }
        return nodes.get(restoredFrom) != null;
    }

    @Override
    public Decision canAllocate(ShardRouting shard, RoutingNode node, RoutingAllocation allocation) {
        String restoredFrom = node.node().getAttributes().get(CACHE_RESTORED_FROM_ATTR);
        if (isReplacementWindowActive(node.node(), allocation.nodes()) == false) {
            return Decision.YES;
        }
        if (restoredFrom.equals(shard.currentNodeId())) {
            // This node's cache was restored from the shard's current home.
            return allocation.decision(Decision.YES, NAME, "node has warm cache restored from source node [%s]", restoredFrom);
        }
        // This is an attributed replacement, but not the one cloned from this shard's source.
        // Defer to the correct replacement if it is present in the cluster.
        if (correctReplacementExists(shard.currentNodeId(), allocation)) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "node's warm cache was restored from [%s], not this shard's current node [%s]; "
                    + "deferring to the designated replacement node",
                restoredFrom,
                shard.currentNodeId()
            );
        }
        // The correct replacement is absent — allow fallback so allocation is never stalled.
        return Decision.YES;
    }

    /**
     * Returns true if any node currently in the cluster carries
     * {@code es_cache_restored_from_node == sourceNodeId}, i.e. a designated replacement
     * for that source is present and eligible to receive the shard.
     */
    private static boolean correctReplacementExists(String sourceNodeId, RoutingAllocation allocation) {
        if (sourceNodeId == null) {
            return false;
        }
        for (RoutingNode rn : allocation.routingNodes()) {
            if (sourceNodeId.equals(rn.node().getAttributes().get(CACHE_RESTORED_FROM_ATTR))) {
                return true;
            }
        }
        return false;
    }
}
