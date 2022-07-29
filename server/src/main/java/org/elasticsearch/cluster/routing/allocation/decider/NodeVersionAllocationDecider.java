/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RecoverySource.SnapshotRecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;

/**
 * An allocation decider that prevents relocation or allocation from nodes
 * that might not be version compatible. If we relocate from a node that runs
 * a newer version than the node we relocate to this might cause {@link org.apache.lucene.index.IndexFormatTooNewException}
 * on the lowest level since it might have already written segments that use a new postings format or codec that is not
 * available on the target node.
 */
public class NodeVersionAllocationDecider extends AllocationDecider {

    public static final String NAME = "node_version";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.currentNodeId() == null) {
                if (shardRouting.recoverySource() != null && shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    // restoring from a snapshot - check that the node can handle the version
                    return isVersionCompatible((SnapshotRecoverySource) shardRouting.recoverySource(), node, allocation);
                } else {
                    // existing or fresh primary on the node
                    return allocation.decision(Decision.YES, NAME, "the primary shard is new or already existed on the node");
                }
            } else {
                // relocating primary, only migrate to newer host
                return isVersionCompatibleRelocatePrimary(allocation.routingNodes(), shardRouting.currentNodeId(), node, allocation);
            }
        } else {
            final ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting.shardId());
            // check that active primary has a newer version so that peer recovery works
            if (primary != null) {
                return isVersionCompatibleAllocatingReplica(allocation.routingNodes(), primary.currentNodeId(), node, allocation);
            } else {
                // ReplicaAfterPrimaryActiveAllocationDecider should prevent this case from occurring
                return allocation.decision(Decision.YES, NAME, "no active primary shard yet");
            }
        }
    }

    @Override
    public Decision canForceAllocateDuringReplace(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocate(shardRouting, node, allocation);
    }

    private static Decision isVersionCompatibleRelocatePrimary(
        final RoutingNodes routingNodes,
        final String sourceNodeId,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getVersion().onOrAfter(source.node().getVersion())) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "can relocate primary shard from a node with version [%s] to a node with equal-or-newer version [%s]",
                source.node().getVersion(),
                target.node().getVersion()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "cannot relocate primary shard from a node with version [%s] to a node with older version [%s]",
                source.node().getVersion(),
                target.node().getVersion()
            );
        }
    }

    private static Decision isVersionCompatibleAllocatingReplica(
        final RoutingNodes routingNodes,
        final String sourceNodeId,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getVersion().onOrAfter(source.node().getVersion())) {
            /* we can allocate if we can recover from a node that is younger or on the same version
             * if the primary is already running on a newer version that won't work due to possible
             * differences in the lucene index format etc.*/
            return allocation.decision(
                Decision.YES,
                NAME,
                "can allocate replica shard to a node with version [%s] since this is equal-or-newer than the primary version [%s]",
                target.node().getVersion(),
                source.node().getVersion()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "cannot allocate replica shard to a node with version [%s] since this is older than the primary version [%s]",
                target.node().getVersion(),
                source.node().getVersion()
            );
        }
    }

    private static Decision isVersionCompatible(
        SnapshotRecoverySource recoverySource,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        if (target.node().getVersion().onOrAfter(recoverySource.version())) {
            /* we can allocate if we can restore from a snapshot that is older or on the same version */
            return allocation.decision(
                Decision.YES,
                NAME,
                "node version [%s] is the same or newer than snapshot version [%s]",
                target.node().getVersion(),
                recoverySource.version()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "node version [%s] is older than the snapshot version [%s]",
                target.node().getVersion(),
                recoverySource.version()
            );
        }
    }
}
