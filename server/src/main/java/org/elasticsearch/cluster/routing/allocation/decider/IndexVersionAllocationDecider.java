/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
 * that might not be index compatible. If we relocate from a node that uses
 * a newer index version than the node we relocate to this might cause {@link org.apache.lucene.index.IndexFormatTooNewException}
 * on the lowest level since it might have already written segments that use a new postings format or codec that is not
 * available on the target node.
 */
public class IndexVersionAllocationDecider extends AllocationDecider {

    public static final String NAME = "index_version";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        if (shardRouting.primary()) {
            if (shardRouting.currentNodeId() == null) {
                if (shardRouting.recoverySource().getType() == RecoverySource.Type.SNAPSHOT) {
                    // restoring from a snapshot - check that the node can handle the version
                    return isVersionCompatible((SnapshotRecoverySource) shardRouting.recoverySource(), node, allocation);
                } else {
                    // existing or fresh primary on the node
                    return allocation.decision(Decision.YES, NAME, "no existing allocation, assuming compatible");
                }
            } else {
                // relocating primary, only migrate to newer host
                return isIndexVersionCompatibleRelocatePrimary(allocation.routingNodes(), shardRouting.currentNodeId(), node, allocation);
            }
        } else {
            final ShardRouting primary = allocation.routingNodes().activePrimary(shardRouting.shardId());
            // check that active primary has a newer version so that peer recovery works
            if (primary != null) {
                return isIndexVersionCompatibleAllocatingReplica(allocation.routingNodes(), primary.currentNodeId(), node, allocation);
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

    private static Decision isIndexVersionCompatibleRelocatePrimary(
        final RoutingNodes routingNodes,
        final String sourceNodeId,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getMaxIndexVersion().onOrAfter(source.node().getMaxIndexVersion())) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "can relocate primary shard from a node with index version [%s] to a node with equal-or-newer index version [%s]",
                source.node().getMaxIndexVersion().toReleaseVersion(),
                target.node().getMaxIndexVersion().toReleaseVersion()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "cannot relocate primary shard from a node with index version [%s] to a node with older index version [%s]",
                source.node().getMaxIndexVersion().toReleaseVersion(),
                target.node().getMaxIndexVersion().toReleaseVersion()
            );
        }
    }

    private static Decision isIndexVersionCompatibleAllocatingReplica(
        final RoutingNodes routingNodes,
        final String sourceNodeId,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        final RoutingNode source = routingNodes.node(sourceNodeId);
        if (target.node().getMaxIndexVersion().onOrAfter(source.node().getMaxIndexVersion())) {
            /* we can allocate if we can recover from a node that is younger or on the same version
             * if the primary is already running on a newer version that won't work due to possible
             * differences in the lucene index format etc.*/
            return allocation.decision(
                Decision.YES,
                NAME,
                "can allocate replica shard to a node with index version [%s]"
                    + " since this is equal-or-newer than the primary index version [%s]",
                target.node().getMaxIndexVersion().toReleaseVersion(),
                source.node().getMaxIndexVersion().toReleaseVersion()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "cannot allocate replica shard to a node with index version [%s]"
                    + " since this is older than the primary index version [%s]",
                target.node().getMaxIndexVersion().toReleaseVersion(),
                source.node().getMaxIndexVersion().toReleaseVersion()
            );
        }
    }

    private static Decision isVersionCompatible(
        SnapshotRecoverySource recoverySource,
        final RoutingNode target,
        final RoutingAllocation allocation
    ) {
        if (target.node().getMaxIndexVersion().onOrAfter(recoverySource.version())) {
            /* we can allocate if we can restore from a snapshot that is older or on the same version */
            return allocation.decision(
                Decision.YES,
                NAME,
                "max supported index version [%s] is the same or newer than snapshot version [%s]",
                target.node().getMaxIndexVersion().toReleaseVersion(),
                recoverySource.version().toReleaseVersion()
            );
        } else {
            return allocation.decision(
                Decision.NO,
                NAME,
                "max supported index version [%s] is older than the snapshot version [%s]",
                target.node().getMaxIndexVersion().toReleaseVersion(),
                recoverySource.version().toReleaseVersion()
            );
        }
    }
}
