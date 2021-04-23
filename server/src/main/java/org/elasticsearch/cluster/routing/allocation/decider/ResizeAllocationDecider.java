/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

/**
 * An allocation decider that ensures we allocate the shards of a target index for resize operations next to the source primaries
 */
public class ResizeAllocationDecider extends AllocationDecider {

    public static final String NAME = "resize";

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
        return canAllocate(shardRouting, null, allocation);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        final UnassignedInfo unassignedInfo = shardRouting.unassignedInfo();
        if (unassignedInfo != null && shardRouting.recoverySource().getType() == RecoverySource.Type.LOCAL_SHARDS) {
            // we only make decisions here if we have an unassigned info and we have to recover from another index ie. split / shrink
            final IndexMetadata indexMetadata = allocation.metadata().getIndexSafe(shardRouting.index());
            Index resizeSourceIndex = indexMetadata.getResizeSourceIndex();
            assert resizeSourceIndex != null;
            if (allocation.metadata().index(resizeSourceIndex) == null) {
                return allocation.decision(Decision.NO, NAME, "resize source index [%s] doesn't exists", resizeSourceIndex.toString());
            }
            IndexMetadata sourceIndexMetadata = allocation.metadata().getIndexSafe(resizeSourceIndex);
            if (indexMetadata.getNumberOfShards() < sourceIndexMetadata.getNumberOfShards()) {
                // this only handles splits and clone so far.
                return Decision.ALWAYS;
            }

            ShardId shardId = indexMetadata.getNumberOfShards() == sourceIndexMetadata.getNumberOfShards() ?
                IndexMetadata.selectCloneShard(shardRouting.id(), sourceIndexMetadata, indexMetadata.getNumberOfShards()) :
                IndexMetadata.selectSplitShard(shardRouting.id(), sourceIndexMetadata, indexMetadata.getNumberOfShards());
            ShardRouting sourceShardRouting = allocation.routingNodes().activePrimary(shardId);
            if (sourceShardRouting == null) {
                return allocation.decision(Decision.NO, NAME, "source primary shard [%s] is not active", shardId);
            }
            if (node != null) { // we might get called from the 2 param canAllocate method..
                if (sourceShardRouting.currentNodeId().equals(node.nodeId())) {
                    return allocation.decision(Decision.YES, NAME, "source primary is allocated on this node");
                } else {
                    return allocation.decision(Decision.NO, NAME, "source primary is allocated on another node");
                }
            } else {
                return allocation.decision(Decision.YES, NAME, "source primary is active");
            }
        }
        return super.canAllocate(shardRouting, node, allocation);
    }

    @Override
    public Decision canForceAllocatePrimary(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        assert shardRouting.primary() : "must not call canForceAllocatePrimary on a non-primary shard " + shardRouting;
        return canAllocate(shardRouting, node, allocation);
    }
}
