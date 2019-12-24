/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetaData;
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
            final IndexMetaData indexMetaData = allocation.metaData().getIndexSafe(shardRouting.index());
            Index resizeSourceIndex = indexMetaData.getResizeSourceIndex();
            assert resizeSourceIndex != null;
            if (allocation.metaData().index(resizeSourceIndex) == null) {
                return allocation.decision(Decision.NO, NAME, "resize source index [%s] doesn't exists", resizeSourceIndex.toString());
            }
            IndexMetaData sourceIndexMetaData = allocation.metaData().getIndexSafe(resizeSourceIndex);
            if (indexMetaData.getNumberOfShards() < sourceIndexMetaData.getNumberOfShards()) {
                // this only handles splits and clone so far.
                return Decision.ALWAYS;
            }

            ShardId shardId = indexMetaData.getNumberOfShards() == sourceIndexMetaData.getNumberOfShards() ?
                IndexMetaData.selectCloneShard(shardRouting.id(), sourceIndexMetaData, indexMetaData.getNumberOfShards()) :
                IndexMetaData.selectSplitShard(shardRouting.id(), sourceIndexMetaData, indexMetaData.getNumberOfShards());
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
