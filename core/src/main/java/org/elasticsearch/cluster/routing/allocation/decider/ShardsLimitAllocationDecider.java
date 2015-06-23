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
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

/**
 * This {@link AllocationDecider} limits the number of shards per node on a per
 * index basis. The allocator prevents a single node to hold more than
 * {@value #INDEX_TOTAL_SHARDS_PER_NODE} per index during the allocation
 * process. The limits of this decider can be changed in real-time via a the
 * index settings API.
 * <p>
 * If {@value #INDEX_TOTAL_SHARDS_PER_NODE} is reset to a negative value shards
 * per index are unlimited per node. Shards currently in the
 * {@link ShardRoutingState#RELOCATING relocating} state are ignored by this
 * {@link AllocationDecider} until the shard changed its state to either
 * {@link ShardRoutingState#STARTED started},
 * {@link ShardRoutingState#INITIALIZING inializing} or
 * {@link ShardRoutingState#UNASSIGNED unassigned}
 * <p>
 * Note: Reducing the number of shards per node via the index update API can
 * trigger relocation and significant additional load on the clusters nodes.
 * </p>
 */
public class ShardsLimitAllocationDecider extends AllocationDecider {

    public static final String NAME = "shards_limit";

    /**
     * Controls the maximum number of shards per index on a single Elasticsearch
     * node. Negative values are interpreted as unlimited.
     */
    public static final String INDEX_TOTAL_SHARDS_PER_NODE = "index.routing.allocation.total_shards_per_node";

    @Inject
    public ShardsLimitAllocationDecider(Settings settings) {
        super(settings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetaData indexMd = allocation.routingNodes().metaData().index(shardRouting.index());
        int totalShardsPerNode = indexMd.settings().getAsInt(INDEX_TOTAL_SHARDS_PER_NODE, -1);
        if (totalShardsPerNode <= 0) {
            return allocation.decision(Decision.YES, NAME, "total shard limit disabled: [%d] <= 0", totalShardsPerNode);
        }

        int nodeCount = 0;
        for (ShardRouting nodeShard : node) {
            if (!nodeShard.index().equals(shardRouting.index())) {
                continue;
            }
            // don't count relocating shards...
            if (nodeShard.relocating()) {
                continue;
            }
            nodeCount++;
        }
        if (nodeCount >= totalShardsPerNode) {
            return allocation.decision(Decision.NO, NAME, "too many shards for this index on node [%d], limit: [%d]",
                    nodeCount, totalShardsPerNode);
        }
        return allocation.decision(Decision.YES, NAME, "shard count under limit [%d] of total shards per node", totalShardsPerNode);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetaData indexMd = allocation.routingNodes().metaData().index(shardRouting.index());
        int totalShardsPerNode = indexMd.settings().getAsInt(INDEX_TOTAL_SHARDS_PER_NODE, -1);
        if (totalShardsPerNode <= 0) {
            return allocation.decision(Decision.YES, NAME, "total shard limit disabled: [%d] <= 0", totalShardsPerNode);
        }

        int nodeCount = 0;
        for (ShardRouting nodeShard : node) {
            if (!nodeShard.index().equals(shardRouting.index())) {
                continue;
            }
            // don't count relocating shards...
            if (nodeShard.relocating()) {
                continue;
            }
            nodeCount++;
        }
        if (nodeCount > totalShardsPerNode) {
            return allocation.decision(Decision.NO, NAME, "too many shards for this index on node [%d], limit: [%d]",
                    nodeCount, totalShardsPerNode);
        }
        return allocation.decision(Decision.YES, NAME, "shard count under limit [%d] of total shards per node", totalShardsPerNode);
    }
}
