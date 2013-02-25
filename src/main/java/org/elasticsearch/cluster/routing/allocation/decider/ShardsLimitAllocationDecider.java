/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
import org.elasticsearch.cluster.routing.MutableShardRouting;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

/**
 *
 */
public class ShardsLimitAllocationDecider extends AllocationDecider {

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
            return Decision.YES;
        }

        int nodeCount = 0;
        List<MutableShardRouting> shards = node.shards();
        for (int i = 0; i < shards.size(); i++) {
            MutableShardRouting nodeShard = shards.get(i);
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
            return Decision.NO;
        }
        return Decision.YES;
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetaData indexMd = allocation.routingNodes().metaData().index(shardRouting.index());
        int totalShardsPerNode = indexMd.settings().getAsInt(INDEX_TOTAL_SHARDS_PER_NODE, -1);
        if (totalShardsPerNode <= 0) {
            return Decision.YES;
        }

        int nodeCount = 0;
        List<MutableShardRouting> shards = node.shards();
        for (int i = 0; i < shards.size(); i++) {
            MutableShardRouting nodeShard = shards.get(i);
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
            return Decision.NO;
        }
        return Decision.YES;
    }
}
