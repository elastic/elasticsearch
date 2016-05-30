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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;

/**
 * This {@link AllocationDecider} limits the number of shards per node on a per
 * index or node-wide basis. The allocator prevents a single node to hold more
 * than <tt>index.routing.allocation.total_shards_per_node</tt> per index and
 * <tt>cluster.routing.allocation.total_shards_per_node</tt> globally during the allocation
 * process. The limits of this decider can be changed in real-time via a the
 * index settings API.
 * <p>
 * If <tt>index.routing.allocation.total_shards_per_node</tt> is reset to a negative value shards
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

    private volatile int clusterShardLimit;

    /**
     * Controls the maximum number of shards per index on a single Elasticsearch
     * node. Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> INDEX_TOTAL_SHARDS_PER_NODE_SETTING =
        Setting.intSetting("index.routing.allocation.total_shards_per_node", -1, -1,
            Property.Dynamic, Property.IndexScope);

    /**
     * Controls the maximum number of shards per node on a global level.
     * Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING =
        Setting.intSetting("cluster.routing.allocation.total_shards_per_node", -1,  -1,
            Property.Dynamic, Property.NodeScope);


    @Inject
    public ShardsLimitAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        super(settings);
        this.clusterShardLimit = CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING, this::setClusterShardLimit);
    }

    private void setClusterShardLimit(int clusterShardLimit) {
        this.clusterShardLimit = clusterShardLimit;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetaData indexMd = allocation.metaData().getIndexSafe(shardRouting.index());
        final int indexShardLimit = INDEX_TOTAL_SHARDS_PER_NODE_SETTING.get(indexMd.getSettings(), settings);
        // Capture the limit here in case it changes during this method's
        // execution
        final int clusterShardLimit = this.clusterShardLimit;

        if (indexShardLimit <= 0 && clusterShardLimit <= 0) {
            return allocation.decision(Decision.YES, NAME, "total shard limits are disabled: [index: %d, cluster: %d] <= 0",
                    indexShardLimit, clusterShardLimit);
        }

        int indexShardCount = 0;
        int nodeShardCount = 0;
        for (ShardRouting nodeShard : node) {
            // don't count relocating shards...
            if (nodeShard.relocating()) {
                continue;
            }
            nodeShardCount++;
            if (nodeShard.index().equals(shardRouting.index())) {
                indexShardCount++;
            }
        }
        if (clusterShardLimit > 0 && nodeShardCount >= clusterShardLimit) {
            return allocation.decision(Decision.NO, NAME, "too many shards for this node [%d], cluster-level limit per node: [%d]",
                    nodeShardCount, clusterShardLimit);
        }
        if (indexShardLimit > 0 && indexShardCount >= indexShardLimit) {
            return allocation.decision(Decision.NO, NAME,
                    "too many shards for this index [%s] on node [%d], index-level limit per node: [%d]",
                    shardRouting.index(), indexShardCount, indexShardLimit);
        }
        return allocation.decision(Decision.YES, NAME,
                "the shard count is under index limit [%d] and cluster level node limit [%d] of total shards per node",
                indexShardLimit, clusterShardLimit);
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        IndexMetaData indexMd = allocation.metaData().getIndexSafe(shardRouting.index());
        final int indexShardLimit = INDEX_TOTAL_SHARDS_PER_NODE_SETTING.get(indexMd.getSettings(), settings);
        // Capture the limit here in case it changes during this method's
        // execution
        final int clusterShardLimit = this.clusterShardLimit;

        if (indexShardLimit <= 0 && clusterShardLimit <= 0) {
            return allocation.decision(Decision.YES, NAME, "total shard limits are disabled: [index: %d, cluster: %d] <= 0",
                    indexShardLimit, clusterShardLimit);
        }

        int indexShardCount = 0;
        int nodeShardCount = 0;
        for (ShardRouting nodeShard : node) {
            // don't count relocating shards...
            if (nodeShard.relocating()) {
                continue;
            }
            nodeShardCount++;
            if (nodeShard.index().equals(shardRouting.index())) {
                indexShardCount++;
            }
        }
        // Subtle difference between the `canAllocate` and `canRemain` is that
        // this checks > while canAllocate checks >=
        if (clusterShardLimit > 0 && nodeShardCount > clusterShardLimit) {
            return allocation.decision(Decision.NO, NAME, "too many shards for this node [%d], cluster-level limit per node: [%d]",
                    nodeShardCount, clusterShardLimit);
        }
        if (indexShardLimit > 0 && indexShardCount > indexShardLimit) {
            return allocation.decision(Decision.NO, NAME,
                    "too many shards for this index [%s] on node [%d], index-level limit per node: [%d]",
                    shardRouting.index(), indexShardCount, indexShardLimit);
        }
        return allocation.decision(Decision.YES, NAME,
                "the shard count is under index limit [%d] and cluster level node limit [%d] of total shards per node",
                indexShardLimit, clusterShardLimit);
    }

    @Override
    public Decision canAllocate(RoutingNode node, RoutingAllocation allocation) {
        // Only checks the node-level limit, not the index-level
        // Capture the limit here in case it changes during this method's
        // execution
        final int clusterShardLimit = this.clusterShardLimit;

        if (clusterShardLimit <= 0) {
            return allocation.decision(Decision.YES, NAME, "total shard limits are disabled: [cluster: %d] <= 0",
                    clusterShardLimit);
        }

        int nodeShardCount = 0;
        for (ShardRouting nodeShard : node) {
            // don't count relocating shards...
            if (nodeShard.relocating()) {
                continue;
            }
            nodeShardCount++;
        }
        if (clusterShardLimit >= 0 && nodeShardCount >= clusterShardLimit) {
            return allocation.decision(Decision.NO, NAME, "too many shards for this node [%d], cluster-level limit per node: [%d]",
                    nodeShardCount, clusterShardLimit);
        }
        return allocation.decision(Decision.YES, NAME, "the shard count is under node limit [%d] of total shards per node",
                clusterShardLimit);
    }
}
