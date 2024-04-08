/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;

import java.util.function.BiPredicate;

/**
 * This {@link AllocationDecider} limits the number of shards per node on a per
 * index or node-wide basis. The allocator prevents a single node to hold more
 * than {@code index.routing.allocation.total_shards_per_node} per index and
 * {@code cluster.routing.allocation.total_shards_per_node} globally during the allocation
 * process. The limits of this decider can be changed in real-time via a the
 * index settings API.
 * <p>
 * If {@code index.routing.allocation.total_shards_per_node} is reset to a negative value shards
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
    public static final Setting<Integer> INDEX_TOTAL_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "index.routing.allocation.total_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.IndexScope
    );

    /**
     * Controls the maximum number of shards per node on a global level.
     * Negative values are interpreted as unlimited.
     */
    public static final Setting<Integer> CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING = Setting.intSetting(
        "cluster.routing.allocation.total_shards_per_node",
        -1,
        -1,
        Property.Dynamic,
        Property.NodeScope
    );

    public ShardsLimitAllocationDecider(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING, this::setClusterShardLimit);
    }

    private void setClusterShardLimit(int clusterShardLimit) {
        this.clusterShardLimit = clusterShardLimit;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(
            allocation.metadata().getIndexSafe(shardRouting.index()),
            shardRouting,
            node,
            allocation,
            (count, limit) -> count >= limit
        );
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(indexMetadata, shardRouting, node, allocation, (count, limit) -> count > limit);

    }

    private Decision doDecide(
        IndexMetadata indexMd,
        ShardRouting shardRouting,
        RoutingNode node,
        RoutingAllocation allocation,
        BiPredicate<Integer, Integer> decider
    ) {
        final int indexShardLimit = indexMd.getShardsPerNodeLimit();
        // Capture the limit here in case it changes during this method's
        // execution
        final int clusterShardLimit = this.clusterShardLimit;

        if (indexShardLimit <= 0 && clusterShardLimit <= 0) {
            return allocation.decision(
                Decision.YES,
                NAME,
                "total shard limits are disabled: [index: %d, cluster: %d] <= 0",
                indexShardLimit,
                clusterShardLimit
            );
        }

        final int nodeShardCount = node.numberOfOwningShards();

        if (clusterShardLimit > 0 && decider.test(nodeShardCount, clusterShardLimit)) {
            return allocation.decision(
                Decision.NO,
                NAME,
                "too many shards [%d] allocated to this node, cluster setting [%s=%d]",
                nodeShardCount,
                CLUSTER_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                clusterShardLimit
            );
        }
        if (indexShardLimit > 0) {
            final int indexShardCount = node.numberOfOwningShardsForIndex(shardRouting.index());
            if (decider.test(indexShardCount, indexShardLimit)) {
                return allocation.decision(
                    Decision.NO,
                    NAME,
                    "too many shards [%d] allocated to this node for index [%s], index setting [%s=%d]",
                    indexShardCount,
                    shardRouting.getIndexName(),
                    INDEX_TOTAL_SHARDS_PER_NODE_SETTING.getKey(),
                    indexShardLimit
                );
            }
        }
        return allocation.decision(
            Decision.YES,
            NAME,
            "the shard count [%d] for this node is under the index limit [%d] and cluster level node limit [%d]",
            nodeShardCount,
            indexShardLimit,
            clusterShardLimit
        );
    }

}
