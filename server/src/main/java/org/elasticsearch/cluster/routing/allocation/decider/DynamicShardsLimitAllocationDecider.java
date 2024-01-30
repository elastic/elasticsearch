/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;

import java.util.function.BiPredicate;

/**
 * This {@link AllocationDecider} limits the number of shards per node on a per
 * index or node-wide basis. The allocator prevents a single node to hold more
 * than {@code totalShard/totalEffectiveDataNode + index.routing.allocation.dynamic.shards.limit.redundance} per index and
 * {@code totalShard/totalEffectiveDataNode + cluster.routing.allocation.dynamic.shards.limit.redundance} globally during the allocation
 * process. The limits of this decider can be changed in real-time via a the
 * index settings API.
 * <p>
 * If {@code cluster.routing.allocation.dynamic.shards.limit.redundance and  index.routing.allocation.dynamic.shards.limit.redundance} is reset to a negative value shards
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
public class DynamicShardsLimitAllocationDecider  extends AllocationDecider {
    private static final Logger logger = LogManager.getLogger(DynamicShardsLimitAllocationDecider.class);
    public static final String NAME = "dynamic_shards_limit";

    public static final Setting<Integer> DYNAMIC_SHARDS_LIMIT_REDUNDANCE = Setting.intSetting(
        "cluster.routing.allocation.dynamic.shards.limit.redundance",
        0,
        -1,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> DYNAMIC_INDEX_SHARDS_LIMIT_REDUNDANCE = Setting.intSetting(
        "index.routing.allocation.dynamic.shards.limit.redundance",
        -1,
        -1,
        Setting.Property.Dynamic,
        Setting.Property.IndexScope
    );

    private final Settings settings;
    private volatile int  dynamicShardsLimitRedundance;
    public DynamicShardsLimitAllocationDecider(Settings settings, ClusterSettings clusterSettings) {
        this.settings = settings;
        this.dynamicShardsLimitRedundance = DYNAMIC_SHARDS_LIMIT_REDUNDANCE.get(settings);
        clusterSettings.addSettingsUpdateConsumer(DYNAMIC_SHARDS_LIMIT_REDUNDANCE, this::setDynamicShardsLimitRedundance);
    }
    public boolean isDynamicShardsLimitEnable() {
        return dynamicShardsLimitRedundance >= 0;
    }

    public void setDynamicShardsLimitRedundance(int dynamicShardsLimitRedundance) {
        this.dynamicShardsLimitRedundance = dynamicShardsLimitRedundance;
    }

    public int getDynamicShardsLimitRedundance(IndexMetadata index) {
        int shardLimit = getDynamicIndexShardLimitRedundance(index);
        if (shardLimit >= 0) return shardLimit;
        return dynamicShardsLimitRedundance;
    }

    public boolean isDynamicIndexShardLimitEnable(IndexMetadata indexMd) {
        return getDynamicIndexShardLimitRedundance(indexMd) >= 0;
    }

    public int getDynamicIndexShardLimitRedundance(IndexMetadata indexMd) {
        return DYNAMIC_INDEX_SHARDS_LIMIT_REDUNDANCE.get(indexMd.getSettings(), settings);
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(allocation.metadata().getIndexSafe(shardRouting.index()),
            shardRouting, node, allocation, (count, limit) -> count >= limit);
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return doDecide(indexMetadata, shardRouting, node, allocation, (count, limit) -> count > limit);
    }

    private Decision doDecide(IndexMetadata indexMetaData, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation,
                              BiPredicate<Integer, Integer> decider) {

        if (isDynamicIndexShardLimitEnable(indexMetaData) == false && isDynamicShardsLimitEnable() == false) {
            return allocation.decision(Decision.YES, NAME,
                "index dynamic_shards_limit decider enable false");
        }

        if (node.node() != null && canAllocateByOtherIndexDecider(indexMetaData, shardRouting, allocation, node)) {
            final int dynamicIndexShardLimit = getDynamicIndexShardLimit(indexMetaData, shardRouting, node, allocation);
            final int owningShardsForIndex = node.numberOfOwningShardsForIndex(indexMetaData.getIndex());
            if (decider.test(owningShardsForIndex, dynamicIndexShardLimit)) {
                if (shardRouting != null) {
                    return allocation.decision(Decision.NO, NAME,
                        "too many shards [%d] allocated to this node[%s] for index [%s] shardId[%d], indexShardLimit is %d",
                        owningShardsForIndex, node.nodeId(), indexMetaData.getIndex().toString(), shardRouting.shardId().getId(), dynamicIndexShardLimit);
                }
                return allocation.decision(Decision.NO, NAME,
                    "too many shards [%d] allocated to this node for index [%s], indexShardLimit is %d",
                    owningShardsForIndex, indexMetaData.getIndex().getName(), dynamicIndexShardLimit);
            }
            return allocation.decision(Decision.YES, NAME,
                "the shard for this node is under the index limit [%d] ", dynamicIndexShardLimit);
        }
        return allocation.decision(Decision.YES, NAME, "paas the shard decider");
    }

    private int getDynamicIndexShardLimit(IndexMetadata indexMetaData, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        double totalEffectiveNode = getTotalEffectiveNode(indexMetaData, shardRouting, node, allocation);
        int  totalShardPerNode = (int) Math.ceil(getTotalShard(indexMetaData) / totalEffectiveNode);
        final int dynamicIndexShardLimit = totalShardPerNode  + getDynamicShardsLimitRedundance(indexMetaData);
        return dynamicIndexShardLimit;
    }

    private boolean canAllocateByOtherIndexDecider(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingAllocation allocation, RoutingNode checkNode) {
        AllocationDecider[] allocations = allocation.deciders().getDeciders();
        for (AllocationDecider allocationDecider : allocations) {
            if ((allocationDecider instanceof  FilterAllocationDecider) == false && (allocationDecider instanceof DiskThresholdDecider) == false) continue;
            Decision decision = allocationDecider.canAllocate(indexMetadata, checkNode, allocation);
            if (decision.type() == Decision.Type.YES) {
                decision = allocationDecider.canAllocate(shardRouting, checkNode, allocation);
            }
            if (decision.type() == Decision.Type.NO) {
                if (logger.isTraceEnabled()) {
                    logger.trace(
                        "can not allocate [{}] on node [{}] due to [{}]",
                        indexMetadata,
                        checkNode.node(),
                        allocationDecider.getClass().getSimpleName()
                    );
                }
                return Boolean.FALSE;
            }
            return Boolean.TRUE;
        }
        return Boolean.TRUE;
    }

    private int getTotalEffectiveNode(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        int totalNode = 1;
        for (RoutingNode checkNode : allocation.routingNodes()) {
            if (isNullOrTheSameNode(node, checkNode) || containDataRole(checkNode) == false ) continue;
            if (canAllocateByOtherIndexDecider(indexMetadata, shardRouting, allocation, checkNode)) {
                totalNode ++ ;
            }
        }
        return totalNode;
    }

    private boolean containDataRole(RoutingNode checkNode) {
        for (DiscoveryNodeRole discoveryNodeRole : checkNode.node().getRoles()){
            if (discoveryNodeRole.canContainData()) {
                return  true;
            }
        }
        return false;
    }

    private int getTotalShard(IndexMetadata indexMetadata) {
        return indexMetadata.getNumberOfShards() * (1 + indexMetadata.getNumberOfReplicas());
    }

    private boolean isNullOrTheSameNode(RoutingNode node, RoutingNode checkNode) {
        return  (checkNode.node() == null || node.nodeId().equals(checkNode.nodeId()));
    }
}
