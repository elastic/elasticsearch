/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation.decider;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheInfoService;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;

public class HasFrozenCacheAllocationDecider extends AllocationDecider {

    private static final String NAME = "has_frozen_cache";

    private static final Decision STILL_FETCHING = Decision.single(
        Decision.Type.THROTTLE,
        NAME,
        "value of [" + SHARED_CACHE_SIZE_SETTING.getKey() + "] on this node is not known yet"
    );

    private static final Decision HAS_FROZEN_CACHE = Decision.single(
        Decision.Type.YES,
        NAME,
        "this node has a searchable snapshot shared cache"
    );

    private static final Decision NO_FROZEN_CACHE = Decision.single(
        Decision.Type.NO,
        NAME,
        "node setting ["
            + SHARED_CACHE_SIZE_SETTING.getKey()
            + "] is set to zero, so shards of partially mounted indices cannot be allocated to this node"
    );

    private static final Decision UNKNOWN_FROZEN_CACHE = Decision.single(
        Decision.Type.NO,
        NAME,
        "there was an error fetching the searchable snapshot shared cache state from this node"
    );

    private final FrozenCacheInfoService frozenCacheService;

    public HasFrozenCacheAllocationDecider(FrozenCacheInfoService frozenCacheService) {
        this.frozenCacheService = frozenCacheService;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(allocation.metadata().indexMetadata(shardRouting.index()), shardRouting, node.node());
    }

    @Override
    public Decision canRemain(IndexMetadata indexMetadata, ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(indexMetadata, shardRouting, node.node());
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(indexMetadata, null, node.node());
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return canAllocateToNode(indexMetadata, null, node);
    }

    private Decision canAllocateToNode(IndexMetadata indexMetadata, @Nullable ShardRouting shardRouting, DiscoveryNode discoveryNode) {
        if (indexMetadata.isPartialSearchableSnapshot() == false) {
            return Decision.ALWAYS;
        }

        return switch (frozenCacheService.getNodeState(discoveryNode)) {
            case HAS_CACHE -> HAS_FROZEN_CACHE;
            case NO_CACHE -> NO_FROZEN_CACHE;
            case FAILED -> UNKNOWN_FROZEN_CACHE;
            // THROTTLE is an odd choice here. In all other places, it means YES but not right now. But here it can eventually
            // turn into a NO if the node responds that it has no frozen cache. Since BalancedShardAllocator effectively handles
            // THROTTLE as a rejection, it is desirable that we simply return a NO here for shard level decisions. For BWC,
            // we keep the THROTTLE decision for unassigned shards.
            // For index level decisions (shardRouting == null), we still need THROTTLE so that the node is not excluded from
            // the candidates too early.
            default -> shardRouting != null && shardRouting.unassigned() ? STILL_FETCHING : Decision.NO;
        };
    }

}
