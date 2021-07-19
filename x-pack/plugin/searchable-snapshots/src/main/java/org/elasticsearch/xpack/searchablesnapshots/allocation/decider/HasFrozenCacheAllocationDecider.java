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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheInfoService;

import static org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService.SNAPSHOT_CACHE_SIZE_SETTING;

public class HasFrozenCacheAllocationDecider extends AllocationDecider {

    private static final String NAME = "has_frozen_cache";

    private static final Decision STILL_FETCHING = Decision.single(
        Decision.Type.THROTTLE,
        NAME,
        "value of [" + SNAPSHOT_CACHE_SIZE_SETTING.getKey() + "] on this node is not known yet"
    );

    private static final Decision HAS_FROZEN_CACHE = Decision.single(
        Decision.Type.YES,
        NAME,
        "this node has a frozen searchable snapshot shard cache"
    );

    private static final Decision NO_FROZEN_CACHE = Decision.single(
        Decision.Type.NO,
        NAME,
        "node setting ["
            + SNAPSHOT_CACHE_SIZE_SETTING.getKey()
            + "] is set to zero, so frozen searchable snapshot shards cannot be allocated to this node"
    );

    private static final Decision UNKNOWN_FROZEN_CACHE = Decision.single(
        Decision.Type.NO,
        NAME,
        "there was an error fetching the frozen cache state from this node"
    );

    private final FrozenCacheInfoService frozenCacheService;

    public HasFrozenCacheAllocationDecider(FrozenCacheInfoService frozenCacheService) {
        this.frozenCacheService = frozenCacheService;
    }

    @Override
    public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(allocation.metadata().getIndexSafe(shardRouting.index()), node.node());
    }

    @Override
    public Decision canRemain(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(allocation.metadata().getIndexSafe(shardRouting.index()), node.node());
    }

    @Override
    public Decision canAllocate(IndexMetadata indexMetadata, RoutingNode node, RoutingAllocation allocation) {
        return canAllocateToNode(indexMetadata, node.node());
    }

    @Override
    public Decision shouldAutoExpandToNode(IndexMetadata indexMetadata, DiscoveryNode node, RoutingAllocation allocation) {
        return canAllocateToNode(indexMetadata, node);
    }

    private Decision canAllocateToNode(IndexMetadata indexMetadata, DiscoveryNode discoveryNode) {
        final Settings indexSettings = indexMetadata.getSettings();

        if (SearchableSnapshotsSettings.isPartialSearchableSnapshotIndex(indexSettings) == false) {
            return Decision.ALWAYS;
        }

        switch (frozenCacheService.getNodeState(discoveryNode)) {
            case HAS_CACHE:
                return HAS_FROZEN_CACHE;
            case NO_CACHE:
                return NO_FROZEN_CACHE;
            case FAILED:
                return UNKNOWN_FROZEN_CACHE;
            default:
                return STILL_FETCHING;
        }
    }

}
