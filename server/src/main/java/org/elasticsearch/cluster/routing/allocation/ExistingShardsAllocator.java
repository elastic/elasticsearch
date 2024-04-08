/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingChangesObserver;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.gateway.GatewayAllocator;

import java.util.List;
import java.util.function.Predicate;

/**
 * Searches for, and allocates, shards for which there is an existing on-disk copy somewhere in the cluster. The default implementation is
 * {@link GatewayAllocator}, but plugins can supply their own implementations too.
 */
public interface ExistingShardsAllocator {

    /**
     * Allows plugins to override how we allocate shards that may already exist on disk in the cluster.
     */
    Setting<String> EXISTING_SHARDS_ALLOCATOR_SETTING = Setting.simpleString(
        "index.allocation.existing_shards_allocator",
        GatewayAllocator.ALLOCATOR_NAME,
        Setting.Property.IndexScope,
        Setting.Property.PrivateIndex
    );

    /**
     * Called before starting a round of allocation, allowing the allocator to invalidate some caches if appropriate.
     */
    void beforeAllocation(RoutingAllocation allocation);

    /**
     * Called during a round of allocation after attempting to allocate all the primaries but before any replicas, allowing the allocator
     * to prepare for replica allocation.
     */
    @Deprecated(forRemoval = true)
    default void afterPrimariesBeforeReplicas(@SuppressWarnings("unused") RoutingAllocation allocation) {
        assert false : "must be overridden";
        throw new UnsupportedOperationException();
    }

    default void afterPrimariesBeforeReplicas(RoutingAllocation allocation, Predicate<ShardRouting> isRelevantShardPredicate) {
        afterPrimariesBeforeReplicas(allocation);
    }

    /**
     * Allocate any unassigned shards in the given {@link RoutingAllocation} for which this {@link ExistingShardsAllocator} is responsible.
     */
    void allocateUnassigned(
        ShardRouting shardRouting,
        RoutingAllocation allocation,
        UnassignedAllocationHandler unassignedAllocationHandler
    );

    /**
     * Returns an explanation for a single unassigned shard.
     */
    AllocateUnassignedDecision explainUnassignedShardAllocation(ShardRouting unassignedShard, RoutingAllocation routingAllocation);

    /**
     * Called when this node becomes the elected master and when it stops being the elected master, so that implementations can clean up any
     * in-flight activity from an earlier mastership.
     */
    void cleanCaches();

    /**
     * Called when the given shards have started, so that implementations can invalidate caches and clean up any in-flight activity for
     * those shards.
     */
    void applyStartedShards(List<ShardRouting> startedShards, RoutingAllocation allocation);

    /**
     * Called when the given shards have failed, so that implementations can invalidate caches and clean up any in-flight activity for
     * those shards.
     */
    void applyFailedShards(List<FailedShard> failedShards, RoutingAllocation allocation);

    /**
     * @return the number of in-flight fetches under this allocator's control.
     */
    int getNumberOfInFlightFetches();

    /**
     * Used by {@link ExistingShardsAllocator#allocateUnassigned} to handle its allocation decisions. A restricted interface to
     * {@link RoutingNodes.UnassignedShards.UnassignedIterator} to limit what allocators can do.
     */
    interface UnassignedAllocationHandler {

        /**
         * Initializes the current unassigned shard and moves it from the unassigned list.
         *
         * @param existingAllocationId allocation id to use. If null, a fresh allocation id is generated.
         */
        ShardRouting initialize(
            String nodeId,
            @Nullable String existingAllocationId,
            long expectedShardSize,
            RoutingChangesObserver routingChangesObserver
        );

        /**
         * Removes and ignores the unassigned shard (will be ignored for this run, but
         * will be added back to unassigned once the metadata is constructed again).
         * Typically this is used when an allocation decision prevents a shard from being allocated such
         * that subsequent consumers of this API won't try to allocate this shard again.
         *
         * @param attempt the result of the allocation attempt
         */
        void removeAndIgnore(UnassignedInfo.AllocationStatus attempt, RoutingChangesObserver changes);

        /**
         * updates the unassigned info and recovery source on the current unassigned shard
         *
         * @param  unassignedInfo the new unassigned info to use
         * @param  recoverySource the new recovery source to use
         * @return the shard with unassigned info updated
         */
        ShardRouting updateUnassigned(UnassignedInfo unassignedInfo, RecoverySource recoverySource, RoutingChangesObserver changes);
    }
}
