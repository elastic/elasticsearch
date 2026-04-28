/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher;

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Map;

/**
 * Encapsulates the routing decision used to assign a watch to exactly one shard copy. Each instance describes a
 * single local shard: the bucket of the local allocation among all active allocation ids for that shard, the total
 * number of those allocations, and the sorted allocation ids themselves.
 *
 * Given a watch id, {@link #shouldBeTriggered} returns whether the local node is the owner of the watch according to
 * the consistent-hash mapping {@code Murmur3(id) mod shardCount == index}. Used by both
 * {@link WatcherIndexingListener#postIndex} and {@link WatcherService} to keep the routing decision in one place.
 */
final class ShardAllocationConfiguration {

    final int index;
    final int shardCount;
    final List<String> allocationIds;

    ShardAllocationConfiguration(int index, int shardCount, List<String> allocationIds) {
        this.index = index;
        this.shardCount = shardCount;
        this.allocationIds = allocationIds;
    }

    public boolean shouldBeTriggered(String id) {
        int hash = Murmur3HashFunction.hash(id);
        int shardIndex = Math.floorMod(hash, shardCount);
        return shardIndex == index;
    }

    /**
     * Returns a mapping of {@link ShardId} to {@link ShardAllocationConfiguration} for each shard copy hosted locally.
     * The configuration captures the bucket of the local allocation id among all active allocation ids for that shard
     * (sorted to keep the result stable across nodes), so that the consistent-hash decision in
     * {@link #shouldBeTriggered} can be made without further routing-table lookups.
     *
     * Example:
     * <ul>
     *   <li>ShardId(".watches", 0)</li>
     *   <li>all sorted allocation ids in the cluster: {@code [a, b, c, d]}</li>
     *   <li>local allocation id: {@code b} (index position 1)</li>
     *   <li>stored as {@code ShardAllocationConfiguration(1, 4, [a, b, c, d])}</li>
     * </ul>
     */
    static Map<ShardId, ShardAllocationConfiguration> forLocalShards(List<ShardRouting> localShards, IndexRoutingTable routingTable) {
        Map<ShardId, ShardAllocationConfiguration> data = Maps.newMapWithExpectedSize(localShards.size());
        for (ShardRouting shardRouting : localShards) {
            ShardId shardId = shardRouting.shardId();
            List<String> allocationIds = routingTable.shard(shardId.getId())
                .activeShards()
                .stream()
                .map(ShardRouting::allocationId)
                .map(AllocationId::getId)
                .sorted()
                .toList();
            int idx = allocationIds.indexOf(shardRouting.allocationId().getId());
            data.put(shardId, new ShardAllocationConfiguration(idx, allocationIds.size(), allocationIds));
        }
        return data;
    }
}
