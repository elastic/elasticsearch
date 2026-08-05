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
import org.elasticsearch.index.shard.ShardId;

import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;

/// Encapsulates the routing decision used to assign a watch to exactly one shard copy. Each instance describes a
/// single local shard: the bucket of the local allocation among all active allocation ids for that shard, the total
/// number of those allocations, and the sorted allocation ids themselves.
record ShardAllocationConfiguration(int index, int shardCount, List<String> allocationIds) {

    /// Given a watch id, [#hostsWatch] returns whether the local node is the owner of the watch according to
    /// the consistent-hash mapping `Murmur3(id) mod shardCount == index`.
    public boolean hostsWatch(String watchId) {
        final int hash = Murmur3HashFunction.hash(watchId);
        final int allocatedShardIndex = Math.floorMod(hash, shardCount);
        return allocatedShardIndex == index;
    }

    /// Returns a mapping of [ShardId] to [ShardAllocationConfiguration] for each shard copy hosted locally.
    /// The configuration captures the bucket of the local allocation id among all active allocation ids for that shard
    /// (sorted to keep the result stable across nodes), so that the consistent-hash decision in
    /// [#hostsWatch] can be made without further routing-table lookups.
    static Map<ShardId, ShardAllocationConfiguration> forLocalShards(List<ShardRouting> localShards, IndexRoutingTable routingTable) {
        return localShards.stream()
            .collect(toMap(ShardRouting::shardId, shardRouting -> shardAllocationConfiguration(routingTable, shardRouting)));
    }

    private static ShardAllocationConfiguration shardAllocationConfiguration(IndexRoutingTable routingTable, ShardRouting shardRouting) {
        final List<String> allocationIds = routingTable.shard(shardRouting.shardId().getId())
            .activeShards()
            .stream()
            .map(ShardRouting::allocationId)
            .map(AllocationId::getId)
            .sorted()
            .toList();
        final int idx = allocationIds.indexOf(shardRouting.allocationId().getId());
        return new ShardAllocationConfiguration(idx, allocationIds.size(), allocationIds);
    }

    /// Find the [ShardAllocationConfiguration] for the local shard that hosts a watch with the given id. Returns
    /// null if the local node does not have a copy of that watch's shard.
    /// visible for testing
    static ShardAllocationConfiguration findShardConfig(Map<ShardId, ShardAllocationConfiguration> shardConfigs, String id, int numShards) {
        final int shardIdNum = Math.floorMod(Murmur3HashFunction.hash(id), numShards);
        for (Map.Entry<ShardId, ShardAllocationConfiguration> entry : shardConfigs.entrySet()) {
            if (entry.getKey().getId() == shardIdNum) {
                return entry.getValue();
            }
        }
        return null;
    }
}
