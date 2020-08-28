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

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Replication group for a shard. Used by a primary shard to coordinate replication and recoveries.
 */
public class ReplicationGroup {
    private final IndexShardRoutingTable routingTable;
    private final Set<String> inSyncAllocationIds;
    private final Set<String> trackedAllocationIds;
    private final long version;

    private final Set<String> unavailableInSyncShards; // derived from the other fields
    private final List<ShardRouting> replicationTargets; // derived from the other fields
    private final List<ShardRouting> skippedShards; // derived from the other fields

    public ReplicationGroup(IndexShardRoutingTable routingTable, Set<String> inSyncAllocationIds, Set<String> trackedAllocationIds,
                            long version) {
        this.routingTable = routingTable;
        this.inSyncAllocationIds = inSyncAllocationIds;
        this.trackedAllocationIds = trackedAllocationIds;
        this.version = version;

        this.unavailableInSyncShards = Sets.difference(inSyncAllocationIds, routingTable.getAllAllocationIds());
        this.replicationTargets = new ArrayList<>();
        this.skippedShards = new ArrayList<>();
        for (final ShardRouting shard : routingTable) {
            if (shard.unassigned()) {
                assert shard.primary() == false : "primary shard should not be unassigned in a replication group: " + shard;
                skippedShards.add(shard);
            } else {
                if (trackedAllocationIds.contains(shard.allocationId().getId())) {
                    replicationTargets.add(shard);
                } else {
                    assert inSyncAllocationIds.contains(shard.allocationId().getId()) == false :
                        "in-sync shard copy but not tracked: " + shard;
                    skippedShards.add(shard);
                }
                if (shard.relocating()) {
                    ShardRouting relocationTarget = shard.getTargetRelocatingShard();
                    if (trackedAllocationIds.contains(relocationTarget.allocationId().getId())) {
                        replicationTargets.add(relocationTarget);
                    } else {
                        skippedShards.add(relocationTarget);
                        assert inSyncAllocationIds.contains(relocationTarget.allocationId().getId()) == false :
                            "in-sync shard copy but not tracked: " + shard;
                    }
                }
            }
        }
    }

    public long getVersion() {
        return version;
    }

    public IndexShardRoutingTable getRoutingTable() {
        return routingTable;
    }

    public Set<String> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    public Set<String> getTrackedAllocationIds() {
        return trackedAllocationIds;
    }

    /**
     * Returns the set of shard allocation ids that are in the in-sync set but have no assigned routing entry
     */
    public Set<String> getUnavailableInSyncShards() {
        return unavailableInSyncShards;
    }

    /**
     * Returns the subset of shards in the routing table that should be replicated to. Includes relocation targets.
     */
    public List<ShardRouting> getReplicationTargets() {
        return replicationTargets;
    }

    /**
     * Returns the subset of shards in the routing table that are unassigned or initializing and not ready yet to receive operations
     * (i.e. engine not opened yet). Includes relocation targets.
     */
    public List<ShardRouting> getSkippedShards() {
        return skippedShards;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationGroup that = (ReplicationGroup) o;

        if (!routingTable.equals(that.routingTable)) return false;
        if (!inSyncAllocationIds.equals(that.inSyncAllocationIds)) return false;
        return trackedAllocationIds.equals(that.trackedAllocationIds);
    }

    @Override
    public int hashCode() {
        int result = routingTable.hashCode();
        result = 31 * result + inSyncAllocationIds.hashCode();
        result = 31 * result + trackedAllocationIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ReplicationGroup{" +
            "routingTable=" + routingTable +
            ", inSyncAllocationIds=" + inSyncAllocationIds +
            ", trackedAllocationIds=" + trackedAllocationIds +
            '}';
    }

}
