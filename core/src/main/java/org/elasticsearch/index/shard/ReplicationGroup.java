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

import java.util.Set;

/**
 * Replication group for a shard. Used by a primary shard to coordinate replication and recoveries.
 */
public class ReplicationGroup {
    private final IndexShardRoutingTable routingTable;
    private final Set<String> inSyncAllocationIds;

    public ReplicationGroup(IndexShardRoutingTable routingTable, Set<String> inSyncAllocationIds) {
        this.routingTable = routingTable;
        this.inSyncAllocationIds = inSyncAllocationIds;
    }

    public IndexShardRoutingTable getRoutingTable() {
        return routingTable;
    }

    public Set<String> getInSyncAllocationIds() {
        return inSyncAllocationIds;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ReplicationGroup that = (ReplicationGroup) o;

        if (!routingTable.equals(that.routingTable)) return false;
        return inSyncAllocationIds.equals(that.inSyncAllocationIds);
    }

    @Override
    public int hashCode() {
        int result = routingTable.hashCode();
        result = 31 * result + inSyncAllocationIds.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "ReplicationGroup{" +
            "routingTable=" + routingTable +
            ", inSyncAllocationIds=" + inSyncAllocationIds +
            '}';
    }

}
