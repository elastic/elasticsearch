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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;

/**
 *
 */
public class ClusterShardHealth implements Streamable {

    private int shardId;

    ClusterHealthStatus status = ClusterHealthStatus.RED;

    private int activeShards = 0;

    private int relocatingShards = 0;

    private int initializingShards = 0;

    private int unassignedShards = 0;

    private boolean primaryActive = false;

    private ClusterShardHealth() {

    }

    public ClusterShardHealth(int shardId, final IndexShardRoutingTable shardRoutingTable) {
        this.shardId = shardId;
        for (ShardRouting shardRouting : shardRoutingTable) {
            if (shardRouting.active()) {
                activeShards++;
                if (shardRouting.relocating()) {
                    // the shard is relocating, the one it is relocating to will be in initializing state, so we don't count it
                    relocatingShards++;
                }
                if (shardRouting.primary()) {
                    primaryActive = true;
                }
            } else if (shardRouting.initializing()) {
                initializingShards++;
            } else if (shardRouting.unassigned()) {
                unassignedShards++;
            }
        }
        if (primaryActive) {
            if (activeShards == shardRoutingTable.size()) {
                status = ClusterHealthStatus.GREEN;
            } else {
                status = ClusterHealthStatus.YELLOW;
            }
        } else {
            status = ClusterHealthStatus.RED;
        }
    }

    public int getId() {
        return shardId;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public boolean isPrimaryActive() {
        return primaryActive;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    static ClusterShardHealth readClusterShardHealth(StreamInput in) throws IOException {
        ClusterShardHealth ret = new ClusterShardHealth();
        ret.readFrom(in);
        return ret;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        shardId = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        primaryActive = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeByte(status.value());
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeBoolean(primaryActive);
    }
}
