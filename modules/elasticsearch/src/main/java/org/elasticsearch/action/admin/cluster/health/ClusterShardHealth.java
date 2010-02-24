/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author kimchy (shay.banon)
 */
public class ClusterShardHealth implements Streamable {

    private int shardId;

    ClusterHealthStatus status = ClusterHealthStatus.RED;

    int activeShards = 0;

    int relocatingShards = 0;

    boolean primaryActive = false;

    private ClusterShardHealth() {

    }

    ClusterShardHealth(int shardId) {
        this.shardId = shardId;
    }

    public int id() {
        return shardId;
    }

    public ClusterHealthStatus status() {
        return status;
    }

    public int relocatingShards() {
        return relocatingShards;
    }

    public int activeShards() {
        return activeShards;
    }

    public boolean primaryActive() {
        return primaryActive;
    }

    static ClusterShardHealth readClusterShardHealth(DataInput in) throws IOException, ClassNotFoundException {
        ClusterShardHealth ret = new ClusterShardHealth();
        ret.readFrom(in);
        return ret;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        shardId = in.readInt();
        status = ClusterHealthStatus.fromValue(in.readByte());
        activeShards = in.readInt();
        relocatingShards = in.readInt();
        primaryActive = in.readBoolean();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeInt(shardId);
        out.writeByte(status.value());
        out.writeInt(activeShards);
        out.writeInt(relocatingShards);
        out.writeBoolean(primaryActive);
    }
}
