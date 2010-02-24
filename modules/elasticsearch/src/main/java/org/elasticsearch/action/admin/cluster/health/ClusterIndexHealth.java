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

import com.google.common.collect.Maps;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.health.ClusterShardHealth.*;

/**
 * @author kimchy (shay.banon)
 */
public class ClusterIndexHealth implements Iterable<ClusterShardHealth>, Streamable {

    private String index;

    private int numberOfShards;

    private int numberOfReplicas;

    int activeShards = 0;

    int relocatingShards = 0;

    int activePrimaryShards = 0;

    ClusterHealthStatus status = ClusterHealthStatus.RED;

    final Map<Integer, ClusterShardHealth> shards = Maps.newHashMap();

    private ClusterIndexHealth() {
    }

    public ClusterIndexHealth(String index, int numberOfShards, int numberOfReplicas) {
        this.index = index;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
    }

    public String index() {
        return index;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public int numberOfReplicas() {
        return numberOfReplicas;
    }

    public int activeShards() {
        return activeShards;
    }

    public int relocatingShards() {
        return relocatingShards;
    }

    public int activePrimaryShards() {
        return activePrimaryShards;
    }

    public ClusterHealthStatus status() {
        return status;
    }

    public Map<Integer, ClusterShardHealth> shards() {
        return this.shards;
    }

    @Override public Iterator<ClusterShardHealth> iterator() {
        return shards.values().iterator();
    }

    public static ClusterIndexHealth readClusterIndexHealth(DataInput in) throws IOException, ClassNotFoundException {
        ClusterIndexHealth indexHealth = new ClusterIndexHealth();
        indexHealth.readFrom(in);
        return indexHealth;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        index = in.readUTF();
        numberOfShards = in.readInt();
        numberOfReplicas = in.readInt();
        activePrimaryShards = in.readInt();
        activeShards = in.readInt();
        relocatingShards = in.readInt();
        status = ClusterHealthStatus.fromValue(in.readByte());

        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            ClusterShardHealth shardHealth = readClusterShardHealth(in);
            shards.put(shardHealth.id(), shardHealth);
        }
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeUTF(index);
        out.writeInt(numberOfShards);
        out.writeInt(numberOfReplicas);
        out.writeInt(activePrimaryShards);
        out.writeInt(activeShards);
        out.writeInt(relocatingShards);
        out.writeByte(status.value());

        out.writeInt(shards.size());
        for (ClusterShardHealth shardHealth : this) {
            shardHealth.writeTo(out);
        }
    }
}
