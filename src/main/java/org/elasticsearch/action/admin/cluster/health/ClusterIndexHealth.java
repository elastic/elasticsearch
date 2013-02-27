/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.health.ClusterShardHealth.readClusterShardHealth;

/**
 *
 */
public class ClusterIndexHealth implements Iterable<ClusterShardHealth>, Streamable {

    private String index;

    private int numberOfShards;

    private int numberOfReplicas;

    int activeShards = 0;

    int relocatingShards = 0;

    int initializingShards = 0;

    int unassignedShards = 0;

    int activePrimaryShards = 0;

    ClusterHealthStatus status = ClusterHealthStatus.RED;

    final Map<Integer, ClusterShardHealth> shards = Maps.newHashMap();

    List<String> validationFailures;

    private ClusterIndexHealth() {
    }

    public ClusterIndexHealth(String index, int numberOfShards, int numberOfReplicas, List<String> validationFailures) {
        this.index = index;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.validationFailures = validationFailures;
    }

    public String getIndex() {
        return index;
    }

    public List<String> getValidationFailures() {
        return this.validationFailures;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<Integer, ClusterShardHealth> getShards() {
        return this.shards;
    }

    @Override
    public Iterator<ClusterShardHealth> iterator() {
        return shards.values().iterator();
    }

    public static ClusterIndexHealth readClusterIndexHealth(StreamInput in) throws IOException {
        ClusterIndexHealth indexHealth = new ClusterIndexHealth();
        indexHealth.readFrom(in);
        return indexHealth;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = in.readString();
        numberOfShards = in.readVInt();
        numberOfReplicas = in.readVInt();
        activePrimaryShards = in.readVInt();
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());

        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            ClusterShardHealth shardHealth = readClusterShardHealth(in);
            shards.put(shardHealth.getId(), shardHealth);
        }
        size = in.readVInt();
        if (size == 0) {
            validationFailures = ImmutableList.of();
        } else {
            for (int i = 0; i < size; i++) {
                validationFailures.add(in.readString());
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(numberOfShards);
        out.writeVInt(numberOfReplicas);
        out.writeVInt(activePrimaryShards);
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeByte(status.value());

        out.writeVInt(shards.size());
        for (ClusterShardHealth shardHealth : this) {
            shardHealth.writeTo(out);
        }

        out.writeVInt(validationFailures.size());
        for (String failure : validationFailures) {
            out.writeString(failure);
        }
    }
}
