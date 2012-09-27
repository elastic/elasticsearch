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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth.readClusterIndexHealth;

/**
 *
 */
public class ClusterHealthResponse extends ActionResponse implements Iterable<ClusterIndexHealth> {

    private String clusterName;
    int numberOfNodes = 0;
    int numberOfDataNodes = 0;
    int activeShards = 0;
    int relocatingShards = 0;
    int activePrimaryShards = 0;
    int initializingShards = 0;
    int unassignedShards = 0;
    boolean timedOut = false;
    ClusterHealthStatus status = ClusterHealthStatus.RED;
    private List<String> validationFailures;
    Map<String, ClusterIndexHealth> indices = Maps.newHashMap();

    ClusterHealthResponse() {
    }

    public ClusterHealthResponse(String clusterName, List<String> validationFailures) {
        this.clusterName = clusterName;
        this.validationFailures = validationFailures;
    }

    public String clusterName() {
        return clusterName;
    }

    public String getClusterName() {
        return clusterName();
    }

    /**
     * The validation failures on the cluster level (without index validation failures).
     */
    public List<String> validationFailures() {
        return this.validationFailures;
    }

    /**
     * The validation failures on the cluster level (without index validation failures).
     */
    public List<String> getValidationFailures() {
        return validationFailures();
    }

    /**
     * All the validation failures, including index level validation failures.
     */
    public List<String> allValidationFailures() {
        List<String> allFailures = newArrayList(validationFailures());
        for (ClusterIndexHealth indexHealth : indices.values()) {
            allFailures.addAll(indexHealth.validationFailures());
        }
        return allFailures;
    }

    /**
     * All the validation failures, including index level validation failures.
     */
    public List<String> getAllValidationFailures() {
        return allValidationFailures();
    }


    public int activeShards() {
        return activeShards;
    }

    public int getActiveShards() {
        return activeShards();
    }

    public int relocatingShards() {
        return relocatingShards;
    }

    public int getRelocatingShards() {
        return relocatingShards();
    }

    public int activePrimaryShards() {
        return activePrimaryShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards();
    }

    public int initializingShards() {
        return initializingShards;
    }

    public int getInitializingShards() {
        return initializingShards();
    }

    public int unassignedShards() {
        return unassignedShards;
    }

    public int getUnassignedShards() {
        return unassignedShards();
    }

    public int numberOfNodes() {
        return this.numberOfNodes;
    }

    public int getNumberOfNodes() {
        return numberOfNodes();
    }

    public int numberOfDataNodes() {
        return this.numberOfDataNodes;
    }

    public int getNumberOfDataNodes() {
        return numberOfDataNodes();
    }

    /**
     * <tt>true</tt> if the waitForXXX has timeout out and did not match.
     */
    public boolean timedOut() {
        return this.timedOut;
    }

    public boolean isTimedOut() {
        return this.timedOut();
    }

    public ClusterHealthStatus status() {
        return status;
    }

    public ClusterHealthStatus getStatus() {
        return status();
    }

    public Map<String, ClusterIndexHealth> indices() {
        return indices;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return indices();
    }

    @Override
    public Iterator<ClusterIndexHealth> iterator() {
        return indices.values().iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = in.readString();
        activePrimaryShards = in.readVInt();
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        numberOfNodes = in.readVInt();
        numberOfDataNodes = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            ClusterIndexHealth indexHealth = readClusterIndexHealth(in);
            indices.put(indexHealth.index(), indexHealth);
        }
        timedOut = in.readBoolean();
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
        super.writeTo(out);
        out.writeString(clusterName);
        out.writeVInt(activePrimaryShards);
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeVInt(numberOfNodes);
        out.writeVInt(numberOfDataNodes);
        out.writeByte(status.value());
        out.writeVInt(indices.size());
        for (ClusterIndexHealth indexHealth : this) {
            indexHealth.writeTo(out);
        }
        out.writeBoolean(timedOut);

        out.writeVInt(validationFailures.size());
        for (String failure : validationFailures) {
            out.writeUTF(failure);
        }
    }

}
