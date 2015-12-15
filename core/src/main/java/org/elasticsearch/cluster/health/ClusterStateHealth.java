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
package org.elasticsearch.cluster.health;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableValidation;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.*;

import static org.elasticsearch.cluster.health.ClusterIndexHealth.readClusterIndexHealth;

public final class ClusterStateHealth implements Iterable<ClusterIndexHealth>, Streamable {
    private int numberOfNodes = 0;
    private int numberOfDataNodes = 0;
    private int activeShards = 0;
    private int relocatingShards = 0;
    private int activePrimaryShards = 0;
    private int initializingShards = 0;
    private int unassignedShards = 0;
    private double activeShardsPercent = 100;
    private ClusterHealthStatus status = ClusterHealthStatus.RED;
    private List<String> validationFailures;
    private Map<String, ClusterIndexHealth> indices = new HashMap<>();

    public static ClusterStateHealth readClusterHealth(StreamInput in) throws IOException {
        ClusterStateHealth clusterStateHealth = new ClusterStateHealth();
        clusterStateHealth.readFrom(in);
        return clusterStateHealth;
    }

    ClusterStateHealth() {
        // only intended for serialization
    }

    /**
     * Creates a new <code>ClusterStateHealth</code> instance based on cluster meta data and its routing table as a convenience.
     *
     * @param clusterMetaData Current cluster meta data. Must not be null.
     * @param routingTables   Current routing table. Must not be null.
     */
    public ClusterStateHealth(MetaData clusterMetaData, RoutingTable routingTables) {
        this(ClusterState.builder(ClusterName.DEFAULT).metaData(clusterMetaData).routingTable(routingTables).build());
    }

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and all indices in the cluster.
     *
     * @param clusterState The current cluster state. Must not be null.
     */
    public ClusterStateHealth(ClusterState clusterState) {
        this(clusterState, clusterState.metaData().concreteAllIndices());
    }

    /**
     * Creates a new <code>ClusterStateHealth</code> instance considering the current cluster state and the provided index names.
     *
     * @param clusterState    The current cluster state. Must not be null.
     * @param concreteIndices An array of index names to consider. Must not be null but may be empty.
     */
    public ClusterStateHealth(ClusterState clusterState, String[] concreteIndices) {
        RoutingTableValidation validation = clusterState.routingTable().validate(clusterState.metaData());
        validationFailures = validation.failures();
        numberOfNodes = clusterState.nodes().size();
        numberOfDataNodes = clusterState.nodes().dataNodes().size();

        for (String index : concreteIndices) {
            IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
            IndexMetaData indexMetaData = clusterState.metaData().index(index);
            if (indexRoutingTable == null) {
                continue;
            }

            ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetaData, indexRoutingTable);

            indices.put(indexHealth.getIndex(), indexHealth);
        }

        status = ClusterHealthStatus.GREEN;

        for (ClusterIndexHealth indexHealth : indices.values()) {
            activePrimaryShards += indexHealth.getActivePrimaryShards();
            activeShards += indexHealth.getActiveShards();
            relocatingShards += indexHealth.getRelocatingShards();
            initializingShards += indexHealth.getInitializingShards();
            unassignedShards += indexHealth.getUnassignedShards();
            if (indexHealth.getStatus() == ClusterHealthStatus.RED) {
                status = ClusterHealthStatus.RED;
            } else if (indexHealth.getStatus() == ClusterHealthStatus.YELLOW && status != ClusterHealthStatus.RED) {
                status = ClusterHealthStatus.YELLOW;
            }
        }

        if (!validationFailures.isEmpty()) {
            status = ClusterHealthStatus.RED;
        } else if (clusterState.blocks().hasGlobalBlock(RestStatus.SERVICE_UNAVAILABLE)) {
            status = ClusterHealthStatus.RED;
        }

        // shortcut on green
        if (status.equals(ClusterHealthStatus.GREEN)) {
            this.activeShardsPercent = 100;
        } else {
            List<ShardRouting> shardRoutings = clusterState.getRoutingTable().allShards();
            int activeShardCount = 0;
            int totalShardCount = 0;
            for (ShardRouting shardRouting : shardRoutings) {
                if (shardRouting.active()) activeShardCount++;
                totalShardCount++;
            }
            this.activeShardsPercent = (((double) activeShardCount) / totalShardCount) * 100;
        }
    }

    public List<String> getValidationFailures() {
        return Collections.unmodifiableList(validationFailures);
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

    public int getNumberOfNodes() {
        return this.numberOfNodes;
    }

    public int getNumberOfDataNodes() {
        return this.numberOfDataNodes;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return Collections.unmodifiableMap(indices);
    }

    public double getActiveShardsPercent() {
        return activeShardsPercent;
    }

    @Override
    public Iterator<ClusterIndexHealth> iterator() {
        return indices.values().iterator();
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
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
            indices.put(indexHealth.getIndex(), indexHealth);
        }
        size = in.readVInt();
        if (size == 0) {
            validationFailures = Collections.emptyList();
        } else {
            for (int i = 0; i < size; i++) {
                validationFailures.add(in.readString());
            }
        }
        activeShardsPercent = in.readDouble();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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
        out.writeVInt(validationFailures.size());
        for (String failure : validationFailures) {
            out.writeString(failure);
        }
        out.writeDouble(activeShardsPercent);
    }
}
