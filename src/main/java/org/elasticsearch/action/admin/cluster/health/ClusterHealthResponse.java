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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableValidation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth.readClusterIndexHealth;

/**
 *
 */
public class ClusterHealthResponse extends ActionResponse implements Iterable<ClusterIndexHealth>, ToXContent {

    private String clusterName;
    int numberOfNodes = 0;
    int numberOfDataNodes = 0;
    int activeShards = 0;
    int relocatingShards = 0;
    int activePrimaryShards = 0;
    int initializingShards = 0;
    int unassignedShards = 0;
    int numberOfPendingTasks = 0;
    boolean timedOut = false;
    ClusterHealthStatus status = ClusterHealthStatus.RED;
    private List<String> validationFailures;
    Map<String, ClusterIndexHealth> indices = Maps.newHashMap();

    ClusterHealthResponse() {
    }

    /** needed for plugins BWC */
    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState) {
        this(clusterName, concreteIndices, clusterState, -1);
    }

    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState, int numberOfPendingTasks) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
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
            activePrimaryShards += indexHealth.activePrimaryShards;
            activeShards += indexHealth.activeShards;
            relocatingShards += indexHealth.relocatingShards;
            initializingShards += indexHealth.initializingShards;
            unassignedShards += indexHealth.unassignedShards;
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
    }

    public String getClusterName() {
        return clusterName;
    }

    /**
     * The validation failures on the cluster level (without index validation failures).
     */
    public List<String> getValidationFailures() {
        return this.validationFailures;
    }

    /**
     * All the validation failures, including index level validation failures.
     */
    public List<String> getAllValidationFailures() {
        List<String> allFailures = newArrayList(getValidationFailures());
        for (ClusterIndexHealth indexHealth : indices.values()) {
            allFailures.addAll(indexHealth.getValidationFailures());
        }
        return allFailures;
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

    public int getNumberOfPendingTasks() {
        return this.numberOfPendingTasks;
    }

    /**
     * <tt>true</tt> if the waitForXXX has timeout out and did not match.
     */
    public boolean isTimedOut() {
        return this.timedOut;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return indices;
    }

    @Override
    public Iterator<ClusterIndexHealth> iterator() {
        return indices.values().iterator();
    }


    public static ClusterHealthResponse readResponseFrom(StreamInput in) throws IOException {
        ClusterHealthResponse response = new ClusterHealthResponse();
        response.readFrom(in);
        return response;
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
        numberOfPendingTasks = in.readInt();
        status = ClusterHealthStatus.fromValue(in.readByte());
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            ClusterIndexHealth indexHealth = readClusterIndexHealth(in);
            indices.put(indexHealth.getIndex(), indexHealth);
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
        out.writeInt(numberOfPendingTasks);
        out.writeByte(status.value());
        out.writeVInt(indices.size());
        for (ClusterIndexHealth indexHealth : this) {
            indexHealth.writeTo(out);
        }
        out.writeBoolean(timedOut);

        out.writeVInt(validationFailures.size());
        for (String failure : validationFailures) {
            out.writeString(failure);
        }
    }


    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }

    static final class Fields {
        static final XContentBuilderString CLUSTER_NAME = new XContentBuilderString("cluster_name");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString TIMED_OUT = new XContentBuilderString("timed_out");
        static final XContentBuilderString NUMBER_OF_NODES = new XContentBuilderString("number_of_nodes");
        static final XContentBuilderString NUMBER_OF_DATA_NODES = new XContentBuilderString("number_of_data_nodes");
        static final XContentBuilderString NUMBER_OF_PENDING_TASKS = new XContentBuilderString("number_of_pending_tasks");
        static final XContentBuilderString ACTIVE_PRIMARY_SHARDS = new XContentBuilderString("active_primary_shards");
        static final XContentBuilderString ACTIVE_SHARDS = new XContentBuilderString("active_shards");
        static final XContentBuilderString RELOCATING_SHARDS = new XContentBuilderString("relocating_shards");
        static final XContentBuilderString INITIALIZING_SHARDS = new XContentBuilderString("initializing_shards");
        static final XContentBuilderString UNASSIGNED_SHARDS = new XContentBuilderString("unassigned_shards");
        static final XContentBuilderString VALIDATION_FAILURES = new XContentBuilderString("validation_failures");
        static final XContentBuilderString INDICES = new XContentBuilderString("indices");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.CLUSTER_NAME, getClusterName());
        builder.field(Fields.STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(Fields.TIMED_OUT, isTimedOut());
        builder.field(Fields.NUMBER_OF_NODES, getNumberOfNodes());
        builder.field(Fields.NUMBER_OF_DATA_NODES, getNumberOfDataNodes());
        builder.field(Fields.ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(Fields.ACTIVE_SHARDS, getActiveShards());
        builder.field(Fields.RELOCATING_SHARDS, getRelocatingShards());
        builder.field(Fields.INITIALIZING_SHARDS, getInitializingShards());
        builder.field(Fields.UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(Fields.NUMBER_OF_PENDING_TASKS, getNumberOfPendingTasks());

        String level = params.param("level", "cluster");
        boolean outputIndices = "indices".equals(level) || "shards".equals(level);


        if (!getValidationFailures().isEmpty()) {
            builder.startArray(Fields.VALIDATION_FAILURES);
            for (String validationFailure : getValidationFailures()) {
                builder.value(validationFailure);
            }
            // if we don't print index level information, still print the index validation failures
            // so we know why the status is red
            if (!outputIndices) {
                for (ClusterIndexHealth indexHealth : indices.values()) {
                    builder.startObject(indexHealth.getIndex());

                    if (!indexHealth.getValidationFailures().isEmpty()) {
                        builder.startArray(Fields.VALIDATION_FAILURES);
                        for (String validationFailure : indexHealth.getValidationFailures()) {
                            builder.value(validationFailure);
                        }
                        builder.endArray();
                    }

                    builder.endObject();
                }
            }
            builder.endArray();
        }

        if (outputIndices) {
            builder.startObject(Fields.INDICES);
            for (ClusterIndexHealth indexHealth : indices.values()) {
                builder.startObject(indexHealth.getIndex(), XContentBuilder.FieldCaseConversion.NONE);
                indexHealth.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }
}
