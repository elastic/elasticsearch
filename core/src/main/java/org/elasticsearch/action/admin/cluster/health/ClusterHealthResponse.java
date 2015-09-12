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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableValidation;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.health.ClusterIndexHealth.readClusterIndexHealth;

/**
 *
 */
public class ClusterHealthResponse extends ActionResponse implements Iterable<ClusterIndexHealth>, StatusToXContent {

    private String clusterName;
    int numberOfNodes = 0;
    int numberOfDataNodes = 0;
    int activeShards = 0;
    int relocatingShards = 0;
    int activePrimaryShards = 0;
    int initializingShards = 0;
    int unassignedShards = 0;
    int numberOfPendingTasks = 0;
    int numberOfInFlightFetch = 0;
    int delayedUnassignedShards = 0;
    TimeValue taskMaxWaitingTime = TimeValue.timeValueMillis(0);
    double activeShardsPercent = 100;
    boolean timedOut = false;
    ClusterHealthStatus status = ClusterHealthStatus.RED;
    private List<String> validationFailures;
    Map<String, ClusterIndexHealth> indices = new HashMap<>();

    ClusterHealthResponse() {
    }

    /** needed for plugins BWC */
    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState) {
        this(clusterName, concreteIndices, clusterState, -1, -1, -1, TimeValue.timeValueHours(0));
    }

    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState, int numberOfPendingTasks,
                                 int numberOfInFlightFetch, int delayedUnassignedShards, TimeValue taskMaxWaitingTime) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.taskMaxWaitingTime = taskMaxWaitingTime;
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
        List<String> allFailures = new ArrayList<>(getValidationFailures());
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

    public int getNumberOfInFlightFetch() {
        return this.numberOfInFlightFetch;
    }

    /**
     * The number of unassigned shards that are currently being delayed (for example,
     * due to node leaving the cluster and waiting for a timeout for the node to come
     * back in order to allocate the shards back to it).
     */
    public int getDelayedUnassignedShards() {
        return this.delayedUnassignedShards;
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

    /**
     *
     * @return The maximum wait time of all tasks in the queue
     */
    public TimeValue getTaskMaxWaitingTime() {
        return taskMaxWaitingTime;
    }

    /**
     * The percentage of active shards, should be 100% in a green system
     */
    public double getActiveShardsPercent() {
        return activeShardsPercent;
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
            validationFailures = Collections.emptyList();
        } else {
            for (int i = 0; i < size; i++) {
                validationFailures.add(in.readString());
            }
        }

        numberOfInFlightFetch = in.readInt();
        if (in.getVersion().onOrAfter(Version.V_1_7_0)) {
            delayedUnassignedShards= in.readInt();
        }

        activeShardsPercent = in.readDouble();
        taskMaxWaitingTime = TimeValue.readTimeValue(in);
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

        out.writeInt(numberOfInFlightFetch);
        if (out.getVersion().onOrAfter(Version.V_1_7_0)) {
            out.writeInt(delayedUnassignedShards);
        }
        out.writeDouble(activeShardsPercent);
        taskMaxWaitingTime.writeTo(out);
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

    @Override
    public RestStatus status() {
        return isTimedOut() ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
    }

    static final class Fields {
        static final XContentBuilderString CLUSTER_NAME = new XContentBuilderString("cluster_name");
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString TIMED_OUT = new XContentBuilderString("timed_out");
        static final XContentBuilderString NUMBER_OF_NODES = new XContentBuilderString("number_of_nodes");
        static final XContentBuilderString NUMBER_OF_DATA_NODES = new XContentBuilderString("number_of_data_nodes");
        static final XContentBuilderString NUMBER_OF_PENDING_TASKS = new XContentBuilderString("number_of_pending_tasks");
        static final XContentBuilderString NUMBER_OF_IN_FLIGHT_FETCH = new XContentBuilderString("number_of_in_flight_fetch");
        static final XContentBuilderString DELAYED_UNASSIGNED_SHARDS = new XContentBuilderString("delayed_unassigned_shards");
        static final XContentBuilderString TASK_MAX_WAIT_TIME_IN_QUEUE = new XContentBuilderString("task_max_waiting_in_queue");
        static final XContentBuilderString TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS = new XContentBuilderString("task_max_waiting_in_queue_millis");
        static final XContentBuilderString ACTIVE_SHARDS_PERCENT_AS_NUMBER = new XContentBuilderString("active_shards_percent_as_number");
        static final XContentBuilderString ACTIVE_SHARDS_PERCENT = new XContentBuilderString("active_shards_percent");
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
        builder.field(Fields.DELAYED_UNASSIGNED_SHARDS, getDelayedUnassignedShards());
        builder.field(Fields.NUMBER_OF_PENDING_TASKS, getNumberOfPendingTasks());
        builder.field(Fields.NUMBER_OF_IN_FLIGHT_FETCH, getNumberOfInFlightFetch());
        builder.timeValueField(Fields.TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS, Fields.TASK_MAX_WAIT_TIME_IN_QUEUE, getTaskMaxWaitingTime());
        builder.percentageField(Fields.ACTIVE_SHARDS_PERCENT_AS_NUMBER, Fields.ACTIVE_SHARDS_PERCENT, getActiveShardsPercent());

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
