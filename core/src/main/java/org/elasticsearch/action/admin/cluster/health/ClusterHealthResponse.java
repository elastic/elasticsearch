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
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.StatusToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class ClusterHealthResponse extends ActionResponse implements StatusToXContent {
    private String clusterName;
    private int numberOfPendingTasks = 0;
    private int numberOfInFlightFetch = 0;
    private int delayedUnassignedShards = 0;
    private TimeValue taskMaxWaitingTime = TimeValue.timeValueMillis(0);
    private boolean timedOut = false;
    private ClusterStateHealth clusterStateHealth;
    private ClusterHealthStatus clusterHealthStatus;

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
        this.clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices);
        this.clusterHealthStatus = clusterStateHealth.getStatus();
    }

    public String getClusterName() {
        return clusterName;
    }

    //package private for testing
    ClusterStateHealth getClusterStateHealth() {
        return clusterStateHealth;
    }

    /**
     * The validation failures on the cluster level (without index validation failures).
     */
    public List<String> getValidationFailures() {
        return clusterStateHealth.getValidationFailures();
    }


    public int getActiveShards() {
        return clusterStateHealth.getActiveShards();
    }

    public int getRelocatingShards() {
        return clusterStateHealth.getRelocatingShards();
    }

    public int getActivePrimaryShards() {
        return clusterStateHealth.getActivePrimaryShards();
    }

    public int getInitializingShards() {
        return clusterStateHealth.getInitializingShards();
    }

    public int getUnassignedShards() {
        return clusterStateHealth.getUnassignedShards();
    }

    public int getNumberOfNodes() {
        return clusterStateHealth.getNumberOfNodes();
    }

    public int getNumberOfDataNodes() {
        return clusterStateHealth.getNumberOfDataNodes();
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

    public void setTimedOut(boolean timedOut) {
        this.timedOut = timedOut;
    }

    public ClusterHealthStatus getStatus() {
        return clusterHealthStatus;
    }

    /**
     * Allows to explicitly override the derived cluster health status.
     *
     * @param status The override status. Must not be null.
     */
    public void setStatus(ClusterHealthStatus status) {
        if (status == null) {
            throw new IllegalArgumentException("'status' must not be null");
        }
        this.clusterHealthStatus = status;
    }

    public Map<String, ClusterIndexHealth> getIndices() {
        return clusterStateHealth.getIndices();
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
        return clusterStateHealth.getActiveShardsPercent();
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
        clusterHealthStatus = ClusterHealthStatus.fromValue(in.readByte());
        clusterStateHealth = ClusterStateHealth.readClusterHealth(in);
        numberOfPendingTasks = in.readInt();
        timedOut = in.readBoolean();
        numberOfInFlightFetch = in.readInt();
        if (in.getVersion().onOrAfter(Version.V_1_7_0)) {
            delayedUnassignedShards= in.readInt();
        }
        taskMaxWaitingTime = TimeValue.readTimeValue(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(clusterName);
        out.writeByte(clusterHealthStatus.value());
        clusterStateHealth.writeTo(out);
        out.writeInt(numberOfPendingTasks);
        out.writeBoolean(timedOut);
        out.writeInt(numberOfInFlightFetch);
        if (out.getVersion().onOrAfter(Version.V_1_7_0)) {
            out.writeInt(delayedUnassignedShards);
        }
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
                for (ClusterIndexHealth indexHealth : clusterStateHealth.getIndices().values()) {
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
            for (ClusterIndexHealth indexHealth : clusterStateHealth.getIndices().values()) {
                builder.startObject(indexHealth.getIndex(), XContentBuilder.FieldCaseConversion.NONE);
                indexHealth.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }
}
