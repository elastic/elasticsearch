/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public class ClusterHealthResponse extends ActionResponse implements ToXContentObject {
    static final String CLUSTER_NAME = "cluster_name";
    static final String STATUS = "status";
    static final String TIMED_OUT = "timed_out";
    static final String NUMBER_OF_NODES = "number_of_nodes";
    static final String NUMBER_OF_DATA_NODES = "number_of_data_nodes";
    static final String NUMBER_OF_PENDING_TASKS = "number_of_pending_tasks";
    static final String NUMBER_OF_IN_FLIGHT_FETCH = "number_of_in_flight_fetch";
    static final String DELAYED_UNASSIGNED_SHARDS = "delayed_unassigned_shards";
    private static final String TASK_MAX_WAIT_TIME_IN_QUEUE = "task_max_waiting_in_queue";
    static final String TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS = "task_max_waiting_in_queue_millis";
    static final String ACTIVE_SHARDS_PERCENT_AS_NUMBER = "active_shards_percent_as_number";
    private static final String ACTIVE_SHARDS_PERCENT = "active_shards_percent";
    static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    static final String ACTIVE_SHARDS = "active_shards";
    static final String RELOCATING_SHARDS = "relocating_shards";
    static final String INITIALIZING_SHARDS = "initializing_shards";
    static final String UNASSIGNED_SHARDS = "unassigned_shards";
    static final String UNASSIGNED_PRIMARY_SHARDS = "unassigned_primary_shards";
    static final String INDICES = "indices";

    private String clusterName;
    private int numberOfPendingTasks = 0;
    private int numberOfInFlightFetch = 0;
    private int delayedUnassignedShards = 0;
    private TimeValue taskMaxWaitingTime = TimeValue.timeValueMillis(0);
    private boolean timedOut = false;
    private ClusterStateHealth clusterStateHealth;
    private ClusterHealthStatus clusterHealthStatus;

    public ClusterHealthResponse() {}

    public ClusterHealthResponse(StreamInput in) throws IOException {
        clusterName = in.readString();
        clusterHealthStatus = ClusterHealthStatus.readFrom(in);
        clusterStateHealth = new ClusterStateHealth(in);
        numberOfPendingTasks = in.readInt();
        timedOut = in.readBoolean();
        numberOfInFlightFetch = in.readInt();
        delayedUnassignedShards = in.readInt();
        taskMaxWaitingTime = in.readTimeValue();
    }

    /** needed for plugins BWC */
    public ClusterHealthResponse(String clusterName, String[] concreteIndices, ClusterState clusterState) {
        this(
            clusterName,
            concreteIndices,
            clusterState,
            clusterState.metadata().getProject().id(),
            -1,
            -1,
            -1,
            TimeValue.timeValueHours(0)
        );
    }

    public ClusterHealthResponse(
        String clusterName,
        String[] concreteIndices,
        ClusterState clusterState,
        ProjectId projectId,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime
    ) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.taskMaxWaitingTime = taskMaxWaitingTime;
        this.clusterStateHealth = new ClusterStateHealth(clusterState, concreteIndices, projectId);
        this.clusterHealthStatus = clusterStateHealth.getStatus();
    }

    /**
     * For XContent Parser and serialization tests
     */
    ClusterHealthResponse(
        String clusterName,
        int numberOfPendingTasks,
        int numberOfInFlightFetch,
        int delayedUnassignedShards,
        TimeValue taskMaxWaitingTime,
        boolean timedOut,
        ClusterStateHealth clusterStateHealth
    ) {
        this.clusterName = clusterName;
        this.numberOfPendingTasks = numberOfPendingTasks;
        this.numberOfInFlightFetch = numberOfInFlightFetch;
        this.delayedUnassignedShards = delayedUnassignedShards;
        this.taskMaxWaitingTime = taskMaxWaitingTime;
        this.timedOut = timedOut;
        this.clusterStateHealth = clusterStateHealth;
        this.clusterHealthStatus = clusterStateHealth.getStatus();
    }

    public String getClusterName() {
        return clusterName;
    }

    // package private for testing
    ClusterStateHealth getClusterStateHealth() {
        return clusterStateHealth;
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

    public int getUnassignedPrimaryShards() {
        return clusterStateHealth.getUnassignedPrimaryShards();
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
     * {@code true} if the waitForXXX has timeout out and did not match.
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
        return new ClusterHealthResponse(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(clusterName);
        out.writeByte(clusterHealthStatus.value());
        clusterStateHealth.writeTo(out);
        out.writeInt(numberOfPendingTasks);
        out.writeBoolean(timedOut);
        out.writeInt(numberOfInFlightFetch);
        out.writeInt(delayedUnassignedShards);
        out.writeTimeValue(taskMaxWaitingTime);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public RestStatus status() {
        return isTimedOut() ? RestStatus.REQUEST_TIMEOUT : RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CLUSTER_NAME, getClusterName());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(TIMED_OUT, isTimedOut());
        builder.field(NUMBER_OF_NODES, getNumberOfNodes());
        builder.field(NUMBER_OF_DATA_NODES, getNumberOfDataNodes());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(UNASSIGNED_PRIMARY_SHARDS, getUnassignedPrimaryShards());
        builder.field(DELAYED_UNASSIGNED_SHARDS, getDelayedUnassignedShards());
        builder.field(NUMBER_OF_PENDING_TASKS, getNumberOfPendingTasks());
        builder.field(NUMBER_OF_IN_FLIGHT_FETCH, getNumberOfInFlightFetch());
        builder.humanReadableField(TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS, TASK_MAX_WAIT_TIME_IN_QUEUE, getTaskMaxWaitingTime());
        builder.percentageField(ACTIVE_SHARDS_PERCENT_AS_NUMBER, ACTIVE_SHARDS_PERCENT, getActiveShardsPercent());

        ClusterStatsLevel level = ClusterStatsLevel.of(params, ClusterStatsLevel.CLUSTER);
        boolean outputIndices = level == ClusterStatsLevel.INDICES || level == ClusterStatsLevel.SHARDS;

        if (outputIndices) {
            builder.startObject(INDICES);
            for (ClusterIndexHealth indexHealth : clusterStateHealth.getIndices().values()) {
                indexHealth.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterHealthResponse that = (ClusterHealthResponse) o;
        return Objects.equals(clusterName, that.clusterName)
            && numberOfPendingTasks == that.numberOfPendingTasks
            && numberOfInFlightFetch == that.numberOfInFlightFetch
            && delayedUnassignedShards == that.delayedUnassignedShards
            && Objects.equals(taskMaxWaitingTime, that.taskMaxWaitingTime)
            && timedOut == that.timedOut
            && Objects.equals(clusterStateHealth, that.clusterStateHealth)
            && clusterHealthStatus == that.clusterHealthStatus;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            clusterName,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            delayedUnassignedShards,
            taskMaxWaitingTime,
            timedOut,
            clusterStateHealth,
            clusterHealthStatus
        );
    }
}
