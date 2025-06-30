/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

/**
 * {@link Task.Status} reported from a {@link Driver} to be reported by the tasks api.
 *
 * @param sessionId The session for this driver.
 * @param description Description of the driver.
 * @param clusterName The name of the cluster this driver is running on.
 * @param nodeName The name of the node this driver is running on.
 * @param started When this {@link Driver} was started.
 * @param lastUpdated When this status was generated.
 * @param cpuNanos Nanos this {@link Driver} has been running on the cpu. Does not include async or waiting time.
 * @param iterations The number of times the driver has moved a single page up the chain of operators as far as it'll go.
 * @param status The state of the overall driver - queue, starting, running, finished.
 * @param completedOperators Status of each completed {@link Operator} in the driver.
 * @param activeOperators Status of each active {@link Operator} in the driver.
 */
public record DriverStatus(
    String sessionId,
    String description,
    String clusterName,
    String nodeName,
    long started,
    long lastUpdated,
    long cpuNanos,
    long iterations,
    Status status,
    List<OperatorStatus> completedOperators,
    List<OperatorStatus> activeOperators,
    DriverSleeps sleeps
) implements Task.Status {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Task.Status.class,
        "driver",
        DriverStatus::readFrom
    );

    public static DriverStatus readFrom(StreamInput in) throws IOException {
        return new DriverStatus(
            in.readString(),
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_TASK_DESCRIPTION)
                || in.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0)
                || in.getTransportVersion().isPatchFrom(TransportVersions.ESQL_DRIVER_TASK_DESCRIPTION_8_19) ? in.readString() : "",
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_NODE_DESCRIPTION) ? in.readString() : "",
            in.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_NODE_DESCRIPTION) ? in.readString() : "",
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readLong() : 0,
            in.readLong(),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0,
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0) ? in.readVLong() : 0,
            Status.read(in),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)
                ? in.readCollectionAsImmutableList(OperatorStatus::readFrom)
                : List.of(),
            in.readCollectionAsImmutableList(OperatorStatus::readFrom),
            DriverSleeps.read(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sessionId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_TASK_DESCRIPTION)
            || out.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0)
            || out.getTransportVersion().isPatchFrom(TransportVersions.ESQL_DRIVER_TASK_DESCRIPTION_8_19)) {
            out.writeString(description);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_DRIVER_NODE_DESCRIPTION)) {
            out.writeString(clusterName);
            out.writeString(nodeName);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeLong(started);
        }
        out.writeLong(lastUpdated);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_14_0)) {
            out.writeVLong(cpuNanos);
            out.writeVLong(iterations);
        }
        status.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeCollection(completedOperators);
        }
        out.writeCollection(activeOperators);
        sleeps.writeTo(out);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("session_id", sessionId);
        builder.field("description", description);
        builder.field("cluster_name", clusterName);
        builder.field("node_name", nodeName);
        builder.field("started", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(started));
        builder.field("last_updated", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(lastUpdated));
        builder.field("cpu_nanos", cpuNanos);
        if (builder.humanReadable()) {
            builder.field("cpu_time", TimeValue.timeValueNanos(cpuNanos));
        }
        builder.field("documents_found", documentsFound());
        builder.field("values_loaded", valuesLoaded());
        builder.field("iterations", iterations);
        builder.field("status", status, params);
        builder.startArray("completed_operators");
        for (OperatorStatus completed : completedOperators) {
            builder.value(completed);
        }
        builder.endArray();
        builder.startArray("active_operators");
        for (OperatorStatus active : activeOperators) {
            builder.value(active);
        }
        builder.endArray();
        builder.field("sleeps", sleeps, params);
        return builder.endObject();
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * The number of documents found by this driver.
     */
    public long documentsFound() {
        long documentsFound = 0;
        for (OperatorStatus s : completedOperators) {
            documentsFound += s.documentsFound();
        }
        for (OperatorStatus s : activeOperators) {
            documentsFound += s.documentsFound();
        }
        return documentsFound;
    }

    /**
     * The number of values loaded by this operator.
     */
    public long valuesLoaded() {
        long valuesLoaded = 0;
        for (OperatorStatus s : completedOperators) {
            valuesLoaded += s.valuesLoaded();
        }
        for (OperatorStatus s : activeOperators) {
            valuesLoaded += s.valuesLoaded();
        }
        return valuesLoaded;
    }

    public enum Status implements Writeable, ToXContentFragment {
        QUEUED,
        STARTING,
        RUNNING,
        ASYNC,
        WAITING,
        DONE;

        public static Status read(StreamInput in) throws IOException {
            return Status.valueOf(in.readString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(toString());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(toString().toLowerCase(Locale.ROOT));
        }
    }
}
