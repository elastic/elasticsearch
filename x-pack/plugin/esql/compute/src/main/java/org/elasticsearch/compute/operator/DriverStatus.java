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
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * {@link Task.Status} reported from a {@link Driver} to be reported by the tasks api.
 */
public class DriverStatus implements Task.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Task.Status.class,
        "driver",
        DriverStatus::new
    );

    /**
     * The session for this driver.
     */
    private final String sessionId;

    /**
     * Milliseconds since epoch when this driver started.
     */
    private final long started;

    /**
     * When this status was generated.
     */
    private final long lastUpdated;

    /**
     * Nanos this {@link Driver} has been running on the cpu. Does not
     * include async or waiting time.
     */
    private final long cpuNanos;

    /**
     * The number of times the driver has moved a single page up the
     * chain of operators as far as it'll go.
     */
    private final long iterations;

    /**
     * The state of the overall driver - queue, starting, running, finished.
     */
    private final Status status;

    /**
     * Status of each completed {@link Operator} in the driver.
     */
    private final List<OperatorStatus> completedOperators;

    /**
     * Status of each active {@link Operator} in the driver.
     */
    private final List<OperatorStatus> activeOperators;

    DriverStatus(
        String sessionId,
        long started,
        long lastUpdated,
        long cpuTime,
        long iterations,
        Status status,
        List<OperatorStatus> completedOperators,
        List<OperatorStatus> activeOperators
    ) {
        this.sessionId = sessionId;
        this.started = started;
        this.lastUpdated = lastUpdated;
        this.cpuNanos = cpuTime;
        this.iterations = iterations;
        this.status = status;
        this.completedOperators = completedOperators;
        this.activeOperators = activeOperators;
    }

    public DriverStatus(StreamInput in) throws IOException {
        this.sessionId = in.readString();
        this.started = in.getTransportVersion().onOrAfter(TransportVersions.ESQL_TIMINGS) ? in.readLong() : 0;
        this.lastUpdated = in.readLong();
        this.cpuNanos = in.getTransportVersion().onOrAfter(TransportVersions.ESQL_TIMINGS) ? in.readVLong() : 0;
        this.iterations = in.getTransportVersion().onOrAfter(TransportVersions.ESQL_TIMINGS) ? in.readVLong() : 0;
        this.status = Status.valueOf(in.readString());
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            this.completedOperators = in.readCollectionAsImmutableList(OperatorStatus::new);
        } else {
            this.completedOperators = List.of();
        }
        this.activeOperators = in.readCollectionAsImmutableList(OperatorStatus::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sessionId);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_TIMINGS)) {
            out.writeLong(started);
        }
        out.writeLong(lastUpdated);
        if (out.getTransportVersion().onOrAfter(TransportVersions.ESQL_TIMINGS)) {
            out.writeVLong(cpuNanos);
            out.writeVLong(iterations);
        }
        out.writeString(status.toString());
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_12_0)) {
            out.writeCollection(completedOperators);
        }
        out.writeCollection(activeOperators);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /**
     * The session for this driver.
     */
    public String sessionId() {
        return sessionId;
    }

    /**
     * When this {@link Driver} was started.
     */
    public long started() {
        return started;
    }

    /**
     * When this status was generated.
     */
    public long lastUpdated() {
        return lastUpdated;
    }

    /**
     * Nanos this {@link Driver} has been running on the cpu. Does not
     * include async or waiting time.
     */
    public long cpuNanos() {
        return cpuNanos;
    }

    /**
     * The number of times the driver has moved a single page up the
     * chain of operators as far as it'll go.
     */
    public long iterations() {
        return iterations;
    }

    /**
     * The state of the overall driver - queue, starting, running, finished.
     */
    public Status status() {
        return status;
    }

    /**
     * Status of each completed {@link Operator} in the driver.
     */
    public List<OperatorStatus> completedOperators() {
        return completedOperators;
    }

    /**
     * Status of each active {@link Operator} in the driver.
     */
    public List<OperatorStatus> activeOperators() {
        return activeOperators;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("sessionId", sessionId);
        builder.field("started", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(started));
        builder.field("last_updated", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(lastUpdated));
        builder.field("cpu_nanos", cpuNanos);
        if (builder.humanReadable()) {
            builder.field("cpu_time", TimeValue.timeValueNanos(cpuNanos));
        }
        builder.field("iterations", iterations);
        builder.field("status", status.toString().toLowerCase(Locale.ROOT));
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
        return builder.endObject();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DriverStatus that = (DriverStatus) o;
        return sessionId.equals(that.sessionId)
            && started == that.started
            && lastUpdated == that.lastUpdated
            && cpuNanos == that.cpuNanos
            && iterations == that.iterations
            && status == that.status
            && completedOperators.equals(that.completedOperators)
            && activeOperators.equals(that.activeOperators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, started, lastUpdated, cpuNanos, iterations, status, completedOperators, activeOperators);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Status of an {@link Operator}.
     */
    public static class OperatorStatus implements Writeable, ToXContentObject {
        /**
         * String representation of the {@link Operator}. Literally just the
         * {@link Object#toString()} of it.
         */
        private final String operator;
        /**
         * Status as reported by the {@link Operator}.
         */
        @Nullable
        private final Operator.Status status;

        public OperatorStatus(String operator, Operator.Status status) {
            this.operator = operator;
            this.status = status;
        }

        OperatorStatus(StreamInput in) throws IOException {
            operator = in.readString();
            status = in.readOptionalNamedWriteable(Operator.Status.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(operator);
            out.writeOptionalNamedWriteable(status != null && VersionedNamedWriteable.shouldSerialize(out, status) ? status : null);
        }

        public String operator() {
            return operator;
        }

        public Operator.Status status() {
            return status;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("operator", operator);
            if (status != null) {
                builder.field("status", status);
            }
            return builder.endObject();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            OperatorStatus that = (OperatorStatus) o;
            return operator.equals(that.operator) && Objects.equals(status, that.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(operator, status);
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }

    public enum Status implements ToXContentFragment {
        QUEUED,
        STARTING,
        RUNNING,
        ASYNC,
        WAITING,
        DONE;

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(toString().toLowerCase(Locale.ROOT));
        }
    }
}
