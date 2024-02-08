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
import org.elasticsearch.core.Nullable;
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
     * When this status was generated.
     */
    private final long lastUpdated;
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
        long lastUpdated,
        Status status,
        List<OperatorStatus> completedOperators,
        List<OperatorStatus> activeOperators
    ) {
        this.sessionId = sessionId;
        this.lastUpdated = lastUpdated;
        this.status = status;
        this.completedOperators = completedOperators;
        this.activeOperators = activeOperators;
    }

    public DriverStatus(StreamInput in) throws IOException {
        this.sessionId = in.readString();
        this.lastUpdated = in.readLong();
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
        out.writeLong(lastUpdated);
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
     * When this status was generated.
     */
    public long lastUpdated() {
        return lastUpdated;
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
        builder.field("last_updated", DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER.formatMillis(lastUpdated));
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
            && lastUpdated == that.lastUpdated
            && status == that.status
            && completedOperators.equals(that.completedOperators)
            && activeOperators.equals(that.activeOperators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, lastUpdated, status, completedOperators, activeOperators);
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
            out.writeOptionalNamedWriteable(status);
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
