/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Objects;

public class DriverStatus implements Task.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Task.Status.class,
        "driver",
        DriverStatus::new
    );

    private final String sessionId;
    private final Status status;
    private final List<OperatorStatus> activeOperators;

    DriverStatus(String sessionId, Status status, List<OperatorStatus> activeOperators) {
        this.sessionId = sessionId;
        this.status = status;
        this.activeOperators = activeOperators;
    }

    DriverStatus(StreamInput in) throws IOException {
        this(in.readString(), Status.valueOf(in.readString()), in.readImmutableList(OperatorStatus::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(sessionId);
        out.writeString(status.toString());
        out.writeList(activeOperators);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    public String sessionId() {
        return sessionId;
    }

    public Status status() {
        return status;
    }

    public List<OperatorStatus> activeOperators() {
        return activeOperators;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("sessionId", sessionId);
        builder.field("status", status.toString().toLowerCase(Locale.ROOT));
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
        return sessionId.equals(that.sessionId) && status == that.status && activeOperators.equals(that.activeOperators);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sessionId, status, activeOperators);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public static class OperatorStatus implements Writeable, ToXContentObject {
        private final String operator;
        @Nullable
        private final Operator.Status status;

        OperatorStatus(String operator, Operator.Status status) {
            this.operator = operator;
            this.status = status;
        }

        private OperatorStatus(StreamInput in) throws IOException {
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
        DONE;

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.value(toString().toLowerCase(Locale.ROOT));
        }
    }
}
