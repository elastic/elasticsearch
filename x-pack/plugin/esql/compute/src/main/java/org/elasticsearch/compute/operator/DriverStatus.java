/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

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

public class DriverStatus implements Task.Status {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Task.Status.class,
        "driver",
        DriverStatus::new
    );

    private final Status status;
    private final List<OperatorStatus> activeOperators;

    DriverStatus(Status status, List<Operator> activeOperators) {
        this.status = status;
        this.activeOperators = activeOperators.stream().map(o -> new OperatorStatus(o.toString(), o.status())).toList();
    }

    private DriverStatus(StreamInput in) throws IOException {
        status = Status.valueOf(in.readString());
        activeOperators = in.readImmutableList(OperatorStatus::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(status.toString());
        out.writeList(activeOperators);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
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
        builder.startArray("active_operators");
        for (OperatorStatus active : activeOperators) {
            builder.value(active);
        }
        builder.endArray();
        return builder.endObject();
    }

    public static class OperatorStatus implements Writeable, ToXContentObject {
        private final String operator;
        @Nullable
        private final Operator.Status status;

        private OperatorStatus(String operator, Operator.Status status) {
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
