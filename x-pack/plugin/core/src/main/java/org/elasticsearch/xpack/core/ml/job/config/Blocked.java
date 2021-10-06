/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class Blocked implements ToXContentObject, Writeable {

    public enum Reason {
        NONE, DELETE, RESET, REVERT;

        public static Reason fromString(String value) {
            return Reason.valueOf(value.toUpperCase(Locale.ROOT));
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public static final ParseField REASON = new ParseField("reason");
    public static final ParseField TASK_ID = new ParseField("task_id");

    public static final ConstructingObjectParser<Blocked, Void> LENIENT_PARSER = createParser(true);
    public static final ConstructingObjectParser<Blocked, Void> STRICT_PARSER = createParser(false);

    private static ConstructingObjectParser<Blocked, Void> createParser(boolean ignoreUnknownFields) {
        ConstructingObjectParser<Blocked, Void> parser = new ConstructingObjectParser<>("blocked", ignoreUnknownFields,
            a -> new Blocked((Reason) a[0], (TaskId) a[1]));
        parser.declareString(ConstructingObjectParser.constructorArg(), Reason::fromString, REASON);
        parser.declareString(ConstructingObjectParser.optionalConstructorArg(), TaskId::new, TASK_ID);
        return parser;
    }

    private final Reason reason;

    @Nullable
    private final TaskId taskId;

    public Blocked(Reason reason, @Nullable TaskId taskId) {
        this.reason = Objects.requireNonNull(reason);
        this.taskId = taskId;
    }

    public Blocked(StreamInput in) throws IOException {
        this.reason = in.readEnum(Reason.class);
        this.taskId = in.readOptionalWriteable(TaskId::readFromStream);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(reason);
        out.writeOptionalWriteable(taskId);
    }

    public Reason getReason() {
        return reason;
    }

    @Nullable
    public TaskId getTaskId() {
        return taskId;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(REASON.getPreferredName(), reason);
        if (taskId != null) {
            builder.field(TASK_ID.getPreferredName(), taskId.toString());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(reason, taskId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Blocked that = (Blocked) o;
        return Objects.equals(reason, that.reason) && Objects.equals(taskId, that.taskId);
    }

    public static Blocked none() {
        return new Blocked(Reason.NONE, null);
    }
}
