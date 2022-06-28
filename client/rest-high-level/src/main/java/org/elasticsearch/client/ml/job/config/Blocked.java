/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ml.job.config;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

public class Blocked implements ToXContentObject {

    public enum Reason {
        NONE,
        DELETE,
        RESET,
        REVERT;

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

    public static final ConstructingObjectParser<Blocked, Void> PARSER = new ConstructingObjectParser<>(
        "blocked",
        true,
        a -> new Blocked((Reason) a[0], (TaskId) a[1])
    );

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Reason::fromString, REASON);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TaskId::new, TASK_ID);
    }

    private final Reason reason;

    @Nullable
    private final TaskId taskId;

    public Blocked(Reason reason, @Nullable TaskId taskId) {
        this.reason = Objects.requireNonNull(reason);
        this.taskId = taskId;
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
}
