/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Information about a currently running task.
 * <p>
 * Tasks are used for communication with transport actions. As a result, they can contain callback
 * references as well as mutable state. That makes it impractical to send tasks over transport channels
 * and use in APIs. Instead, immutable and writeable TaskInfo objects are used to represent
 * snapshot information about currently running tasks.
 */
public record TaskInfo(
    TaskId taskId,
    String type,
    String action,
    String description,
    Task.Status status,
    long startTime,
    long runningTimeNanos,
    boolean cancellable,
    boolean cancelled,
    TaskId parentTaskId,
    Map<String, String> headers
) implements Writeable, ToXContentFragment {

    static final String INCLUDE_CANCELLED_PARAM = "include_cancelled";

    public TaskInfo {
        assert cancellable || cancelled == false : "uncancellable task cannot be cancelled";
    }

    /**
     * Read from a stream.
     */
    public static TaskInfo from(StreamInput in) throws IOException {
        return new TaskInfo(
            TaskId.readFromStream(in),
            in.readString(),
            in.readString(),
            in.readOptionalString(),
            in.readOptionalNamedWriteable(Task.Status.class),
            in.readLong(),
            in.readLong(),
            in.readBoolean(),
            in.readBoolean(),
            TaskId.readFromStream(in),
            in.readMap(StreamInput::readString)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskId.writeTo(out);
        out.writeString(type);
        out.writeString(action);
        out.writeOptionalString(description);
        out.writeOptionalNamedWriteable(status);
        out.writeLong(startTime);
        out.writeLong(runningTimeNanos);
        out.writeBoolean(cancellable);
        out.writeBoolean(cancelled);
        parentTaskId.writeTo(out);
        out.writeMap(headers, StreamOutput::writeString, StreamOutput::writeString);
    }

    public long id() {
        return taskId.getId();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node", taskId.getNodeId());
        builder.field("id", taskId.getId());
        builder.field("type", type);
        builder.field("action", action);
        if (status != null) {
            builder.field("status", status, params);
        }
        if (description != null) {
            builder.field("description", description);
        }
        builder.timeField("start_time_in_millis", "start_time", startTime);
        if (builder.humanReadable()) {
            builder.field("running_time", new TimeValue(runningTimeNanos, TimeUnit.NANOSECONDS).toString());
        }
        builder.field("running_time_in_nanos", runningTimeNanos);
        builder.field("cancellable", cancellable);

        if (params.paramAsBoolean(INCLUDE_CANCELLED_PARAM, true) && cancellable) {
            // don't record this on entries in the tasks index, since we can't add this field to the mapping dynamically and it's not
            // important for completed tasks anyway
            builder.field("cancelled", cancelled);
        }
        if (parentTaskId.isSet()) {
            builder.field("parent_task_id", parentTaskId.toString());
        }
        builder.startObject("headers");
        for (Map.Entry<String, String> attribute : headers.entrySet()) {
            builder.field(attribute.getKey(), attribute.getValue());
        }
        builder.endObject();
        return builder;
    }

    public static TaskInfo fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public static final ConstructingObjectParser<TaskInfo, Void> PARSER = new ConstructingObjectParser<>("task_info", true, a -> {
        int i = 0;
        TaskId id = new TaskId((String) a[i++], (Long) a[i++]);
        String type = (String) a[i++];
        String action = (String) a[i++];
        String description = (String) a[i++];
        BytesReference statusBytes = (BytesReference) a[i++];
        long startTime = (Long) a[i++];
        long runningTimeNanos = (Long) a[i++];
        boolean cancellable = (Boolean) a[i++];
        boolean cancelled = a[i++] == Boolean.TRUE;
        String parentTaskIdString = (String) a[i++];
        @SuppressWarnings("unchecked")
        Map<String, String> headers = (Map<String, String>) a[i++];
        if (headers == null) {
            // This might happen if we are reading an old version of task info
            headers = Collections.emptyMap();
        }
        RawTaskStatus status = statusBytes == null ? null : new RawTaskStatus(statusBytes);
        TaskId parentTaskId = parentTaskIdString == null ? TaskId.EMPTY_TASK_ID : new TaskId(parentTaskIdString);
        return new TaskInfo(
            id,
            type,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers
        );
    });

    static {
        // Note for the future: this has to be backwards and forwards compatible with all changes to the task storage format
        PARSER.declareString(constructorArg(), new ParseField("node"));
        PARSER.declareLong(constructorArg(), new ParseField("id"));
        PARSER.declareString(constructorArg(), new ParseField("type"));
        PARSER.declareString(constructorArg(), new ParseField("action"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("description"));
        ObjectParserHelper.declareRawObject(PARSER, optionalConstructorArg(), new ParseField("status"));
        PARSER.declareLong(constructorArg(), new ParseField("start_time_in_millis"));
        PARSER.declareLong(constructorArg(), new ParseField("running_time_in_nanos"));
        PARSER.declareBoolean(constructorArg(), new ParseField("cancellable"));
        PARSER.declareBoolean(optionalConstructorArg(), new ParseField("cancelled"));
        PARSER.declareString(optionalConstructorArg(), new ParseField("parent_task_id"));
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> p.mapStrings(), new ParseField("headers"));
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
