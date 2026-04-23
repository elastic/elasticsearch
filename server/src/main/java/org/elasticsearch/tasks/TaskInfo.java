/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ObjectParserHelper;
import org.elasticsearch.core.Nullable;
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
 *
 * @param startTime Start time in millis since the epoch. In some API responses, this may be the start time of a task whose work this task
 *     is continuing.
 * @param runningTimeNanos Approximate running time in nanos (relative to the start time given by {@link #startTime}: see notes there)
 * @param originalTaskId If this task is continuing the work of another task on a node that was shut down, this is the ID of the original
 *     task; otherwise, this will be the same as taskId
 * @param originalStartTimeMillis The {@link #startTime} of the task given by {@link #originalTaskId}
 */
public record TaskInfo(
    TaskId taskId,
    String type,
    String node,
    String action,
    String description,
    Task.Status status,
    long startTime,
    long runningTimeNanos,
    boolean cancellable,
    boolean cancelled,
    TaskId parentTaskId,
    Map<String, String> headers,
    TaskId originalTaskId,
    long originalStartTimeMillis
) implements Writeable, ToXContentFragment {

    private static final TransportVersion INCLUDE_ORIGINAL_TASK = TransportVersion.fromName("task_info_include_original_task");
    static final String INCLUDE_CANCELLED_PARAM = "include_cancelled";

    public TaskInfo {
        assert cancellable || cancelled == false : "uncancellable task cannot be cancelled";
    }

    /// Constructor for a task which is not continuing the work of another task, so `originalTaskId == taskId` and
    /// `originalStartTimeMillis == startTime`.
    public TaskInfo(
        TaskId taskId,
        String type,
        String node,
        String action,
        String description,
        Task.Status status,
        long startTime,
        long runningTimeNanos,
        boolean cancellable,
        boolean cancelled,
        TaskId parentTaskId,
        Map<String, String> headers
    ) {
        this(
            taskId,
            type,
            node,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            taskId,
            startTime
        );
    }

    /**
     * Read from a stream.
     */
    public static TaskInfo from(StreamInput in) throws IOException {
        TaskId taskId = TaskId.readFromStream(in);
        String type = in.readString();
        String node = in.readString();
        String action = in.readString();
        String description = in.readOptionalString();
        Task.Status status = in.readOptionalNamedWriteable(Task.Status.class);
        long startTime = in.readLong();
        long runningTimeNanos = in.readLong();
        boolean cancellable = in.readBoolean();
        boolean cancelled = in.readBoolean();
        TaskId parentTaskId = TaskId.readFromStream(in);
        Map<String, String> headers = in.readMap(StreamInput::readString);
        TaskId originalTaskId = in.getTransportVersion().supports(INCLUDE_ORIGINAL_TASK) ? TaskId.readFromStream(in) : taskId;
        long originalStartTimeMillis = in.getTransportVersion().supports(INCLUDE_ORIGINAL_TASK) ? in.readLong() : startTime;
        return new TaskInfo(
            taskId,
            type,
            node,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            originalTaskId,
            originalStartTimeMillis
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        taskId.writeTo(out);
        out.writeString(type);
        out.writeString(node);
        out.writeString(action);
        out.writeOptionalString(description);
        out.writeOptionalNamedWriteable(status);
        out.writeLong(startTime);
        out.writeLong(runningTimeNanos);
        out.writeBoolean(cancellable);
        out.writeBoolean(cancelled);
        parentTaskId.writeTo(out);
        out.writeMap(headers, StreamOutput::writeString);
        if (out.getTransportVersion().supports(INCLUDE_ORIGINAL_TASK)) {
            originalTaskId.writeTo(out);
            out.writeLong(originalStartTimeMillis);
        }
    }

    public long id() {
        return taskId.getId();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("node", node);
        builder.field("id", taskId.getId());
        if (!originalTaskId.equals(taskId)) {
            builder.field("original_task_id", originalTaskId.toString());
        }
        builder.field("type", type);
        builder.field("action", action);
        if (status != null) {
            builder.field("status", status, params);
        }
        if (description != null) {
            builder.field("description", description);
        }
        builder.timestampFieldsFromUnixEpochMillis("start_time_in_millis", "start_time", startTime);
        if (builder.humanReadable()) {
            builder.field("running_time", new TimeValue(runningTimeNanos, TimeUnit.NANOSECONDS).toString());
        }
        builder.field("running_time_in_nanos", runningTimeNanos);
        if (!originalTaskId.equals(taskId)) {
            builder.timestampFieldsFromUnixEpochMillis("original_start_time_in_millis", "original_start_time", originalStartTimeMillis);
        }
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
        String node = (String) a[i++];
        TaskId id = new TaskId(node, (Long) a[i++]);
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
        String originalTaskIdString = (String) a[i++];
        Long optionalOriginalStartTimeMillis = (Long) a[i++];

        if (headers == null) {
            // This might happen if we are reading an old version of task info
            headers = Collections.emptyMap();
        }
        RawTaskStatus status = statusBytes == null ? null : new RawTaskStatus(statusBytes);
        TaskId parentTaskId = parentTaskIdString == null ? TaskId.EMPTY_TASK_ID : new TaskId(parentTaskIdString);
        TaskId originalTaskId = originalTaskIdString == null ? id : new TaskId(originalTaskIdString);
        long originalStartTimeMillis = parseOriginalStartTimeMillis(optionalOriginalStartTimeMillis, startTime, originalTaskIdString);
        return new TaskInfo(
            id,
            type,
            node,
            action,
            description,
            status,
            startTime,
            runningTimeNanos,
            cancellable,
            cancelled,
            parentTaskId,
            headers,
            originalTaskId,
            originalStartTimeMillis
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
        PARSER.declareString(optionalConstructorArg(), new ParseField("original_task_id"));
        PARSER.declareLong(optionalConstructorArg(), new ParseField("original_start_time_in_millis"));
    }

    private static long parseOriginalStartTimeMillis(
        @Nullable Long optionalOriginalStartTimeMillis,
        long startTime,
        @Nullable String originalTaskIdString
    ) {
        if (originalTaskIdString == null) {
            if (optionalOriginalStartTimeMillis == null) {
                // The regular case: neither original_task_id nor original_start_time_in_millis is set.
                // We default originalStartTimeMillis to startTime.
                return startTime;
            } else {
                throw new IllegalArgumentException("Task info must not set original_start_time_in_millis if original_task_id is not set");
            }
        } else {
            if (optionalOriginalStartTimeMillis != null) {
                // This task is continuing the work of another one: both original_task_id and original_start_time_in_millis are set.
                return optionalOriginalStartTimeMillis;
            } else {
                throw new IllegalArgumentException("Task info must set original_start_time_in_millis if original_task_id is set");
            }
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
