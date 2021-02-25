/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.execution;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.watcher.actions.ActionWrapperResult;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;

public class WatchExecutionSnapshot implements Writeable, ToXContentObject {

    private final String watchId;
    private final String watchRecordId;
    private final ZonedDateTime triggeredTime;
    private final ZonedDateTime executionTime;
    private final ExecutionPhase phase;
    private final StackTraceElement[] executionStackTrace;
    private String[] executedActions;

    public WatchExecutionSnapshot(WatchExecutionContext context, StackTraceElement[] executionStackTrace) {
        watchId = context.id().watchId();
        watchRecordId = context.id().value();
        triggeredTime = context.triggerEvent().triggeredTime();
        executionTime = context.executionTime();
        phase = context.executionPhase();
        if (phase == ExecutionPhase.ACTIONS) {
            Map<String, ActionWrapperResult> actionResults = context.actionsResults();
            executedActions = new String[actionResults.size()];
            int i = 0;
            for (ActionWrapperResult actionResult : actionResults.values()) {
                executedActions[i++] = actionResult.id();
            }
        }
        this.executionStackTrace = executionStackTrace;
    }

    public WatchExecutionSnapshot(StreamInput in) throws IOException {
        watchId = in.readString();
        watchRecordId = in.readString();
        triggeredTime = Instant.ofEpochMilli(in.readVLong()).atZone(ZoneOffset.UTC);
        executionTime = Instant.ofEpochMilli(in.readVLong()).atZone(ZoneOffset.UTC);
        phase = ExecutionPhase.resolve(in.readString());
        int size = in.readVInt();
        executionStackTrace = new StackTraceElement[size];
        for (int i = 0; i < size; i++) {
            String declaringClass = in.readString();
            String methodName = in.readString();
            String fileName = in.readOptionalString();
            int lineNumber = in.readInt();
            executionStackTrace[i] = new StackTraceElement(declaringClass, methodName, fileName, lineNumber);
        }
    }

    public String watchId() {
        return watchId;
    }

    public String watchRecordId() {
        return watchRecordId;
    }

    public ZonedDateTime triggeredTime() {
        return triggeredTime;
    }

    public ZonedDateTime executionTime() {
        return executionTime;
    }

    public ExecutionPhase executionPhase() {
        return phase;
    }

    public StackTraceElement[] executionStackTrace() {
        return executionStackTrace;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(watchId);
        out.writeString(watchRecordId);
        out.writeVLong(triggeredTime.toInstant().toEpochMilli());
        out.writeVLong(executionTime.toInstant().toEpochMilli());
        out.writeString(phase.id());
        out.writeVInt(executionStackTrace.length);
        for (StackTraceElement element : executionStackTrace) {
            out.writeString(element.getClassName());
            out.writeString(element.getMethodName());
            out.writeOptionalString(element.getFileName());
            out.writeInt(element.getLineNumber());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("watch_id", watchId);
        builder.field("watch_record_id", watchRecordId);
        builder.timeField("triggered_time", triggeredTime);
        builder.timeField("execution_time", executionTime);
        builder.field("execution_phase", phase);
        if (executedActions != null) {
            builder.startArray("executed_actions");
            for (String executedAction : executedActions) {
                builder.value(executedAction);
            }
            builder.endArray();
        }
        if (params.paramAsBoolean("emit_stacktraces", false)) {
            builder.startArray("stack_trace");
            for (StackTraceElement element : executionStackTrace) {
                builder.value(element.toString());
            }
            builder.endArray();
        }
        builder.endObject();
        return builder;
    }
}
