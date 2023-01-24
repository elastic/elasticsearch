/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiler;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.rest.RestStatus.OK;

public class GetProfilingResponse extends ActionResponse implements StatusToXContentObject {
    @Nullable
    private final Map<String, StackTrace> stackTraces;
    @Nullable
    private final Map<String, StackFrame> stackFrames;
    @Nullable
    private final Map<String, String> executables;
    @Nullable
    private final Map<String, Integer> stackTraceEvents;
    private final int totalFrames;
    @Nullable
    private final Exception error;

    public GetProfilingResponse(StreamInput in) throws IOException {
        this.stackTraces = in.readBoolean()
            ? in.readMap(
                StreamInput::readString,
                i -> new StackTrace(i.readIntArray(), i.readStringArray(), i.readStringArray(), i.readIntArray())
            )
            : null;
        this.stackFrames = in.readBoolean()
            ? in.readMap(
                StreamInput::readString,
                i -> new StackFrame(
                    i.readList(StreamInput::readString),
                    i.readList(StreamInput::readString),
                    i.readList(StreamInput::readInt),
                    i.readList(StreamInput::readInt),
                    i.readList(StreamInput::readInt)
                )
            )
            : null;
        this.executables = in.readBoolean() ? in.readMap(StreamInput::readString, StreamInput::readString) : null;
        this.stackTraceEvents = in.readBoolean() ? in.readMap(StreamInput::readString, StreamInput::readInt) : null;
        this.totalFrames = in.readInt();
        this.error = in.readBoolean() ? in.readException() : null;
    }

    public GetProfilingResponse(
        Map<String, StackTrace> stackTraces,
        Map<String, StackFrame> stackFrames,
        Map<String, String> executables,
        Map<String, Integer> stackTraceEvents,
        int totalFrames
    ) {
        this(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames, null);
    }

    public GetProfilingResponse(Exception error) {
        this(null, null, null, null, 0, error);
    }

    private GetProfilingResponse(
        Map<String, StackTrace> stackTraces,
        Map<String, StackFrame> stackFrames,
        Map<String, String> executables,
        Map<String, Integer> stackTraceEvents,
        int totalFrames,
        Exception error
    ) {
        this.stackTraces = stackTraces;
        this.stackFrames = stackFrames;
        this.executables = executables;
        this.stackTraceEvents = stackTraceEvents;
        this.totalFrames = totalFrames;
        this.error = error;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (stackTraces != null) {
            out.writeBoolean(true);
            out.writeMap(stackTraces, StreamOutput::writeString, (o, v) -> {
                o.writeIntArray(v.addressOrLines);
                o.writeStringArray(v.fileIds);
                o.writeStringArray(v.frameIds);
                o.writeIntArray(v.typeIds);
            });
        } else {
            out.writeBoolean(false);
        }
        if (stackFrames != null) {
            out.writeBoolean(true);
            out.writeMap(stackFrames, StreamOutput::writeString, (o, v) -> {
                o.writeCollection(v.fileName, StreamOutput::writeString);
                o.writeCollection(v.functionName, StreamOutput::writeString);
                o.writeCollection(v.functionOffset, StreamOutput::writeInt);
                o.writeCollection(v.lineNumber, StreamOutput::writeInt);
                o.writeCollection(v.sourceType, StreamOutput::writeInt);
            });
        } else {
            out.writeBoolean(false);
        }
        if (executables != null) {
            out.writeBoolean(true);
            out.writeMap(executables, StreamOutput::writeString, StreamOutput::writeString);
        } else {
            out.writeBoolean(false);
        }
        if (stackTraceEvents != null) {
            out.writeBoolean(true);
            out.writeMap(stackTraceEvents, StreamOutput::writeString, StreamOutput::writeInt);
        } else {
            out.writeBoolean(false);
        }
        out.writeInt(totalFrames);
        if (error != null) {
            out.writeBoolean(true);
            out.writeException(error);
        } else {
            out.writeBoolean(false);
        }
    }

    @Override
    public RestStatus status() {
        return error != null ? ExceptionsHelper.status(ExceptionsHelper.unwrapCause(error)) : OK;
    }

    public Map<String, StackTrace> getStackTraces() {
        return stackTraces;
    }

    public Map<String, StackFrame> getStackFrames() {
        return stackFrames;
    }

    public Map<String, String> getExecutables() {
        return executables;
    }

    public Map<String, Integer> getStackTraceEvents() {
        return stackTraceEvents;
    }

    public int getTotalFrames() {
        return totalFrames;
    }

    public Exception getError() {
        return error;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (stackTraces != null) {
            builder.startObject("stack_traces");
            builder.mapContents(stackTraces);
            builder.endObject();
        }
        if (stackFrames != null) {
            builder.startObject("stack_frames");
            builder.mapContents(stackFrames);
            builder.endObject();
        }
        if (executables != null) {
            builder.startObject("executables");
            builder.mapContents(executables);
            builder.endObject();
        }
        if (stackTraceEvents != null) {
            builder.startObject("stack_trace_events");
            builder.mapContents(stackTraceEvents);
            builder.endObject();
        }
        builder.field("total_frames", totalFrames);
        if (error != null) {
            builder.startObject("error");
            ElasticsearchException.generateThrowableXContent(builder, params, error);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetProfilingResponse response = (GetProfilingResponse) o;
        return totalFrames == response.totalFrames
            && Objects.equals(stackTraces, response.stackTraces)
            && Objects.equals(stackFrames, response.stackFrames)
            && Objects.equals(executables, response.executables)
            && Objects.equals(stackTraceEvents, response.stackTraceEvents)
            && Objects.equals(error, response.error);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames, error);
    }
}
