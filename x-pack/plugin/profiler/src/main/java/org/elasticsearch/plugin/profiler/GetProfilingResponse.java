/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.plugin.profiler;

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

import static org.elasticsearch.rest.RestStatus.OK;

//TODO: Doc comment
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
        // TODO: Map this
        // this.stackTraces = in.readBoolean() ? in.readMap(StreamInput::readString, );
        this.stackTraces = null;
        this.stackFrames = null;
        this.executables = null;
        this.stackTraceEvents = null;
        this.totalFrames = 0;
        this.error = in.readBoolean() ? in.readException() : null;
        // TODO: Remove me
        // this.searchResponse = in.readOptionalWriteable(SearchResponse::new);
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
        // TODO: Map everything
        if (stackTraces != null) {
            out.writeBoolean(true);
            // out.writeMap(stackTraces);
        } else {
            out.writeBoolean(false);
        }
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
}
