/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiler;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

public class GetProfilingResponse extends ActionResponse implements ChunkedToXContentObject {
    @Nullable
    private final Map<String, StackTrace> stackTraces;
    @Nullable
    private final Map<String, StackFrame> stackFrames;
    @Nullable
    private final Map<String, String> executables;
    @Nullable
    private final Map<String, Integer> stackTraceEvents;
    private final int totalFrames;
    private final double samplingRate;

    public GetProfilingResponse(StreamInput in) throws IOException {
        this.stackTraces = in.readBoolean()
            ? in.readMap(
                StreamInput::readString,
                i -> new StackTrace(
                    i.readList(StreamInput::readInt),
                    i.readList(StreamInput::readString),
                    i.readList(StreamInput::readString),
                    i.readList(StreamInput::readInt)
                )
            )
            : null;
        this.stackFrames = in.readBoolean()
            ? in.readMap(
                StreamInput::readString,
                i -> new StackFrame(
                    i.readList(StreamInput::readString),
                    i.readList(StreamInput::readString),
                    i.readList(StreamInput::readInt),
                    i.readList(StreamInput::readInt)
                )
            )
            : null;
        this.executables = in.readBoolean() ? in.readMap(StreamInput::readString, StreamInput::readString) : null;
        this.stackTraceEvents = in.readBoolean() ? in.readMap(StreamInput::readString, StreamInput::readInt) : null;
        this.totalFrames = in.readInt();
        this.samplingRate = in.readDouble();
    }

    public GetProfilingResponse(
        Map<String, StackTrace> stackTraces,
        Map<String, StackFrame> stackFrames,
        Map<String, String> executables,
        Map<String, Integer> stackTraceEvents,
        int totalFrames,
        double samplingRate
    ) {
        this.stackTraces = stackTraces;
        this.stackFrames = stackFrames;
        this.executables = executables;
        this.stackTraceEvents = stackTraceEvents;
        this.totalFrames = totalFrames;
        this.samplingRate = samplingRate;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (stackTraces != null) {
            out.writeBoolean(true);
            out.writeMap(stackTraces, StreamOutput::writeString, (o, v) -> {
                o.writeCollection(v.addressOrLines, StreamOutput::writeInt);
                o.writeCollection(v.fileIds, StreamOutput::writeString);
                o.writeCollection(v.frameIds, StreamOutput::writeString);
                o.writeCollection(v.typeIds, StreamOutput::writeInt);
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
        out.writeDouble(samplingRate);
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

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            optional("stack_traces", stackTraces, ChunkedToXContentHelper::xContentValuesMap),
            optional("stack_frames", stackFrames, ChunkedToXContentHelper::xContentValuesMap),
            optional("executables", executables, ChunkedToXContentHelper::map),
            optional("stack_trace_events", stackTraceEvents, ChunkedToXContentHelper::map),
            Iterators.single((b, p) -> b.field("total_frames", totalFrames)),
            Iterators.single((b, p) -> b.field("sampling_rate", samplingRate)),
            ChunkedToXContentHelper.endObject()
        );
    }

    private <T> Iterator<? extends ToXContent> optional(
        String name,
        Map<String, T> values,
        BiFunction<String, Map<String, T>, Iterator<? extends ToXContent>> supplier
    ) {
        return (values != null) ? supplier.apply(name, values) : Collections.emptyIterator();
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
            && samplingRate == response.samplingRate
            && Objects.equals(stackTraces, response.stackTraces)
            && Objects.equals(stackFrames, response.stackFrames)
            && Objects.equals(executables, response.executables)
            && Objects.equals(stackTraceEvents, response.stackTraceEvents);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames, samplingRate);
    }
}
