/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.profiling;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.xcontent.ToXContent;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;

public class GetStackTracesResponse extends ActionResponse implements ChunkedToXContentObject {
    @Nullable
    private final Map<String, StackTrace> stackTraces;
    @Nullable
    private final Map<String, StackFrame> stackFrames;
    @Nullable
    private final Map<String, String> executables;
    @UpdateForV9 // remove this field - it is unused in Kibana
    @Nullable
    private final Map<String, TraceEvent> stackTraceEvents;
    @UpdateForV9 // remove this field - it is unused in Kibana
    private final int totalFrames;
    private final double samplingRate;
    private final long totalSamples;

    public GetStackTracesResponse(
        Map<String, StackTrace> stackTraces,
        Map<String, StackFrame> stackFrames,
        Map<String, String> executables,
        Map<String, TraceEvent> stackTraceEvents,
        int totalFrames,
        double samplingRate,
        long totalSamples
    ) {
        this.stackTraces = stackTraces;
        this.stackFrames = stackFrames;
        this.executables = executables;
        this.stackTraceEvents = stackTraceEvents;
        this.totalFrames = totalFrames;
        this.samplingRate = samplingRate;
        this.totalSamples = totalSamples;
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
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

    public Map<String, TraceEvent> getStackTraceEvents() {
        return stackTraceEvents;
    }

    public int getTotalFrames() {
        return totalFrames;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public long getTotalSamples() {
        return totalSamples;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            optional("stack_traces", stackTraces, ChunkedToXContentHelper::xContentValuesMap),
            optional("stack_frames", stackFrames, ChunkedToXContentHelper::xContentValuesMap),
            optional("executables", executables, ChunkedToXContentHelper::map),
            // render only count for backwards-compatibility
            optional(
                "stack_trace_events",
                stackTraceEvents,
                (n, v) -> ChunkedToXContentHelper.map(n, v, entry -> (b, p) -> b.field(entry.getKey(), entry.getValue().count))
            ),
            Iterators.single((b, p) -> b.field("total_frames", totalFrames)),
            Iterators.single((b, p) -> b.field("sampling_rate", samplingRate)),
            // the following fields are intentionally not written to the XContent representation (only needed on the transport layer):
            //
            // * start
            // * end
            // * totalSamples
            ChunkedToXContentHelper.endObject()
        );
    }

    private static <T> Iterator<? extends ToXContent> optional(
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
        GetStackTracesResponse response = (GetStackTracesResponse) o;
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

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
