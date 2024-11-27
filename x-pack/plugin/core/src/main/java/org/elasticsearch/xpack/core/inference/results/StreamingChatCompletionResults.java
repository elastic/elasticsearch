/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResults.COMPLETION;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingChatCompletionResults(Flow.Publisher<? extends ChunkedToXContent> publisher) implements InferenceServiceResults {

    @Override
    public boolean isStreaming() {
        return true;
    }

    @Override
    public List<? extends InferenceResults> transformToCoordinationFormat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public List<? extends InferenceResults> transformToLegacyFormat() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Map<String, Object> asMap() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public String getWriteableName() {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Not implemented");
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public record Results(Deque<Result> results) implements ChunkedToXContent {
        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.startArray(COMPLETION),
                Iterators.flatMap(results.iterator(), d -> d.toXContentChunked(params)),
                ChunkedToXContentHelper.endArray(),
                ChunkedToXContentHelper.endObject()
            );
        }
    }

    public record Result(String delta, String refusal, List<ToolCall> toolCalls) implements ChunkedToXContent {

        private static final String RESULT = "delta";
        private static final String REFUSAL = "refusal";
        private static final String TOOL_CALLS = "tool_calls";

        public Result(String delta) {
            this(delta, "", List.of());
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.field(RESULT, delta),
                ChunkedToXContentHelper.field(REFUSAL, refusal),
                ChunkedToXContentHelper.startArray(TOOL_CALLS),
                Iterators.flatMap(toolCalls.iterator(), t -> t.toXContentChunked(params)),
                ChunkedToXContentHelper.endArray(),
                ChunkedToXContentHelper.endObject()
            );
        }
    }

    public static class ToolCall implements ChunkedToXContent {
        private final int index;
        private final String id;
        private final String functionName;
        private final String functionArguments;

        public ToolCall(int index, String id, String functionName, String functionArguments) {
            this.index = index;
            this.id = id;
            this.functionName = functionName;
            this.functionArguments = functionArguments;
        }

        public int getIndex() {
            return index;
        }

        public String getId() {
            return id;
        }

        public String getFunctionName() {
            return functionName;
        }

        public String getFunctionArguments() {
            return functionArguments;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ToolCall toolCall = (ToolCall) o;
            return index == toolCall.index
                && Objects.equals(id, toolCall.id)
                && Objects.equals(functionName, toolCall.functionName)
                && Objects.equals(functionArguments, toolCall.functionArguments);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.field("index", index),
                ChunkedToXContentHelper.field("id", id),
                ChunkedToXContentHelper.field("functionName", functionName),
                ChunkedToXContentHelper.field("functionArguments", functionArguments),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public int hashCode() {
            return Objects.hash(index, id, functionName, functionArguments);
        }

        @Override
        public String toString() {
            return "ToolCall{"
                + "index="
                + index
                + ", id='"
                + id
                + '\''
                + ", functionName='"
                + functionName
                + '\''
                + ", functionArguments='"
                + functionArguments
                + '\''
                + '}';
        }
    }
}
