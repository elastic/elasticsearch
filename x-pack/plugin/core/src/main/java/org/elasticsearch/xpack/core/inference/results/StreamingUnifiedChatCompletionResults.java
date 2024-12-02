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
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Flow;

import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResults.COMPLETION;
import static org.elasticsearch.xpack.core.inference.results.ChatCompletionResults.Result.RESULT;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingUnifiedChatCompletionResults(Flow.Publisher<? extends ChunkedToXContent> publisher)
    implements
        InferenceServiceResults {

    public static final String MODEL_FIELD = "model";
    public static final String OBJECT_FIELD = "object";
    public static final String USAGE_FIELD = "usage";

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

    private static final String REFUSAL_FIELD = "refusal";
    private static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String FINISH_REASON_FIELD = "finish_reason";

    public record Result(
        String delta,
        String refusal,
        List<ToolCall> toolCalls,
        String finishReason,
        String model,
        String object,
        ChatCompletionChunk.Usage usage
    ) implements ChunkedToXContent {

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            Iterator<? extends ToXContent> toolCallsIterator = Collections.emptyIterator();
            if (toolCalls != null && toolCalls.isEmpty() == false) {
                toolCallsIterator = Iterators.concat(
                    ChunkedToXContentHelper.startArray(TOOL_CALLS_FIELD),
                    Iterators.flatMap(toolCalls.iterator(), d -> d.toXContentChunked(params)),
                    ChunkedToXContentHelper.endArray()
                );
            }

            Iterator<? extends ToXContent> usageIterator = Collections.emptyIterator();
            if (usage != null) {
                usageIterator = Iterators.concat(
                    ChunkedToXContentHelper.startObject(USAGE_FIELD),
                    ChunkedToXContentHelper.field("completion_tokens", usage.completionTokens()),
                    ChunkedToXContentHelper.field("prompt_tokens", usage.promptTokens()),
                    ChunkedToXContentHelper.field("total_tokens", usage.totalTokens()),
                    ChunkedToXContentHelper.endObject()
                );
            }

            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.field(RESULT, delta),
                ChunkedToXContentHelper.field(REFUSAL_FIELD, refusal),
                toolCallsIterator,
                ChunkedToXContentHelper.field(FINISH_REASON_FIELD, finishReason),
                ChunkedToXContentHelper.field(MODEL_FIELD, model),
                ChunkedToXContentHelper.field(OBJECT_FIELD, object),
                usageIterator,
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

    public static class ChatCompletionChunk {
        private final String id;
        private List<Choice> choices;
        private final String model;
        private final String object;
        private ChatCompletionChunk.Usage usage;

        public ChatCompletionChunk(String id, List<Choice> choices, String model, String object, ChatCompletionChunk.Usage usage) {
            this.id = id;
            this.choices = choices;
            this.model = model;
            this.object = object;
            this.usage = usage;
        }

        public ChatCompletionChunk(
            String id,
            ChatCompletionChunk.Choice[] choices,
            String model,
            String object,
            ChatCompletionChunk.Usage usage
        ) {
            this.id = id;
            this.choices = Arrays.stream(choices).toList();
            this.model = model;
            this.object = object;
            this.usage = usage;
        }

        public String getId() {
            return id;
        }

        public List<Choice> getChoices() {
            return choices;
        }

        public String getModel() {
            return model;
        }

        public String getObject() {
            return object;
        }

        public ChatCompletionChunk.Usage getUsage() {
            return usage;
        }

        public static class Choice {
            private final ChatCompletionChunk.Choice.Delta delta;
            private final String finishReason;
            private final int index;

            public Choice(ChatCompletionChunk.Choice.Delta delta, String finishReason, int index) {
                this.delta = delta;
                this.finishReason = finishReason;
                this.index = index;
            }

            public ChatCompletionChunk.Choice.Delta getDelta() {
                return delta;
            }

            public String getFinishReason() {
                return finishReason;
            }

            public int getIndex() {
                return index;
            }

            public static class Delta {
                private final String content;
                private final String refusal;
                private final String role;
                private List<ToolCall> toolCalls;

                public Delta(String content, String refusal, String role, List<ToolCall> toolCalls) {
                    this.content = content;
                    this.refusal = refusal;
                    this.role = role;
                    this.toolCalls = toolCalls;
                }

                public String getContent() {
                    return content;
                }

                public String getRefusal() {
                    return refusal;
                }

                public String getRole() {
                    return role;
                }

                public List<ToolCall> getToolCalls() {
                    return toolCalls;
                }

                public static class ToolCall {
                    private final int index;
                    private final String id;
                    public ChatCompletionChunk.Choice.Delta.ToolCall.Function function;
                    private final String type;

                    public ToolCall(int index, String id, ChatCompletionChunk.Choice.Delta.ToolCall.Function function, String type) {
                        this.index = index;
                        this.id = id;
                        this.function = function;
                        this.type = type;
                    }

                    public int getIndex() {
                        return index;
                    }

                    public String getId() {
                        return id;
                    }

                    public ChatCompletionChunk.Choice.Delta.ToolCall.Function getFunction() {
                        return function;
                    }

                    public String getType() {
                        return type;
                    }

                    public static class Function {
                        private final String arguments;
                        private final String name;

                        public Function(String arguments, String name) {
                            this.arguments = arguments;
                            this.name = name;
                        }

                        public String getArguments() {
                            return arguments;
                        }

                        public String getName() {
                            return name;
                        }
                    }
                }
            }
        }

        public record Usage(int completionTokens, int promptTokens, int totalTokens) {}
    }
}
