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
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Flow;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingUnifiedChatCompletionResults(Flow.Publisher<? extends ChunkedToXContent> publisher)
    implements
        InferenceServiceResults {

    public static final String NAME = "chat_completion_chunk";
    public static final String MODEL_FIELD = "model";
    public static final String OBJECT_FIELD = "object";
    public static final String USAGE_FIELD = "usage";
    public static final String INDEX_FIELD = "index";
    public static final String ID_FIELD = "id";
    public static final String FUNCTION_NAME_FIELD = "name";
    public static final String FUNCTION_ARGUMENTS_FIELD = "arguments";
    public static final String FUNCTION_FIELD = "function";
    public static final String CHOICES_FIELD = "choices";
    public static final String DELTA_FIELD = "delta";
    public static final String CONTENT_FIELD = "content";
    public static final String REFUSAL_FIELD = "refusal";
    public static final String ROLE_FIELD = "role";
    private static final String TOOL_CALLS_FIELD = "tool_calls";
    public static final String FINISH_REASON_FIELD = "finish_reason";
    public static final String COMPLETION_TOKENS_FIELD = "completion_tokens";
    public static final String TOTAL_TOKENS_FIELD = "total_tokens";
    public static final String PROMPT_TOKENS_FIELD = "prompt_tokens";
    public static final String TYPE_FIELD = "type";

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

    public record Results(Deque<ChatCompletionChunk> chunks) implements ChunkedToXContent {
        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(Iterators.flatMap(chunks.iterator(), c -> c.toXContentChunked(params)));
        }
    }

    public static class ChatCompletionChunk implements ChunkedToXContent {
        private final String id;

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

        public Usage getUsage() {
            return usage;
        }

        private final List<Choice> choices;
        private final String model;
        private final String object;
        private final ChatCompletionChunk.Usage usage;

        public ChatCompletionChunk(String id, List<Choice> choices, String model, String object, ChatCompletionChunk.Usage usage) {
            this.id = id;
            this.choices = choices;
            this.model = model;
            this.object = object;
            this.usage = usage;
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {

            Iterator<? extends ToXContent> choicesIterator = Collections.emptyIterator();
            if (choices != null) {
                choicesIterator = Iterators.concat(
                    ChunkedToXContentHelper.startArray(CHOICES_FIELD),
                    Iterators.flatMap(choices.iterator(), c -> c.toXContentChunked(params)),
                    ChunkedToXContentHelper.endArray()
                );
            }

            Iterator<? extends ToXContent> usageIterator = Collections.emptyIterator();
            if (usage != null) {
                usageIterator = Iterators.concat(
                    ChunkedToXContentHelper.startObject(USAGE_FIELD),
                    ChunkedToXContentHelper.field(COMPLETION_TOKENS_FIELD, usage.completionTokens()),
                    ChunkedToXContentHelper.field(PROMPT_TOKENS_FIELD, usage.promptTokens()),
                    ChunkedToXContentHelper.field(TOTAL_TOKENS_FIELD, usage.totalTokens()),
                    ChunkedToXContentHelper.endObject()
                );
            }

            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                ChunkedToXContentHelper.field(ID_FIELD, id),
                choicesIterator,
                ChunkedToXContentHelper.field(MODEL_FIELD, model),
                ChunkedToXContentHelper.field(OBJECT_FIELD, object),
                usageIterator,
                ChunkedToXContentHelper.endObject()
            );
        }

        public record Choice(ChatCompletionChunk.Choice.Delta delta, String finishReason, int index) {

            /*
              choices: Array<{
                delta: { ... };
                finish_reason: string | null;
                index: number;
              }>;
             */
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                return Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    delta.toXContentChunked(params),
                    ChunkedToXContentHelper.optionalField(FINISH_REASON_FIELD, finishReason),
                    ChunkedToXContentHelper.field(INDEX_FIELD, index),
                    ChunkedToXContentHelper.endObject()
                );
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

                /*
                delta: {
                    content?: string | null;
                    refusal?: string | null;
                    role?: 'system' | 'user' | 'assistant' | 'tool';
                    tool_calls?: Array<{ ... }>;
                };
                */
                public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                    var xContent = Iterators.concat(
                        ChunkedToXContentHelper.startObject(DELTA_FIELD),
                        ChunkedToXContentHelper.optionalField(CONTENT_FIELD, content),
                        ChunkedToXContentHelper.optionalField(REFUSAL_FIELD, refusal),
                        ChunkedToXContentHelper.optionalField(ROLE_FIELD, role)
                    );

                    if (toolCalls != null && toolCalls.isEmpty() == false) {
                        xContent = Iterators.concat(
                            xContent,
                            ChunkedToXContentHelper.startArray(TOOL_CALLS_FIELD),
                            Iterators.flatMap(toolCalls.iterator(), t -> t.toXContentChunked(params)),
                            ChunkedToXContentHelper.endArray()
                        );
                    }
                    xContent = Iterators.concat(xContent, ChunkedToXContentHelper.endObject());
                    return xContent;

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

                    /*
                        index: number;
                        id?: string;
                        function?: {
                          arguments?: string;
                          name?: string;
                        };
                        type?: 'function';
                     */
                    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                        var content = Iterators.concat(
                            ChunkedToXContentHelper.startObject(),
                            ChunkedToXContentHelper.field(INDEX_FIELD, index),
                            ChunkedToXContentHelper.optionalField(ID_FIELD, id)
                        );

                        if (function != null) {
                            content = Iterators.concat(
                                content,
                                ChunkedToXContentHelper.startObject(FUNCTION_FIELD),
                                ChunkedToXContentHelper.optionalField(FUNCTION_ARGUMENTS_FIELD, function.getArguments()),
                                ChunkedToXContentHelper.optionalField(FUNCTION_NAME_FIELD, function.getName()),
                                ChunkedToXContentHelper.endObject()
                            );
                        }

                        content = Iterators.concat(
                            content,
                            ChunkedToXContentHelper.field(TYPE_FIELD, type),
                            ChunkedToXContentHelper.endObject()
                        );
                        return content;
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
