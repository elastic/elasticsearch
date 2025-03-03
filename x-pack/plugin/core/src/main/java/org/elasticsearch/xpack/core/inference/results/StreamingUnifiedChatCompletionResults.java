/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Flow;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeEquals;
import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeHashCode;
import static org.elasticsearch.xpack.core.inference.DequeUtils.readDeque;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingUnifiedChatCompletionResults(Flow.Publisher<? extends InferenceServiceResults.Result> publisher)
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
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        throw new UnsupportedOperationException("Not implemented");
    }

    public record Results(Deque<ChatCompletionChunk> chunks) implements InferenceServiceResults.Result {
        public static String NAME = "streaming_unified_chat_completion_results";

        public Results(StreamInput in) throws IOException {
            this(readDeque(in, ChatCompletionChunk::new));
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.flatMap(chunks.iterator(), c -> c.toXContentChunked(params));
        }

        @Override
        public String getWriteableName() {
            return NAME;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(chunks, StreamOutput::writeWriteable);
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) return false;
            Results results = (Results) o;
            return dequeEquals(chunks, results.chunks());
        }

        @Override
        public int hashCode() {
            return dequeHashCode(chunks);
        }
    }

    public record ChatCompletionChunk(String id, List<Choice> choices, String model, String object, ChatCompletionChunk.Usage usage)
        implements
            ChunkedToXContent,
            Writeable {

        private ChatCompletionChunk(StreamInput in) throws IOException {
            this(
                in.readString(),
                in.readOptionalCollectionAsList(Choice::new),
                in.readString(),
                in.readString(),
                in.readOptional(Usage::new)
            );
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunk((b, p) -> b.field(ID_FIELD, id)),
                choices != null ? ChunkedToXContentHelper.array(CHOICES_FIELD, choices.iterator(), params) : Collections.emptyIterator(),
                chunk((b, p) -> b.field(MODEL_FIELD, model).field(OBJECT_FIELD, object)),
                usage != null
                    ? chunk(
                        (b, p) -> b.startObject(USAGE_FIELD)
                            .field(COMPLETION_TOKENS_FIELD, usage.completionTokens())
                            .field(PROMPT_TOKENS_FIELD, usage.promptTokens())
                            .field(TOTAL_TOKENS_FIELD, usage.totalTokens())
                            .endObject()
                    )
                    : Collections.emptyIterator(),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeOptionalCollection(choices);
            out.writeString(model);
            out.writeString(object);
            out.writeOptionalWriteable(usage);
        }

        public record Choice(ChatCompletionChunk.Choice.Delta delta, String finishReason, int index)
            implements
                ChunkedToXContentObject,
                Writeable {

            private Choice(StreamInput in) throws IOException {
                this(new Delta(in), in.readOptionalString(), in.readInt());
            }

            /*
              choices: Array<{
                delta: { ... };
                finish_reason: string | null;
                index: number;
              }>;
             */
            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                return Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    delta.toXContentChunked(params),
                    optionalField(FINISH_REASON_FIELD, finishReason),
                    chunk((b, p) -> b.field(INDEX_FIELD, index)),
                    ChunkedToXContentHelper.endObject()
                );
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeWriteable(delta);
                out.writeOptionalString(finishReason);
                out.writeInt(index);
            }

            public record Delta(String content, String refusal, String role, List<ToolCall> toolCalls) implements Writeable {

                private Delta(StreamInput in) throws IOException {
                    this(
                        in.readOptionalString(),
                        in.readOptionalString(),
                        in.readOptionalString(),
                        in.readOptionalCollectionAsList(ToolCall::new)
                    );
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
                        optionalField(CONTENT_FIELD, content),
                        optionalField(REFUSAL_FIELD, refusal),
                        optionalField(ROLE_FIELD, role)
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

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeOptionalString(content);
                    out.writeOptionalString(refusal);
                    out.writeOptionalString(role);
                    out.writeOptionalCollection(toolCalls);
                }

                public record ToolCall(int index, String id, ChatCompletionChunk.Choice.Delta.ToolCall.Function function, String type)
                    implements
                        ChunkedToXContentObject,
                        Writeable {

                    private ToolCall(StreamInput in) throws IOException {
                        this(
                            in.readInt(),
                            in.readOptionalString(),
                            in.readOptional(ChatCompletionChunk.Choice.Delta.ToolCall.Function::new),
                            in.readOptionalString()
                        );
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
                    @Override
                    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                        var content = Iterators.concat(
                            ChunkedToXContentHelper.startObject(),
                            chunk((b, p) -> b.field(INDEX_FIELD, index)),
                            optionalField(ID_FIELD, id)
                        );

                        if (function != null) {
                            content = Iterators.concat(
                                content,
                                ChunkedToXContentHelper.startObject(FUNCTION_FIELD),
                                optionalField(FUNCTION_ARGUMENTS_FIELD, function.arguments()),
                                optionalField(FUNCTION_NAME_FIELD, function.name()),
                                ChunkedToXContentHelper.endObject()
                            );
                        }

                        content = Iterators.concat(
                            content,
                            ChunkedToXContentHelper.chunk((b, p) -> b.field(TYPE_FIELD, type)),
                            ChunkedToXContentHelper.endObject()
                        );
                        return content;
                    }

                    @Override
                    public void writeTo(StreamOutput out) throws IOException {
                        out.writeInt(index);
                        out.writeOptionalString(id);
                        out.writeOptionalWriteable(function);
                        out.writeOptionalString(type);
                    }

                    public record Function(String arguments, String name) implements Writeable {

                        private Function(StreamInput in) throws IOException {
                            this(in.readOptionalString(), in.readOptionalString());
                        }

                        @Override
                        public void writeTo(StreamOutput out) throws IOException {
                            out.writeOptionalString(arguments);
                            out.writeOptionalString(name);
                        }
                    }
                }
            }
        }

        public record Usage(int completionTokens, int promptTokens, int totalTokens) implements Writeable {
            private Usage(StreamInput in) throws IOException {
                this(in.readInt(), in.readInt(), in.readInt());
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeInt(completionTokens);
                out.writeInt(promptTokens);
                out.writeInt(totalTokens);
            }
        }

        private static Iterator<ToXContent> optionalField(String name, String value) {
            if (value == null) {
                return Collections.emptyIterator();
            } else {
                return ChunkedToXContentHelper.chunk((b, p) -> b.field(name, value));
            }
        }

    }
}
