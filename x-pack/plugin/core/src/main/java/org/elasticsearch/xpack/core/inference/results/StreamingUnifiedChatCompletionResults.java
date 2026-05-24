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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xpack.core.inference.DequeUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunkNullable;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHED_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHOICES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.COMPLETION_TOKENS_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.COMPLETION_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CONTENT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.DELTA_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FINISH_REASON_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_ARGUMENTS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INDEX_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INFERENCE_CACHED_TOKENS;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.OBJECT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.PROMPT_TOKENS_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.PROMPT_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REFUSAL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ROLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CALLS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOTAL_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.USAGE_FIELD;
import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeEquals;
import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeHashCode;
import static org.elasticsearch.xpack.core.inference.DequeUtils.readDeque;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingUnifiedChatCompletionResults(Flow.Publisher<Results> publisher) implements InferenceServiceResults {

    public static final String NAME = "chat_completion_chunk";

    /**
     * OpenAI Spec only returns one result at a time, and Chat Completion adheres to that spec as much as possible.
     * So we will insert a buffer in between the upstream data and the downstream client so that we only send one request at a time.
     */
    public StreamingUnifiedChatCompletionResults(Flow.Publisher<Results> publisher) {
        Deque<StreamingUnifiedChatCompletionResults.ChatCompletionChunk> buffer = new LinkedBlockingDeque<>();
        AtomicBoolean onComplete = new AtomicBoolean();
        this.publisher = downstream -> {
            publisher.subscribe(new Flow.Subscriber<>() {
                @Override
                public void onSubscribe(Flow.Subscription subscription) {
                    downstream.onSubscribe(new Flow.Subscription() {
                        @Override
                        public void request(long n) {
                            var nextItem = buffer.poll();
                            if (nextItem != null) {
                                downstream.onNext(new Results(DequeUtils.of(nextItem)));
                            } else if (onComplete.get()) {
                                downstream.onComplete();
                            } else {
                                subscription.request(n);
                            }
                        }

                        @Override
                        public void cancel() {
                            subscription.cancel();
                        }
                    });
                }

                @Override
                public void onNext(Results item) {
                    var chunks = item.chunks();
                    var firstItem = chunks.poll();
                    chunks.forEach(buffer::offer);
                    downstream.onNext(new Results(DequeUtils.of(firstItem)));
                }

                @Override
                public void onError(Throwable throwable) {
                    downstream.onError(throwable);
                }

                @Override
                public void onComplete() {
                    // only complete if the buffer is empty, so that the client has a chance to drain the buffer
                    if (onComplete.compareAndSet(false, true)) {
                        if (buffer.isEmpty()) {
                            downstream.onComplete();
                        }
                    }
                }
            });
        };
    }

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
                usage != null ? chunk((b, p) -> {
                    var builder = b.startObject(USAGE_FIELD)
                        .field(COMPLETION_TOKENS_FIELD, usage.completionTokens())
                        .field(PROMPT_TOKENS_FIELD, usage.promptTokens())
                        .field(TOTAL_TOKENS_FIELD, usage.totalTokens());
                    if (usage.cachedTokens() != null) {
                        builder.startObject(PROMPT_TOKENS_DETAILS_FIELD).field(CACHED_TOKENS_FIELD, usage.cachedTokens()).endObject();
                    }
                    if (usage.completionTokenDetails() != null && usage.completionTokenDetails().reasoningTokens() != null) {
                        builder.startObject(COMPLETION_TOKENS_DETAILS_FIELD)
                            .field(REASONING_TOKENS_FIELD, usage.completionTokenDetails().reasoningTokens())
                            .endObject();
                    }
                    return builder.endObject();
                }) : Collections.emptyIterator(),
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
                    chunkNullable(FINISH_REASON_FIELD, finishReason),
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

            public record Delta(
                @Nullable String content,
                @Nullable String refusal,
                @Nullable String role,
                @Nullable List<ToolCall> toolCalls,
                @Nullable String reasoning,
                @Nullable List<ReasoningDetail> reasoningDetails
            ) implements Writeable {

                public Delta(
                    @Nullable String content,
                    @Nullable String refusal,
                    @Nullable String role,
                    @Nullable List<ToolCall> toolCalls
                ) {
                    this(content, refusal, role, toolCalls, null, null);
                }

                private Delta(StreamInput in) throws IOException {
                    this(
                        in.readOptionalString(),
                        in.readOptionalString(),
                        in.readOptionalString(),
                        in.readOptionalCollectionAsList(ToolCall::new),
                        in.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) ? in.readOptionalString() : null,
                        in.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED)
                            ? in.readOptionalNamedWriteableCollectionAsList(ReasoningDetail.class)
                            : null
                    );
                }

                /*
                delta: {
                    content?: string | null;
                    refusal?: string | null;
                    role?: 'system' | 'user' | 'assistant' | 'tool';
                    tool_calls?: Array<{ ... }>;
                    reasoning?: string | null;
                    reasoning_details?: Array<{ ... }>;
                };
                */
                public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                    var xContent = Iterators.concat(
                        ChunkedToXContentHelper.startObject(DELTA_FIELD),
                        chunkNullable(CONTENT_FIELD, content),
                        chunkNullable(REFUSAL_FIELD, refusal),
                        chunkNullable(ROLE_FIELD, role),
                        chunkNullable(REASONING_FIELD, reasoning)
                    );

                    if (toolCalls != null && toolCalls.isEmpty() == false) {
                        xContent = Iterators.concat(
                            xContent,
                            ChunkedToXContentHelper.startArray(TOOL_CALLS_FIELD),
                            Iterators.flatMap(toolCalls.iterator(), t -> t.toXContentChunked(params)),
                            ChunkedToXContentHelper.endArray()
                        );
                    }

                    if (reasoningDetails != null && reasoningDetails.isEmpty() == false) {
                        xContent = Iterators.concat(
                            xContent,
                            ChunkedToXContentHelper.startArray(REASONING_DETAILS_FIELD),
                            Iterators.flatMap(reasoningDetails.iterator(), r -> r.toXContentChunked(params)),
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
                    if (out.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED)) {
                        out.writeOptionalString(reasoning);
                        out.writeOptionalNamedWriteableCollection(reasoningDetails);
                    }
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
                            chunkNullable(ID_FIELD, id)
                        );

                        if (function != null) {
                            content = Iterators.concat(
                                content,
                                ChunkedToXContentHelper.startObject(FUNCTION_FIELD),
                                chunkNullable(FUNCTION_ARGUMENTS_FIELD, function.arguments()),
                                chunkNullable(FUNCTION_NAME_FIELD, function.name()),
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

        public record Usage(
            int completionTokens,
            int promptTokens,
            int totalTokens,
            @Nullable Integer cachedTokens,
            @Nullable CompletionTokenDetails completionTokenDetails
        ) implements Writeable {

            public Usage(int completionTokens, int promptTokens, int totalTokens, @Nullable Integer cachedTokens) {
                this(completionTokens, promptTokens, totalTokens, cachedTokens, null);
            }

            public Usage(int completionTokens, int promptTokens, int totalTokens) {
                this(completionTokens, promptTokens, totalTokens, null);
            }

            private Usage(StreamInput in) throws IOException {
                this(
                    in.readInt(),
                    in.readInt(),
                    in.readInt(),
                    in.getTransportVersion().supports(INFERENCE_CACHED_TOKENS) ? in.readOptionalInt() : null,
                    in.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED)
                        ? in.readOptionalWriteable(CompletionTokenDetails::new)
                        : null
                );
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeInt(completionTokens);
                out.writeInt(promptTokens);
                out.writeInt(totalTokens);
                if (out.getTransportVersion().supports(INFERENCE_CACHED_TOKENS)) {
                    out.writeOptionalInt(cachedTokens);
                }
                if (out.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED)) {
                    out.writeOptionalWriteable(completionTokenDetails);
                }
            }

            public record CompletionTokenDetails(@Nullable Integer reasoningTokens) implements Writeable {

                private CompletionTokenDetails(StreamInput in) throws IOException {
                    this(in.readOptionalVInt());
                }

                @Override
                public void writeTo(StreamOutput out) throws IOException {
                    out.writeOptionalVInt(reasoningTokens);
                }
            }
        }
    }
}
