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
import org.elasticsearch.xpack.core.inference.DequeUtils;

import java.io.IOException;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Flow;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeEquals;
import static org.elasticsearch.xpack.core.inference.DequeUtils.dequeHashCode;
import static org.elasticsearch.xpack.core.inference.DequeUtils.readDeque;

/**
 * Chat Completion results that only contain a Flow.Publisher.
 */
public record StreamingUnifiedChatCompletionResults(Flow.Publisher<Results> publisher) implements InferenceServiceResults {

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
            return Iterators.concat(Iterators.flatMap(chunks.iterator(), c -> c.toXContentChunked(params)));
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
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                return Iterators.concat(
                    ChunkedToXContentHelper.startObject(),
                    delta.toXContentChunked(params),
                    ChunkedToXContentHelper.optionalField(FINISH_REASON_FIELD, finishReason),
                    ChunkedToXContentHelper.field(INDEX_FIELD, index),
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
                                ChunkedToXContentHelper.optionalField(FUNCTION_ARGUMENTS_FIELD, function.arguments()),
                                ChunkedToXContentHelper.optionalField(FUNCTION_NAME_FIELD, function.name()),
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

    }
}
