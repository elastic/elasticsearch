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
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunkNullable;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHOICES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.COMPLETION_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CONTENT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FINISH_REASON_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_ARGUMENTS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INDEX_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.OBJECT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.PROMPT_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REFUSAL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ROLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CALLS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOTAL_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.USAGE_FIELD;

/**
 * Non-streaming chat completion results in the OpenAI-compatible format.
 * The outer JSON object wrapper is provided by {@code InferenceAction.Response.toXContentChunked()},
 * so {@link #toXContentChunked} emits only the field-level content.
 */
public record UnifiedChatCompletionResults(String id, List<Choice> choices, String model, String object, @Nullable Usage usage)
    implements
        InferenceServiceResults {

    public static final String NAME = "unified_chat_completion";
    public static final String CHAT_COMPLETION_OBJECT = "chat.completion";

    public UnifiedChatCompletionResults(StreamInput in) throws IOException {
        this(in.readString(), in.readCollectionAsList(Choice::new), in.readString(), in.readString(), in.readOptionalWriteable(Usage::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeCollection(choices);
        out.writeString(model);
        out.writeString(object);
        out.writeOptionalWriteable(usage);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        var iter = Iterators.concat(
            chunk((b, p) -> b.field(ID_FIELD, id)),
            ChunkedToXContentHelper.array(CHOICES_FIELD, choices.iterator(), params),
            chunk((b, p) -> b.field(MODEL_FIELD, model).field(OBJECT_FIELD, object))
        );
        if (usage != null) {
            iter = Iterators.concat(iter, usage.toXContentChunked(params));
        }
        return iter;
    }

    public record Choice(int index, Message message, @Nullable String finishReason) implements ChunkedToXContentObject, Writeable {

        public Choice(StreamInput in) throws IOException {
            this(in.readInt(), new Message(in), in.readOptionalString());
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunk((b, p) -> b.field(INDEX_FIELD, index)),
                ChunkedToXContentHelper.object(MESSAGE_FIELD, message.toXContentChunked(params)),
                chunkNullable(FINISH_REASON_FIELD, finishReason),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(index);
            message.writeTo(out);
            out.writeOptionalString(finishReason);
        }
    }

    public record Message(@Nullable String role, @Nullable String content, @Nullable List<ToolCall> toolCalls, @Nullable String refusal)
        implements
            ChunkedToXContentObject,
            Writeable {

        public Message(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalString(), in.readOptionalCollectionAsList(ToolCall::new), in.readOptionalString());
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            var iter = Iterators.concat(
                chunkNullable(ROLE_FIELD, role),
                chunkNullable(CONTENT_FIELD, content),
                chunkNullable(REFUSAL_FIELD, refusal)
            );
            if (toolCalls != null) {
                iter = Iterators.concat(iter, ChunkedToXContentHelper.array(TOOL_CALLS_FIELD, toolCalls.iterator(), params));
            }
            return iter;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(role);
            out.writeOptionalString(content);
            out.writeOptionalCollection(toolCalls);
            out.writeOptionalString(refusal);
        }
    }

    public record ToolCall(String id, String type, Function function) implements ChunkedToXContentObject, Writeable {

        public ToolCall(StreamInput in) throws IOException {
            this(in.readString(), in.readString(), new Function(in));
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunk((b, p) -> b.field(ID_FIELD, id).field(TYPE_FIELD, type)),
                ChunkedToXContentHelper.object(FUNCTION_FIELD, function.toXContentChunked(params)),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(id);
            out.writeString(type);
            function.writeTo(out);
        }

        public record Function(String name, String arguments) implements ChunkedToXContentObject, Writeable {

            public Function(StreamInput in) throws IOException {
                this(in.readString(), in.readString());
            }

            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                return Iterators.concat(chunk((b, p) -> b.field(FUNCTION_NAME_FIELD, name).field(FUNCTION_ARGUMENTS_FIELD, arguments)));
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeString(name);
                out.writeString(arguments);
            }
        }
    }

    public record Usage(int completionTokens, int promptTokens, int totalTokens) implements ChunkedToXContentObject, Writeable {

        public Usage(StreamInput in) throws IOException {
            this(in.readInt(), in.readInt(), in.readInt());
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(USAGE_FIELD),
                chunk(
                    (b, p) -> b.field(COMPLETION_TOKENS_FIELD, completionTokens)
                        .field(PROMPT_TOKENS_FIELD, promptTokens)
                        .field(TOTAL_TOKENS_FIELD, totalTokens)
                ),
                ChunkedToXContentHelper.endObject()
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeInt(completionTokens);
            out.writeInt(promptTokens);
            out.writeInt(totalTokens);
        }
    }

}
