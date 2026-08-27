/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results.completion;

import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.nullableChunk;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CONTENT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REFUSAL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ROLE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOOL_CALLS_FIELD;

/**
 * The message (or delta, in streaming) within a {@link ChatCompletionChoiceResponse}.
 *
 * <p>XContent is emitted as a named object under {@code messageFieldName} — either
 * {@code "message"} for non-streaming responses or {@code "delta"} for streaming SSE chunks.
 */
public record ChatCompletionMessageResponse(
    @Nullable String content,
    @Nullable String refusal,
    @Nullable String role,
    @Nullable List<ChatCompletionToolCall> toolCalls,
    @Nullable String reasoning,
    @Nullable List<ReasoningDetail> reasoningDetails
) implements Writeable {

    public ChatCompletionMessageResponse(
        @Nullable String content,
        @Nullable String refusal,
        @Nullable String role,
        @Nullable List<ChatCompletionToolCall> toolCalls
    ) {
        this(content, refusal, role, toolCalls, null, null);
    }

    public ChatCompletionMessageResponse(StreamInput in) throws IOException {
        this(
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalString(),
            in.readOptionalCollectionAsList(ChatCompletionToolCall::new),
            in.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED) ? in.readOptionalString() : null,
            in.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED)
                ? in.readOptionalNamedWriteableCollectionAsList(ReasoningDetail.class)
                : null
        );
    }

    /**
     * Emits {@code startObject(messageFieldName) ... endObject}, where
     * {@code messageFieldName} is either {@code "delta"} (streaming) or {@code "message"} (non-streaming).
     * {@code
     *   delta: {
     *       content?: string | null;
     *       refusal?: string | null;
     *       role?: 'system' | 'user' | 'assistant' | 'tool';
     *       tool_calls?: Array<{ ... }>;
     *       reasoning?: string | null;
     *       reasoning_details?: Array<{ ... }>;
     *   };
     * }
     */
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params, String messageFieldName) {
        var xContent = Iterators.concat(
            ChunkedToXContentHelper.startObject(messageFieldName),
            nullableChunk(CONTENT_FIELD, content),
            nullableChunk(REFUSAL_FIELD, refusal),
            nullableChunk(ROLE_FIELD, role),
            nullableChunk(REASONING_FIELD, reasoning)
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
}
