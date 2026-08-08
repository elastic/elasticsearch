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
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunkNullable;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.field;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.fieldNullable;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHED_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CACHE_WRITE_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHAT_COMPLETION_REASONING_SUPPORT_ADDED;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.COMPLETION_TOKENS_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.COMPLETION_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INFERENCE_CACHED_TOKENS;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.PROMPT_TOKENS_DETAILS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.PROMPT_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.REASONING_TOKENS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TOTAL_TOKENS_FIELD;

/**
 * Usage statistics for a unified chat completion response.
 */
public record ChatCompletionUsage(
    int completionTokens,
    int promptTokens,
    int totalTokens,
    @Nullable PromptTokensDetails promptTokensDetails,
    @Nullable CompletionTokenDetails completionTokenDetails
) implements Writeable, ChunkedToXContentObject {

    public ChatCompletionUsage(int completionTokens, int promptTokens, int totalTokens) {
        this(completionTokens, promptTokens, totalTokens, null, null);
    }

    public ChatCompletionUsage(StreamInput in) throws IOException {
        this(
            in.readInt(),
            in.readInt(),
            in.readInt(),
            readOptionalPromptTokensDetails(in),
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
        if (out.getTransportVersion().supports(CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED)) {
            out.writeOptionalWriteable(promptTokensDetails);
        } else if (out.getTransportVersion().supports(INFERENCE_CACHED_TOKENS)) {
            out.writeOptionalInt(promptTokensDetails() == null ? null : promptTokensDetails().cachedTokens);
        }
        if (out.getTransportVersion().supports(CHAT_COMPLETION_REASONING_SUPPORT_ADDED)) {
            out.writeOptionalWriteable(completionTokenDetails);
        }
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        var xContent = Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            chunk((b, p) -> b.field(COMPLETION_TOKENS_FIELD, completionTokens)),
            chunk((b, p) -> b.field(PROMPT_TOKENS_FIELD, promptTokens)),
            chunk((b, p) -> b.field(TOTAL_TOKENS_FIELD, totalTokens)),
            fieldNullable(PROMPT_TOKENS_DETAILS_FIELD, promptTokensDetails, params)
        );

        if (completionTokenDetails != null && completionTokenDetails.reasoningTokens() != null) {
            xContent = Iterators.concat(xContent, field(COMPLETION_TOKENS_DETAILS_FIELD, completionTokenDetails, params));
        }

        xContent = Iterators.concat(xContent, ChunkedToXContentHelper.endObject());
        return xContent;
    }

    private static PromptTokensDetails readOptionalPromptTokensDetails(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED)) {
            return in.readOptionalWriteable(PromptTokensDetails::new);
        } else if (in.getTransportVersion().supports(INFERENCE_CACHED_TOKENS)) {
            return PromptTokensDetails.ofNullable(in.readOptionalInt(), null);
        }
        return null;
    }

    public record CompletionTokenDetails(@Nullable Integer reasoningTokens) implements Writeable, ChunkedToXContentObject {

        public CompletionTokenDetails(StreamInput in) throws IOException {
            this(in.readOptionalVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(reasoningTokens);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunkNullable(REASONING_TOKENS_FIELD, reasoningTokens),
                ChunkedToXContentHelper.endObject()
            );
        }
    }

    public record PromptTokensDetails(@Nullable Integer cachedTokens, @Nullable Integer cacheWriteTokens)
        implements
            Writeable,
            ChunkedToXContentObject {

        public PromptTokensDetails(StreamInput in) throws IOException {
            this(in.readOptionalVInt(), in.readOptionalVInt());
        }

        @Nullable
        public static PromptTokensDetails ofNullable(@Nullable Integer cachedTokens, @Nullable Integer cacheWriteTokens) {
            if (cachedTokens == null && cacheWriteTokens == null) {
                return null;
            }
            return new PromptTokensDetails(cachedTokens, cacheWriteTokens);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(cachedTokens);
            out.writeOptionalVInt(cacheWriteTokens);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            if (isEmpty()) {
                return Collections.emptyIterator();
            }

            return Iterators.concat(
                ChunkedToXContentHelper.startObject(),
                chunkNullable(CACHED_TOKENS_FIELD, cachedTokens),
                chunkNullable(CACHE_WRITE_TOKENS_FIELD, cacheWriteTokens),
                ChunkedToXContentHelper.endObject()
            );
        }

        private boolean isEmpty() {
            return cachedTokens() == null && cacheWriteTokens() == null;
        }
    }
}
