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
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.nullableChunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.nullableFragment;
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
public record ChatCompletionUsageResponse(
    int completionTokens,
    int promptTokens,
    int totalTokens,
    @Nullable PromptTokensDetails promptTokensDetails,
    @Nullable CompletionTokenDetails completionTokenDetails
) implements Writeable, ChunkedToXContentObject {

    public ChatCompletionUsageResponse(int completionTokens, int promptTokens, int totalTokens) {
        this(completionTokens, promptTokens, totalTokens, null, null);
    }

    public ChatCompletionUsageResponse(StreamInput in) throws IOException {
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
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            chunk((b, p) -> b.field(COMPLETION_TOKENS_FIELD, completionTokens)),
            chunk((b, p) -> b.field(PROMPT_TOKENS_FIELD, promptTokens)),
            chunk((b, p) -> b.field(TOTAL_TOKENS_FIELD, totalTokens)),
            nullableFragment(promptTokensDetails, params),
            nullableFragment(completionTokenDetails, params),
            ChunkedToXContentHelper.endObject()
        );
    }

    private static PromptTokensDetails readOptionalPromptTokensDetails(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED)) {
            return in.readOptionalWriteable(PromptTokensDetails::new);
        } else if (in.getTransportVersion().supports(INFERENCE_CACHED_TOKENS)) {
            return PromptTokensDetails.ofNullable(in.readOptionalInt(), null);
        }
        return null;
    }

    /**
     * Serializes as the named {@code "completion_tokens_details"} field rather than a bare object, so that
     * the field is omitted entirely when this instance carries no data. This is a fragment — see
     * {@link ChunkedToXContent#isFragment()}.
     */
    public record CompletionTokenDetails(@Nullable Integer reasoningTokens) implements Writeable, ChunkedToXContent {

        public CompletionTokenDetails(StreamInput in) throws IOException {
            this(in.readOptionalVInt());
        }

        @Nullable
        public static CompletionTokenDetails ofNullable(@Nullable Integer reasoningTokens) {
            var details = new CompletionTokenDetails(reasoningTokens);
            return details.isEmpty() ? null : details;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(reasoningTokens);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            if (isEmpty()) {
                return Collections.emptyIterator();
            }

            return Iterators.concat(
                ChunkedToXContentHelper.startObject(COMPLETION_TOKENS_DETAILS_FIELD),
                nullableChunk(REASONING_TOKENS_FIELD, reasoningTokens),
                ChunkedToXContentHelper.endObject()
            );
        }

        private boolean isEmpty() {
            return reasoningTokens == null;
        }
    }

    /**
     * Serializes as the named {@code "prompt_tokens_details"} field rather than a bare object, so that the
     * field is omitted entirely when this instance carries no data. This is a fragment — see
     * {@link ChunkedToXContent#isFragment()}.
     */
    public record PromptTokensDetails(@Nullable Integer cachedTokens, @Nullable Integer cacheWriteTokens)
        implements
            Writeable,
            ChunkedToXContent {

        public PromptTokensDetails(StreamInput in) throws IOException {
            this(in.readOptionalVInt(), in.readOptionalVInt());
        }

        @Nullable
        public static PromptTokensDetails ofNullable(@Nullable Integer cachedTokens, @Nullable Integer cacheWriteTokens) {
            var details = new PromptTokensDetails(cachedTokens, cacheWriteTokens);
            return details.isEmpty() ? null : details;
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
                ChunkedToXContentHelper.startObject(PROMPT_TOKENS_DETAILS_FIELD),
                nullableChunk(CACHED_TOKENS_FIELD, cachedTokens),
                nullableChunk(CACHE_WRITE_TOKENS_FIELD, cacheWriteTokens),
                ChunkedToXContentHelper.endObject()
            );
        }

        private boolean isEmpty() {
            return cachedTokens == null && cacheWriteTokens == null;
        }
    }
}
