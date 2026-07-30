/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.results.completion;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
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
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.USAGE_FIELD;

/**
 * Usage statistics for a unified chat completion response.
 *
 * <p>The {@link #writeTo}/{@link #Usage(StreamInput)} pair preserves the streaming
 * wire-format gates ({@code CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED},
 * {@code INFERENCE_CACHED_TOKENS}, {@code CHAT_COMPLETION_REASONING_SUPPORT_ADDED})
 * verbatim so that the shared class does not alter the byte-level format that shipped
 * with the streaming path.
 */
public record Usage(
    int completionTokens,
    int promptTokens,
    int totalTokens,
    @Nullable PromptTokensDetails promptTokensDetails,
    @Nullable CompletionTokenDetails completionTokenDetails
) implements Writeable {

    public Usage(int completionTokens, int promptTokens, int totalTokens) {
        this(completionTokens, promptTokens, totalTokens, null, null);
    }

    public Usage(StreamInput in) throws IOException {
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

    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return chunk((b, p) -> {
            var builder = b.startObject(USAGE_FIELD)
                .field(COMPLETION_TOKENS_FIELD, completionTokens)
                .field(PROMPT_TOKENS_FIELD, promptTokens)
                .field(TOTAL_TOKENS_FIELD, totalTokens);
            if (promptTokensDetails != null) {
                promptTokensDetails.toXContent(builder, params);
            }
            if (completionTokenDetails != null && completionTokenDetails.reasoningTokens() != null) {
                builder.startObject(COMPLETION_TOKENS_DETAILS_FIELD)
                    .field(REASONING_TOKENS_FIELD, completionTokenDetails.reasoningTokens())
                    .endObject();
            }
            return builder.endObject();
        });
    }

    private static PromptTokensDetails readOptionalPromptTokensDetails(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(CHAT_COMPLETION_CACHE_WRITE_TOKENS_SUPPORT_ADDED)) {
            return in.readOptionalWriteable(PromptTokensDetails::new);
        } else if (in.getTransportVersion().supports(INFERENCE_CACHED_TOKENS)) {
            return PromptTokensDetails.ofNullable(in.readOptionalInt(), null);
        }
        return null;
    }

    public record CompletionTokenDetails(@Nullable Integer reasoningTokens) implements Writeable {

        public CompletionTokenDetails(StreamInput in) throws IOException {
            this(in.readOptionalVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalVInt(reasoningTokens);
        }
    }

    public record PromptTokensDetails(@Nullable Integer cachedTokens, @Nullable Integer cacheWriteTokens)
        implements
            ToXContentFragment,
            Writeable {

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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (isEmpty()) {
                return builder;
            }

            builder.startObject(PROMPT_TOKENS_DETAILS_FIELD);

            if (cachedTokens() != null) {
                builder.field(CACHED_TOKENS_FIELD, cachedTokens());
            }

            if (cacheWriteTokens() != null) {
                builder.field(CACHE_WRITE_TOKENS_FIELD, cacheWriteTokens());
            }

            builder.endObject();

            return builder;
        }

        private boolean isEmpty() {
            return cachedTokens() == null && cacheWriteTokens() == null;
        }
    }
}
