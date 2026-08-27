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
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.nullableChunk;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FINISH_REASON_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INDEX_FIELD;

/**
 * A single choice within a {@link ChatCompletionChunkResponse} response.
 *
 * <p>The {@code message} wrapper is rendered as either {@code "delta"} (streaming) or
 * {@code "message"} (non-streaming) depending on the {@code messageFieldName} argument
 * passed to {@link #toXContentChunked(ToXContent.Params, String)}.
 */
public record ChatCompletionChoiceResponse(ChatCompletionMessage message, @Nullable String finishReason, int index) implements Writeable {

    public ChatCompletionChoiceResponse(StreamInput in) throws IOException {
        this(new ChatCompletionMessage(in), in.readOptionalString(), in.readInt());
    }

    /*
      choices: Array<{
        delta/message: { ... };
        finish_reason: string | null;
        index: number;
      }>;
     */
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params, String messageFieldName) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(),
            message.toXContentChunked(params, messageFieldName),
            nullableChunk(FINISH_REASON_FIELD, finishReason),
            chunk((b, p) -> b.field(INDEX_FIELD, index)),
            ChunkedToXContentHelper.endObject()
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeWriteable(message);
        out.writeOptionalString(finishReason);
        out.writeInt(index);
    }
}
