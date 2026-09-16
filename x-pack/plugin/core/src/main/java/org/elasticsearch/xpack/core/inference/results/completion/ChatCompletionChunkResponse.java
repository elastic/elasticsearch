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
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.nullableField;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.CHOICES_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.DELTA_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MESSAGE_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.MODEL_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.OBJECT_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.USAGE_FIELD;

/**
 * This class handles both the non-streaming
 * {@code chat.completion} payload and {@code chat.completion.chunk}: the two response types share all field shapes, component
 * order, and wire encoding.  The only difference is the field name for the message wrapper
 * ({@code "delta"} vs {@code "message"}) and whether the top-level JSON object is emitted by
 * this class or by the outer {@code InferenceAction.Response}.
 *
 * <h2>XContent entry points</h2>
 * <ul>
 *   <li>{@link #toXContentChunked(ToXContent.Params)} — fields only, no enclosing
 *       object, using {@code "message"}.  The wrapper is supplied by
 *       {@code InferenceAction.Response.toXContentChunked()}.  This is the
 *       <em>non-streaming</em> form and is the implementation required by
 *       {@link InferenceServiceResults}.</li>
 *   <li>{@link #toStreamingXContentChunked(ToXContent.Params)} — self-contained
 *       {@code { ... }} object using {@code "delta"}.  This is the
 *       <em>streaming SSE chunk</em> form consumed by
 *       {@code StreamingUnifiedChatCompletionResults.Results}.</li>
 * </ul>
 */
public record ChatCompletionChunkResponse(
    String id,
    @Nullable List<ChatCompletionChoiceResponse> choices,
    String model,
    String object,
    @Nullable ChatCompletionUsageResponse usage
) implements InferenceServiceResults {

    public static final String NAME = "chat_completion_chunk";

    public ChatCompletionChunkResponse(StreamInput in) throws IOException {
        this(
            in.readString(),
            in.readOptionalCollectionAsList(ChatCompletionChoiceResponse::new),
            in.readString(),
            in.readString(),
            in.readOptional(ChatCompletionUsageResponse::new)
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

    @Override
    public String getWriteableName() {
        return NAME;
    }

    /**
     * Fields only (no enclosing object), using {@code "message"}.
     * The outer object wrapper is supplied by {@code InferenceAction.Response.toXContentChunked()}.
     * This is the <em>non-streaming</em> form.
     */
    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return fields(params, MESSAGE_FIELD);
    }

    /**
     * Self-contained {@code { ... }} object using {@code "delta"} — the <em>streaming SSE chunk</em> form.
     */
    public Iterator<? extends ToXContent> toStreamingXContentChunked(ToXContent.Params params) {
        return Iterators.concat(ChunkedToXContentHelper.startObject(), fields(params, DELTA_FIELD), ChunkedToXContentHelper.endObject());
    }

    private Iterator<? extends ToXContent> fields(ToXContent.Params params, String messageFieldName) {
        return Iterators.concat(
            chunk((b, p) -> b.field(ID_FIELD, id)),
            choices != null
                ? Iterators.concat(
                    ChunkedToXContentHelper.startArray(CHOICES_FIELD),
                    Iterators.flatMap(choices.iterator(), c -> c.toXContentChunked(params, messageFieldName)),
                    ChunkedToXContentHelper.endArray()
                )
                : Collections.emptyIterator(),
            chunk((b, p) -> b.field(MODEL_FIELD, model).field(OBJECT_FIELD, object)),
            nullableField(USAGE_FIELD, usage, params)
        );
    }
}
