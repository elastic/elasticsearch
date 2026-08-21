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
import java.util.Iterator;

import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.chunk;
import static org.elasticsearch.common.xcontent.ChunkedToXContentHelper.nullableChunk;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_ARGUMENTS_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.FUNCTION_NAME_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.ID_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.INDEX_FIELD;
import static org.elasticsearch.inference.completion.UnifiedCompletionUtils.TYPE_FIELD;

/**
 * A tool call within a {@link ChatCompletionMessage}.
 */
public record ChatCompletionToolCall(int index, @Nullable String id, @Nullable Function function, @Nullable String type)
    implements
        ChunkedToXContentObject,
        Writeable {

    public ChatCompletionToolCall(StreamInput in) throws IOException {
        this(in.readInt(), in.readOptionalString(), in.readOptional(Function::new), in.readOptionalString());
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
            nullableChunk(ID_FIELD, id)
        );

        if (function != null) {
            content = Iterators.concat(
                content,
                ChunkedToXContentHelper.startObject(FUNCTION_FIELD),
                nullableChunk(FUNCTION_ARGUMENTS_FIELD, function.arguments()),
                nullableChunk(FUNCTION_NAME_FIELD, function.name()),
                ChunkedToXContentHelper.endObject()
            );
        }

        content = Iterators.concat(content, chunk((b, p) -> b.field(TYPE_FIELD, type)), ChunkedToXContentHelper.endObject());
        return content;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(index);
        out.writeOptionalString(id);
        out.writeOptionalWriteable(function);
        out.writeOptionalString(type);
    }

    /**
     * The function associated with a tool call.
     *
     * <p>Field order: {@code arguments, name}.
     */
    public record Function(@Nullable String arguments, @Nullable String name) implements Writeable {

        public Function(StreamInput in) throws IOException {
            this(in.readOptionalString(), in.readOptionalString());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeOptionalString(arguments);
            out.writeOptionalString(name);
        }
    }
}
