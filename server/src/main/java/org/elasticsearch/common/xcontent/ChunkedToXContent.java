/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * An extension of {@link ToXContent} that can be serialized in chunks by creating a {@link ChunkedXContentSerialization}.
 * This is used by the REST layer to implement flow control that does not rely on blocking the serializing thread when writing the
 * serialized bytes to a non-blocking channel.
 */
public interface ChunkedToXContent extends ToXContent {

    /**
     * Create a serialization session for the implementation instance that will flush to the given {@code builder} and use the given
     * {@code params} the same way {@link ToXContent#toXContent(XContentBuilder, Params)} would.
     * @param builder builder to flush to
     * @param params params for serialization
     * @return serialization session
     */
    ChunkedXContentSerialization toXContentChunked(XContentBuilder builder, ToXContent.Params params);

    @Override
    default XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        ChunkedXContentSerialization serialization = toXContentChunked(builder, params);
        XContentBuilder b = null;
        while (b == null) {
            b = serialization.writeChunk();
        }
        return b;
    }

    /**
     * The state of a chunked x-content serialization.
     */
    interface ChunkedXContentSerialization {

        /**
         * Writes a single chunk to the underlying {@link XContentBuilder}. Must be called repeatedly until a non-null return to complete
         * the serialization.
         *
         * @return the {@link XContentBuilder} that was used for this serialization once finished
         * @throws IOException on serialization failure
         */
        @Nullable
        XContentBuilder writeChunk() throws IOException;
    }
}
