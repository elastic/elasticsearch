/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Nullable;

/**
 * A super-interface for different HTTP content implementations
 */
public sealed interface HttpBody permits HttpBody.Full, HttpBody.Stream {

    static Full fromBytesReference(BytesReference bytesRef) {
        return new ByteRefHttpBody(bytesRef);
    }

    static Full empty() {
        return new ByteRefHttpBody(BytesArray.EMPTY);
    }

    default boolean isFull() {
        return this instanceof Full;
    }

    default boolean isStream() {
        return this instanceof Stream;
    }

    default Full asFull() {
        assert this instanceof Full;
        return (Full) this;
    }

    default Stream asStream() {
        assert this instanceof Stream;
        return (Stream) this;
    }

    /**
     * Full content represents a complete http body content that can be accessed immediately
     */
    non-sealed interface Full extends HttpBody {
        BytesReference bytes();
    }

    /**
     * Stream is a lazy-loaded content. Stream supports only single handler, this handler must be set before requesting next chunk.
     */
    non-sealed interface Stream extends HttpBody {
        /**
         * Returns current handler
         */
        @Nullable
        ChunkHandler handler();

        /**
         * Sets handler that can handle next chunk
         */
        void setHandler(ChunkHandler chunkHandler);

        /**
         * Request next chunk of bytes. For every request there will be at least one chunk.
         * Size of the next chunk might vary, depending on following factors:
         * <ul>
         * <li>
         *     Size might round up to optimal network chunk size.
         *     For example, default HttpDecoder content size is 8kb.
         *     Request for 1 byte will produce 8kb chunk, 5 requests for 1 byte will produce 5 chunks of 8kb.
         *     Request for 10kb will produce 16kb chunk.
         * </li>
         * <li>
         *     Size will be lass or equal request size if its a last chunk.
         *     Request for Integer.MAX_VALUE(or other number larger than actual payload) will always produce single full content chunk.
         * </li>
         * <li>
         *     After last chunk there will be no more chunks. This method will not do anything.
         * </li>
         * </ul>
         */
        void requestBytes(int bytes);
    }

    @FunctionalInterface
    interface ChunkHandler {
        void onNext(ReleasableBytesReference chunk, boolean isLast);
    }

    record ByteRefHttpBody(BytesReference bytes) implements Full {}
}
