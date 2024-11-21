/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

/**
 * A super-interface for different HTTP content implementations
 */
public sealed interface HttpBody extends Releasable permits HttpBody.Full, HttpBody.Stream {

    static Full fromBytesReference(BytesReference bytesRef) {
        return new ByteRefHttpBody(ReleasableBytesReference.wrap(bytesRef));
    }

    static Full empty() {
        return new ByteRefHttpBody(ReleasableBytesReference.empty());
    }

    default boolean isFull() {
        return this instanceof Full;
    }

    default boolean isStream() {
        return this instanceof Stream;
    }

    /**
     * Assumes that HTTP body is a full content. If not sure, use {@link HttpBody#isFull()}.
     */
    default Full asFull() {
        assert this instanceof Full;
        return (Full) this;
    }

    /**
     * Assumes that HTTP body is a lazy-stream. If not sure, use {@link HttpBody#isStream()}.
     */
    default Stream asStream() {
        assert this instanceof Stream;
        return (Stream) this;
    }

    /**
     * Full content represents a complete http body content that can be accessed immediately.
     */
    non-sealed interface Full extends HttpBody {
        ReleasableBytesReference bytes();

        @Override
        default void close() {}
    }

    /**
     * Stream is a lazy-loaded content. Stream supports only single handler, this handler must be
     * set before requesting next chunk.
     */
    non-sealed interface Stream extends HttpBody {
        /**
         * Returns current handler
         */
        @Nullable
        ChunkHandler handler();

        /**
         * Adds tracing chunk handler. Tracing handler will be invoked before main handler, and
         * should never release or call for next chunk. It should be used for monitoring and
         * logging purposes.
         */
        void addTracingHandler(ChunkHandler chunkHandler);

        /**
         * Sets handler that can handle next chunk
         */
        void setHandler(ChunkHandler chunkHandler);

        /**
         * Request next chunk of data from the network. The size of the chunk depends on following
         * factors. If request is not compressed then chunk size will be up to
         * {@link HttpTransportSettings#SETTING_HTTP_MAX_CHUNK_SIZE}. If request is compressed then
         * chunk size will be up to max_chunk_size * compression_ratio. Multiple calls can be
         * deduplicated when next chunk is not yet available. It's recommended to call "next" once
         * for every chunk.
         * <pre>
         * {@code
         *     stream.setHandler((chunk, isLast) -> {
         *         processChunk(chunk);
         *         if (isLast == false) {
         *             stream.next();
         *         }
         *     });
         * }
         * </pre>
         */
        void next();
    }

    @FunctionalInterface
    interface ChunkHandler extends Releasable {
        void onNext(ReleasableBytesReference chunk, boolean isLast);

        @Override
        default void close() {}
    }

    record ByteRefHttpBody(ReleasableBytesReference bytes) implements Full {}
}
