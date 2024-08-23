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
import org.elasticsearch.core.Releasable;

/**
 * A super-interface for different HTTP content implementations
 */
public sealed interface HttpBody permits HttpBody.Full, HttpBody.Stream {

    static Full fromBytesReference(BytesReference bytesRef) {
        return new ByteRefHttpBody(bytesRef);
    }

    static Full fromReleasableBytesReference(ReleasableBytesReference bytesRef) {
        return new RelByteRefHttpBody(bytesRef);
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

    /**
     * Assumes that HTTP body is a full content. If not sure, use {@link HttpBody#isFull()}.
     */
    default Full asFull() {
        assert this instanceof Full : "must be full body, got " + this;
        return (Full) this;
    }

    /**
     * Assumes that HTTP body is a lazy-stream. If not sure, use {@link HttpBody#isStream()}.
     */
    default Stream asStream() {
        assert this instanceof Stream : "must be stream body, got " + this;
        return (Stream) this;
    }

    /**
     * Full content represents a complete http body content that can be accessed immediately.
     */
    non-sealed interface Full extends HttpBody, Releasable {
        BytesReference bytes();

        @Override
        default void close() {}
    }

    /**
     * Stream is a lazy-loaded content.
     */
    non-sealed interface Stream extends HttpBody {

        /**
         * Adds tracing handler to stream. Tracing handlers can be used for metering and monitoring.
         * Stream will broadcast chunk to all tracing handlers before sending chunk to consuming
         * handler. Tracing handler should not invoke {@link Stream#next()}.
         */
        void addTracingHandler(ChunkHandler chunkHandler);

        /**
         * Sets chunk consuming handler. This handler must be set only once and responsible for
         * chunk processing, releasing memory, and calling {@link Stream#next()}.
         */
        void setConsumingHandler(ChunkHandler chunkHandler);

        /**
         * Discards all queued and following chunks. In some cases, for example bad-request, content
         * processing is not required. In this case there might be no consuming handler attached to
         * stream. This method prevents buffers leaking.
         */
        void discard();

        /**
         * Request next chunk of data from the network. The size of the chunk depends on following
         * factors. If request is not compressed then chunk size will be up to
         * {@link HttpTransportSettings#SETTING_HTTP_MAX_CHUNK_SIZE}. If request is compressed then
         * chunk size will be up to max_chunk_size * compression_ratio. Multiple calls can be
         * deduplicated when next chunk is not yet available. It's recommended to call "next" once
         * for every chunk.
         * <pre>
         * {@code
         *     stream.setConsumingHandler((chunk, isLast) -> {
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
    interface ChunkHandler {
        void onNext(ReleasableBytesReference chunk, boolean isLast);
    }

    record ByteRefHttpBody(BytesReference bytes) implements Full {}

    record RelByteRefHttpBody(ReleasableBytesReference bytes) implements Full {
        @Override
        public void close() {
            bytes.close();
        }
    }
}
