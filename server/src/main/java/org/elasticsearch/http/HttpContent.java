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
import org.elasticsearch.core.Nullable;

import java.util.function.Consumer;

/**
 * A super-interface for different HTTP content implementations
 */
public sealed interface HttpContent permits HttpContent.Full, HttpContent.Stream {

    static Full fromBytesReference(BytesReference bytesRef) {
        return new ByteRefHttpContent(bytesRef);
    }

    static Full empty() {
        return new ByteRefHttpContent(BytesArray.EMPTY);
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
    non-sealed interface Full extends HttpContent, Chunk {
        @Override
        default boolean isLast() {
            return true;
        }

        @Override
        default void release() {}
    }

    /**
     * Stream is a lazy-loaded content. Stream supports only single handler, this handler must be set before requesting next chunk.
     */
    non-sealed interface Stream extends HttpContent {
        /**
         * Returns current handler
         */
        @Nullable
        Consumer<Chunk> handler();

        /**
         * Sets handler that can handle next chunk
         */
        void setHandler(Consumer<Chunk> chunkHandler);

        /**
         * Request next chunk of bytes. The size of next chunk might deffer from requested size in following cases:
         * <ul>
         *     <li>Last content always less or equal requested size</li>
         *     <li>Implementation is free to round up chunk size to optimal network chunk size. For example, transport default chunk size
         *     is 8kb, then requesting 10kb might return 16kb chunk</li>
         * </ul>
         */
        void request(int bytes);

        /**
         * Abort stream consumption, it's terminal operation and no more chunks can be received from stream
         */
        void cancel();
    }

    interface Chunk {
        BytesReference bytes();

        void release();

        boolean isLast();
    }

    record ByteRefHttpContent(BytesReference bytes) implements Full {}
}
