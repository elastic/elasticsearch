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

import java.util.function.Consumer;

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

    non-sealed interface Full extends HttpContent, Chunk {
        @Override
        default boolean isLast() {
            return true;
        }

        @Override
        default void release() {}
    }

    non-sealed interface Stream extends HttpContent {
        Consumer<Chunk> handler();

        void setHandler(Consumer<Chunk> chunkHandler);

        void request(int bytes);

        void cancel();
    }

    interface Chunk {
        BytesReference bytes();

        void release();

        boolean isLast();
    }

    record ByteRefHttpContent(BytesReference bytes) implements Full {}
}
