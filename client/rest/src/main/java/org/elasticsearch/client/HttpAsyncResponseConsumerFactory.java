/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.HttpResponse;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

import static org.elasticsearch.client.HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory.DEFAULT_BUFFER_LIMIT;

/**
 * Factory used to create instances of {@link HttpAsyncResponseConsumer}. Each request retry needs its own instance of the
 * consumer object. Users can implement this interface and pass their own instance to the specialized
 * performRequest methods that accept an {@link HttpAsyncResponseConsumerFactory} instance as argument.
 */
public interface HttpAsyncResponseConsumerFactory {

    /**
     * Creates the default type of {@link HttpAsyncResponseConsumer}, based on heap buffering with a buffer limit of 100MB.
     */
    HttpAsyncResponseConsumerFactory DEFAULT = new HeapBufferedResponseConsumerFactory(DEFAULT_BUFFER_LIMIT);

    /**
     * Creates the {@link HttpAsyncResponseConsumer}, called once per request attempt.
     */
    HttpAsyncResponseConsumer<HttpResponse> createHttpAsyncResponseConsumer();

    /**
     * Default factory used to create instances of {@link HttpAsyncResponseConsumer}.
     * Creates one instance of {@link HeapBufferedAsyncResponseConsumer} for each request attempt, with a configurable
     * buffer limit which defaults to 100MB.
     */
    class HeapBufferedResponseConsumerFactory implements HttpAsyncResponseConsumerFactory {

        //default buffer limit is 100MB
        static final int DEFAULT_BUFFER_LIMIT = 100 * 1024 * 1024;

        private final int bufferLimit;

        public HeapBufferedResponseConsumerFactory(int bufferLimitBytes) {
            this.bufferLimit = bufferLimitBytes;
        }

        @Override
        public HttpAsyncResponseConsumer<HttpResponse> createHttpAsyncResponseConsumer() {
            return new HeapBufferedAsyncResponseConsumer(bufferLimit);
        }
    }
}
