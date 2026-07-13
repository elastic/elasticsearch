/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

        // default buffer limit is 100MB
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
