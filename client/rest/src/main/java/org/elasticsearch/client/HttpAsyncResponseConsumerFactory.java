/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Factory used to create instances of {@link HttpAsyncResponseConsumer}. Each request retry needs its own instance of the
 * consumer object. Users can implement this interface and pass their own instance to the specialized
 * performRequest methods that accept an {@link HttpAsyncResponseConsumerFactory} instance as argument.
 */
interface HttpAsyncResponseConsumerFactory {

    HttpAsyncResponseConsumerFactory DEFAULT = new Default();

    /**
     * Creates the default type of {@link HttpAsyncResponseConsumer}, based on heap buffering.
     */
    HttpAsyncResponseConsumer<HttpResponse> createHttpAsyncResponseConsumer();

    /**
     * Default factory used to create instances of {@link HttpAsyncResponseConsumer}.
     * Creates one instance of {@link HeapBufferedAsyncResponseConsumer} for each retry with a buffer limit of 100MB.
     */
    class Default implements HttpAsyncResponseConsumerFactory {

        //default buffer limit is 100MB
        static final int DEFAULT_BUFFER_LIMIT = 100 * 1024 * 1024;

        private Default() {

        }

        /**
         * Creates the default type of {@link HttpAsyncResponseConsumer}, based on heap buffering.
         */
        @Override
        public HttpAsyncResponseConsumer<HttpResponse> createHttpAsyncResponseConsumer() {
            return new HeapBufferedAsyncResponseConsumer(DEFAULT_BUFFER_LIMIT);
        }
    }
}
