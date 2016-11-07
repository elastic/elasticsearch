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
 * Default factory used to create instances of {@link HttpAsyncResponseConsumer}. Each request retry needs its own instance of the
 * consumer object, which can also be customized by users. By default an instance of {@link HeapBufferedAsyncResponseConsumer} is created
 * for each retry. Otherwise users can extend this class and pass their own instance to the specialized performRequest methods that accept
 * an {@link HttpAsyncResponseConsumerFactory} instance as argument.
 */
public class HttpAsyncResponseConsumerFactory {

    static HttpAsyncResponseConsumerFactory INSTANCE = new HttpAsyncResponseConsumerFactory();

    protected HttpAsyncResponseConsumerFactory() {

    }

    /**
     * Creates the default type of {@link HttpAsyncResponseConsumer}, based on heap buffering.
     */
    public HttpAsyncResponseConsumer<HttpResponse> createHttpAsyncResponseConsumer() {
        return new HeapBufferedAsyncResponseConsumer();
    }
}
