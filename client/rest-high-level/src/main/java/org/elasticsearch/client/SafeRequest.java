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

import org.apache.http.Header;

/**
 * Wrapper around {@link Request} that exposes things that may be customized
 * by clients of the {@link RestHighLevelClient} without modifying the behavior
 * of the requests.
 */
public class SafeRequest {
    private final Request request;

    /**
     * Build the {@linkplain SafeRequest}. Package private because it is only
     * called by {@link RestHighLevelClient}.
     */
    SafeRequest(Request request) {
        this.request = request;
    }

    /**
     * Set the headers to attach to the request.
     */
    public void setHeaders(Header[] headers) {
        request.setHeaders(headers);
    }

    /**
     * set the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the
     * response body gets streamed from a non-blocking HTTP connection on the
     * client side.
     */
    public void setHttpAsyncResponseConsumerFactory(HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory) {
        request.setHttpAsyncResponseConsumerFactory(httpAsyncResponseConsumerFactory);
    }
}
