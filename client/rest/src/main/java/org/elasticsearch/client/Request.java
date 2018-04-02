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
import org.apache.http.HttpEntity;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public final class Request {
    private final String method;
    private final String endpoint;

    private Map<String, String> parameters = Collections.<String, String>emptyMap();
    private HttpEntity entity;
    private Header[] headers;
    private HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory =
            HttpAsyncResponseConsumerFactory.DEFAULT;

    public Request(String method, String endpoint, Map<String, String> parameters, HttpEntity entity) {
        this(method, endpoint);
        this.parameters = Objects.requireNonNull(parameters, "parameters cannot be null");
        this.entity = entity;
        // TODO drop this ctor
    }

    /**
     * Create the {@linkplain Request}.
     * @param method the HTTP method
     * @param endpoint the path of the request (without scheme, host, port, or prefix)
     */
    public Request(String method, String endpoint) {
        this.method = Objects.requireNonNull(method, "method cannot be null");
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint cannot be null");
    }

    /**
     * The HTTP method.
     */
    public String getMethod() {
        return method;
    }

    /**
     * The path of the request (without scheme, host, port, or prefix).
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * Set the query string parameters. Polite users will not manipulate the
     * Map after setting it.
     */
    public void setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
    }

    /**
     * Query string parameters.
     */
    public Map<String, String> getParameters() {
        return parameters;
    }

    /**
     * Set the body of the request. If not set or set to {@code null} then no
     * body is sent with the request.
     */
    public void setEntity(HttpEntity entity) {
        this.entity = entity;
    }

    /**
     * The body of the request. If {@code null} then no body
     * is sent with the request.
     */
    public HttpEntity getEntity() {
        return entity;
    }

    /**
     * Set the headers to attach to the request.
     */
    public void setHeaders(Header... headers) {
        this.headers = headers;
    }

    /**
     * Headers to attach to the request.
     */
    public Header[] getHeaders() {
        return headers;
    }

    /**
     * set the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the
     * response body gets streamed from a non-blocking HTTP connection on the
     * client side.
     */
    public void setHttpAsyncResponseConsumerFactory(HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory) {
        this.httpAsyncResponseConsumerFactory = httpAsyncResponseConsumerFactory;
    }

    /**
     * The {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the
     * response body gets streamed from a non-blocking HTTP connection on the
     * client side.
     */
    public HttpAsyncResponseConsumerFactory getHttpAsyncResponseConsumerFactory() {
        return httpAsyncResponseConsumerFactory;
    }

    @Override
    public String toString() {
        return "Request{" +
                "method='" + method + '\'' +
                ", endpoint='" + endpoint + '\'' +
                ", params=" + parameters +
                ", hasBody=" + (entity != null) +
                '}';
    }
}
