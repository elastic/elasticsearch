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
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * HTTP Request to Elasticsearch.
 */
public final class Request {
    private final String method;
    private final String endpoint;
    private final Map<String, String> parameters = new HashMap<>();
    private final List<Header> headers = new ArrayList<>();

    private HttpEntity entity;
    private HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory =
            HttpAsyncResponseConsumerFactory.DEFAULT;

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
     * Add a query string parameter.
     * @param name the name of the url parameter. Must not be null.
     * @param value the value of the url url parameter. If {@code null} then
     *      the parameter is sent as {@code name} rather than {@code name=value}
     * @throws IllegalArgumentException if a parameter with that name has
     *      already been set
     */
    public void addParameter(String name, String value) {
        Objects.requireNonNull(name, "url parameter name cannot be null");
        // .putIfAbsent(name, value) except we are in Java 7 which doesn't have that.
        if (parameters.containsKey(name)) {
            throw new IllegalArgumentException("url parameter [" + name + "] has already been set to [" + parameters.get(name) + "]");
        } else {
            parameters.put(name, value);
        }
    }

    /**
     * Query string parameters. The returned map is an unmodifiable view of the
     * map in the request so calls to {@link #addParameter(String, String)}
     * will change it.
     */
    public Map<String, String> getParameters() {
        return unmodifiableMap(parameters);
    }

    /**
     * Set the body of the request. If not set or set to {@code null} then no
     * body is sent with the request.
     */
    public void setEntity(HttpEntity entity) {
        this.entity = entity;
    }

    /**
     * Set the body of the request to a string. If not set or set to
     * {@code null} then no body is sent with the request. The
     * {@code Content-Type} will be sent as {@code application/json}.
     * If you need a different content type then use
     * {@link #setEntity(HttpEntity)}.
     */
    public void setJsonEntity(String entity) {
        setEntity(entity == null ? null : new NStringEntity(entity, ContentType.APPLICATION_JSON));
    }

    /**
     * The body of the request. If {@code null} then no body
     * is sent with the request.
     */
    public HttpEntity getEntity() {
        return entity;
    }

    /**
     * Add the provided header to the request.
     */
    public void addHeader(String name, String value) {
        Objects.requireNonNull(name, "header name cannot be null");
        Objects.requireNonNull(value, "header value cannot be null");
        this.headers.add(new ReqHeader(name, value));
    }

    /**
     * Headers to attach to the request.
     */
    List<Header> getHeaders() {
        return Collections.unmodifiableList(headers);
    }

    /**
     * set the {@link HttpAsyncResponseConsumerFactory} used to create one
     * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the
     * response body gets streamed from a non-blocking HTTP connection on the
     * client side.
     */
    public void setHttpAsyncResponseConsumerFactory(HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory) {
        this.httpAsyncResponseConsumerFactory =
                Objects.requireNonNull(httpAsyncResponseConsumerFactory, "httpAsyncResponseConsumerFactory cannot be null");
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
        StringBuilder b = new StringBuilder();
        b.append("Request{");
        b.append("method='").append(method).append('\'');
        b.append(", endpoint='").append(endpoint).append('\'');
        if (false == parameters.isEmpty()) {
            b.append(", params=").append(parameters);
        }
        if (entity != null) {
            b.append(", entity=").append(entity);
        }
        if (headers.size() > 0) {
            b.append(", headers=");
            for (int h = 0; h < headers.size(); h++) {
                if (h != 0) {
                    b.append(',');
                }
                b.append(headers.get(h).toString());
            }
        }
        if (httpAsyncResponseConsumerFactory != HttpAsyncResponseConsumerFactory.DEFAULT) {
            b.append(", consumerFactory=").append(httpAsyncResponseConsumerFactory);
        }
        return b.append('}').toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || (obj.getClass() != getClass())) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        Request other = (Request) obj;
        return method.equals(other.method)
                && endpoint.equals(other.endpoint)
                && parameters.equals(other.parameters)
                && Objects.equals(entity, other.entity)
                && headers.equals(other.headers)
                && httpAsyncResponseConsumerFactory.equals(other.httpAsyncResponseConsumerFactory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(method, endpoint, parameters, entity, headers.hashCode(), httpAsyncResponseConsumerFactory);
    }

    /**
     * Custom implementation of {@link BasicHeader} that overrides equals and hashCode.
     */
    static final class ReqHeader extends BasicHeader {

        ReqHeader(String name, String value) {
            super(name, value);
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other instanceof ReqHeader) {
                Header otherHeader = (Header) other;
                return Objects.equals(getName(), otherHeader.getName()) &&
                        Objects.equals(getValue(), otherHeader.getValue());
            }
            return false;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getName(), getValue());
        }
    }
}
