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

import org.apache.http.message.BasicHeader;
import org.apache.http.Header;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;


import java.util.ArrayList;

/**
 * The portion of an HTTP request to Elasticsearch that can be
 * manipulated without changing Elasticsearch's behavior.
 */
public final class RequestOptions {
    /**
     * Default request options.
     */
    public static final RequestOptions DEFAULT = new Builder(
            Collections.<Header>emptyList(), HeapBufferedResponseConsumerFactory.DEFAULT, null).build();

    private final List<Header> headers;
    private final HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory;
    private final Boolean strictDeprecationMode;

    private RequestOptions(Builder builder) {
        this.headers = Collections.unmodifiableList(new ArrayList<>(builder.headers));
        this.httpAsyncResponseConsumerFactory = builder.httpAsyncResponseConsumerFactory;
        this.strictDeprecationMode = builder.strictDeprecationMode;
    }

    /**
     * Create a builder that contains these options but can be modified.
     */
    public Builder toBuilder() {
        return new Builder(headers, httpAsyncResponseConsumerFactory, strictDeprecationMode);
    }

    /**
     * Headers to attach to the request.
     */
    public List<Header> getHeaders() {
        return headers;
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

    /**
     * Override the client's default for
     * {@link RestClientBuilder#setStrictDeprecationMode strict deprecation}
     * . Null means accept the client's default, true means throw an
     * exception if there are warnings, and false means don't throw.
     */
    public Boolean getStrictDeprecationMode() {
        return strictDeprecationMode;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("RequestOptions{");
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

        RequestOptions other = (RequestOptions) obj;
        return headers.equals(other.headers)
                && httpAsyncResponseConsumerFactory.equals(other.httpAsyncResponseConsumerFactory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(headers, httpAsyncResponseConsumerFactory);
    }

    /**
     * Builds {@link RequestOptions}. Get one by calling
     * {@link RequestOptions#toBuilder} on {@link RequestOptions#DEFAULT} or
     * any other {@linkplain RequestOptions}.
     */
    public static class Builder {
        private final List<Header> headers;
        private HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory;
        private Boolean strictDeprecationMode;

        private Builder(List<Header> headers, HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory,
                Boolean strictDeprecationMode) {
            this.headers = new ArrayList<>(headers);
            this.httpAsyncResponseConsumerFactory = httpAsyncResponseConsumerFactory;
            this.strictDeprecationMode = strictDeprecationMode;
        }

        /**
         * Build the {@linkplain RequestOptions}.
         */
        public RequestOptions build() {
            return new RequestOptions(this);
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
         * Set the {@link HttpAsyncResponseConsumerFactory} used to create one
         * {@link HttpAsyncResponseConsumer} callback per retry. Controls how the
         * response body gets streamed from a non-blocking HTTP connection on the
         * client side.
         */
        public void setHttpAsyncResponseConsumerFactory(HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory) {
            this.httpAsyncResponseConsumerFactory =
                    Objects.requireNonNull(httpAsyncResponseConsumerFactory, "httpAsyncResponseConsumerFactory cannot be null");
        }

        /**
         * Override the client's default for
         * {@link RestClientBuilder#setStrictDeprecationMode strict deprecation}
         * . Null means accept the client's default, true means throw an
         * exception if there are warnings, and false means don't throw.
         */
        public void setStrictDeprecationMode(Boolean strictDeprecationMode) {
            this.strictDeprecationMode = strictDeprecationMode;
        }
    }

    /**
     * Custom implementation of {@link BasicHeader} that overrides equals and
     * hashCode so it is easier to test equality of {@link RequestOptions}.
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
