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

import org.apache.http.entity.ContentType;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.apache.http.nio.protocol.HttpAsyncResponseConsumer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.unmodifiableMap;

/**
 * Portion the configuraiton of an HTTP request to Elasticsearch that
 * can be manipulated without changing Elasticsearch's behavior.
 */
public final class RequestOptions {

    public static Builder builder() {
        Builder builder = new Builder();
        builder.setHeaders(NO_HEADERS);
        builder.setHttpAsyncResponseConsumerFactory(HttpAsyncResponseConsumerFactory.DEFAULT);
        return builder;
    }

    private static final Header[] NO_HEADERS = new Header[0];

    private final Header[] headers;
    private final HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory;

    private RequestOptions(Builder builder) {
        this.headers = builder.headers;
        this.httpAsyncResponseConsumerFactory = builder.httpAsyncResponseConsumerFactory;
    }

    public Builder toBuilder() {
        Builder builder = new Builder();
        builder.setHeaders(headers);
        builder.setHttpAsyncResponseConsumerFactory(httpAsyncResponseConsumerFactory);
        return builder;
    }

    /**
     * Headers to attach to the request.
     */
    public Header[] getHeaders() {
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

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("RequestOptions{");
        if (headers.length > 0) {
            b.append(", headers=");
            for (int h = 0; h < headers.length; h++) {
                if (h != 0) {
                    b.append(',');
                }
                b.append(headers[h].toString());
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
        return Arrays.equals(headers, other.headers)
                && httpAsyncResponseConsumerFactory.equals(other.httpAsyncResponseConsumerFactory);
    }

    @Override
    public int hashCode() {
        return Objects.hash(Arrays.hashCode(headers), httpAsyncResponseConsumerFactory);
    }

    public static class Builder {
        private Header[] headers;
        private HttpAsyncResponseConsumerFactory httpAsyncResponseConsumerFactory;

        /**
         * Build the {@linkplain RequestOptions}.
         */
        public RequestOptions builder() {
            return new RequestOptions(this);
        }

        /**
         * Set the headers to attach to the request.
         */
        public void setHeaders(Header... headers) {
            Objects.requireNonNull(headers, "headers cannot be null");
            for (Header header : headers) {
                Objects.requireNonNull(header, "header cannot be null");
            }
            this.headers = headers;
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
    }
}
