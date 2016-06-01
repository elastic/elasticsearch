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
import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Holds an elasticsearch response. It wraps the {@link CloseableHttpResponse} response and associates it with
 * its corresponding {@link RequestLine} and {@link HttpHost}.
 * It must be closed to free any resource held by it, as well as the corresponding connection in the connection pool.
 */
public class ElasticsearchResponse implements Closeable {

    private final RequestLine requestLine;
    private final HttpHost host;
    private final CloseableHttpResponse response;

    ElasticsearchResponse(RequestLine requestLine, HttpHost host, CloseableHttpResponse response) {
        Objects.requireNonNull(requestLine, "requestLine cannot be null");
        Objects.requireNonNull(host, "node cannot be null");
        Objects.requireNonNull(response, "response cannot be null");
        this.requestLine = requestLine;
        this.host = host;
        this.response = response;
    }

    /**
     * Returns the request line that generated this response
     */
    public RequestLine getRequestLine() {
        return requestLine;
    }

    /**
     * Returns the node that returned this response
     */
    public HttpHost getHost() {
        return host;
    }

    /**
     * Returns the status line of the current response
     */
    public StatusLine getStatusLine() {
        return response.getStatusLine();
    }

    /**
     * Returns all the response headers
     */
    public Header[] getHeaders() {
        return response.getAllHeaders();
    }

    /**
     * Returns the value of the first header with a specified name of this message.
     * If there is more than one matching header in the message the first element is returned.
     * If there is no matching header in the message <code>null</code> is returned.
     */
    public String getFirstHeader(String name) {
        Header header = response.getFirstHeader(name);
        if (header == null) {
            return null;
        }
        return header.getValue();
    }

    /**
     * Returns the response body available, null otherwise
     * @see HttpEntity
     */
    public HttpEntity getEntity() {
        return response.getEntity();
    }

    @Override
    public String toString() {
        return "ElasticsearchResponse{" +
                "requestLine=" + requestLine +
                ", host=" + host +
                ", response=" + response.getStatusLine() +
                '}';
    }

    @Override
    public void close() throws IOException {
        this.response.close();
    }
}
