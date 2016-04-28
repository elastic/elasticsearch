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
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;

/**
 * Holds an elasticsearch response. It wraps the {@link CloseableHttpResponse} response and associates it with
 * its corresponding {@link RequestLine} and {@link Node}
 */
public class ElasticsearchResponse implements Closeable {

    private final RequestLine requestLine;
    private final Node node;
    private final CloseableHttpResponse response;

    ElasticsearchResponse(RequestLine requestLine, Node node, CloseableHttpResponse response) {
        Objects.requireNonNull(requestLine, "requestLine cannot be null");
        Objects.requireNonNull(node, "node cannot be null");
        Objects.requireNonNull(response, "response cannot be null");
        this.requestLine = requestLine;
        this.node = node;
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
    public Node getNode() {
        return node;
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
     * Returns the response bodyi available, null otherwise
     */
    public HttpEntity getEntity() {
        return response.getEntity();
    }

    @Override
    public String toString() {
        return "ElasticsearchResponse{" +
                "requestLine=" + requestLine +
                ", node=" + node +
                ", response=" + response.getStatusLine() +
                '}';
    }

    @Override
    public void close() throws IOException {
        this.response.close();
    }
}
