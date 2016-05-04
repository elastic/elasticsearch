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

import org.apache.http.HttpHost;
import org.apache.http.RequestLine;
import org.apache.http.StatusLine;

import java.io.IOException;

/**
 * Exception thrown when an elasticsearch node responds to a request with a status code that indicates an error
 */
public class ElasticsearchResponseException extends IOException {

    private final HttpHost host;
    private final RequestLine requestLine;
    private final StatusLine statusLine;

    ElasticsearchResponseException(RequestLine requestLine, HttpHost host, StatusLine statusLine) {
        super(buildMessage(requestLine, host, statusLine));
        this.host = host;
        this.requestLine = requestLine;
        this.statusLine = statusLine;
    }

    private static String buildMessage(RequestLine requestLine, HttpHost host, StatusLine statusLine) {
        return requestLine.getMethod() + " " + host + requestLine.getUri() + ": " + statusLine.toString();
    }

    /**
     * Returns whether the error is recoverable or not, hence whether the same request should be retried on other nodes or not
     */
    public boolean isRecoverable() {
        //clients don't retry on 500 because elasticsearch still misuses it instead of 400 in some places
        return statusLine.getStatusCode() >= 502 && statusLine.getStatusCode() <= 504;
    }

    /**
     * Returns the {@link HttpHost} that returned the error
     */
    public HttpHost getHost() {
        return host;
    }

    /**
     * Returns the {@link RequestLine} that triggered the error
     */
    public RequestLine getRequestLine() {
        return requestLine;
    }

    /**
     * Returns the {@link StatusLine} that was returned by elasticsearch
     */
    public StatusLine getStatusLine() {
        return statusLine;
    }
}
