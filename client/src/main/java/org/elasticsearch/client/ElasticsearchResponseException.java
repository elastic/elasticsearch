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

import org.apache.http.RequestLine;
import org.apache.http.StatusLine;

import java.io.IOException;

public class ElasticsearchResponseException extends IOException {

    private final Node node;
    private final RequestLine requestLine;
    private final StatusLine statusLine;

    ElasticsearchResponseException(RequestLine requestLine, Node node, StatusLine statusLine) {
        super(buildMessage(requestLine, node, statusLine));
        this.node = node;
        this.requestLine = requestLine;
        this.statusLine = statusLine;
    }

    private static String buildMessage(RequestLine requestLine, Node node, StatusLine statusLine) {
        return requestLine.getMethod() + " " + node.getHttpHost() + requestLine.getUri() + ": " + statusLine.toString();
    }

    public boolean isRecoverable() {
        //clients don't retry on 500 because elasticsearch still misuses it instead of 400 in some places
        return statusLine.getStatusCode() >= 502 && statusLine.getStatusCode() <= 504;
    }

    public Node getNode() {
        return node;
    }

    public RequestLine getRequestLine() {
        return requestLine;
    }

    public StatusLine getStatusLine() {
        return statusLine;
    }
}
