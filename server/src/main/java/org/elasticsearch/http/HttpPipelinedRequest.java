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

package org.elasticsearch.http;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;

import java.util.List;
import java.util.Map;

public class HttpPipelinedRequest implements HttpRequest, HttpPipelinedMessage {

    private final int sequence;
    private final HttpRequest delegate;

    public HttpPipelinedRequest(int sequence, HttpRequest delegate) {
        this.sequence = sequence;
        this.delegate = delegate;
    }

    @Override
    public RestRequest.Method method() {
        return delegate.method();
    }

    @Override
    public String uri() {
        return delegate.uri();
    }

    @Override
    public BytesReference content() {
        return delegate.content();
    }

    @Override
    public Map<String, List<String>> getHeaders() {
        return delegate.getHeaders();
    }

    @Override
    public List<String> strictCookies() {
        return delegate.strictCookies();
    }

    @Override
    public HttpVersion protocolVersion() {
        return delegate.protocolVersion();
    }

    @Override
    public HttpRequest removeHeader(String header) {
        return delegate.removeHeader(header);
    }

    @Override
    public HttpPipelinedResponse createResponse(RestStatus status, BytesReference content) {
        return new HttpPipelinedResponse(sequence, delegate.createResponse(status, content));
    }

    @Override
    public void release() {
        delegate.release();
    }

    @Override
    public HttpRequest releaseAndCopy() {
        return delegate.releaseAndCopy();
    }

    @Override
    public Exception getInboundException() {
        return delegate.getInboundException();
    }

    @Override
    public int getSequence() {
        return sequence;
    }

    public HttpRequest getDelegateRequest() {
        return delegate;
    }
}
