/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
