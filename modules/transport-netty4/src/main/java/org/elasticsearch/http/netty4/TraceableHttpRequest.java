/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.http.netty4;

import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.telemetry.tracing.Traceable;

public class TraceableHttpRequest implements Traceable, HttpRequest {

    private HttpRequest delegate;
    private String spanId;

    public TraceableHttpRequest(HttpRequest request) {
        this.delegate = request;
        this.spanId = UUIDs.randomBase64UUID(Randomness.get());
    }

    @Override
    public HttpMethod getMethod() {
        return delegate.getMethod();
    }

    @Override
    public HttpMethod method() {
        return delegate.method();
    }

    @Override
    public HttpRequest setMethod(HttpMethod method) {
        return delegate.setMethod(method);
    }

    @Override
    public String getUri() {
        return delegate.getUri();
    }

    @Override
    public String uri() {
        return delegate.uri();
    }

    @Override
    public HttpRequest setUri(String uri) {
        return delegate.setUri(uri);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return delegate.getProtocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
        return delegate.protocolVersion();
    }

    @Override
    public HttpRequest setProtocolVersion(HttpVersion version) {
        return delegate.setProtocolVersion(version);
    }

    @Override
    public HttpHeaders headers() {
        return delegate.headers();
    }

    @Override
    public DecoderResult getDecoderResult() {
        return delegate.getDecoderResult();
    }

    @Override
    public DecoderResult decoderResult() {
        return delegate.decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        delegate.setDecoderResult(result);
    }

    @Override
    public String getSpanId() {
        return spanId;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof HttpRequest) {
            return obj.equals(delegate);
        }
        return delegate.equals(obj);
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public String toString() {
        return delegate.toString();
    }

    public HttpRequest getDelegate() {
        return delegate;
    }
}
