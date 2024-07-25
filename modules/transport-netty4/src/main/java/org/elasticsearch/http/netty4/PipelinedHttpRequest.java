/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

/**
 * A {@link HttpRequest} with pipeline sequence number.
 */
public record PipelinedHttpRequest(HttpRequest request, int sequence)
    implements
        PipelinedHttpObject,
        PipelinedHttpRequestPart,
        HttpRequest {

    public PipelinedHttpRequest(HttpMethod method, String uri, int sequence) {
        this(new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, uri), sequence);
    }

    @Override
    public HttpMethod getMethod() {
        return method();
    }

    @Override
    public HttpMethod method() {
        return request.method();
    }

    @Override
    public HttpRequest setMethod(HttpMethod method) {
        request.setMethod(method);
        return this;
    }

    @Override
    public String getUri() {
        return uri();
    }

    @Override
    public String uri() {
        return request.uri();
    }

    @Override
    public HttpRequest setUri(String uri) {
        request.setUri(uri);
        return this;
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return protocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
        return request.protocolVersion();
    }

    @Override
    public HttpRequest setProtocolVersion(HttpVersion version) {
        request.setProtocolVersion(version);
        return this;
    }

    @Override
    public HttpHeaders headers() {
        return request.headers();
    }

    @Override
    public DecoderResult getDecoderResult() {
        return decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        request.setDecoderResult(result);
    }

    @Override
    public DecoderResult decoderResult() {
        return request.decoderResult();
    }
}
