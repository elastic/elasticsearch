/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;

public class ValidatedHttpRequest implements HttpRequest {

    // decorated instance
    private final HttpRequest httpRequest;
    private final HeaderValidationResult validationResult;

    public static ValidatedHttpRequest decorateOK(HttpRequest httpRequest) {
        return new ValidatedHttpRequest(httpRequest, HeaderValidationResult.OK);
    }

    private ValidatedHttpRequest(HttpRequest httpRequest, HeaderValidationResult validationResult) {
        this.httpRequest = httpRequest;
        this.validationResult = validationResult;
    }

    public HeaderValidationResult validationResult() {
        return validationResult;
    }

    // TODO remove this
    public HttpRequest httpRequest() {
        return httpRequest;
    }

    @Override
    public HttpMethod getMethod() {
        return httpRequest.getMethod();
    }

    @Override
    public HttpMethod method() {
        return httpRequest.method();
    }

    @Override
    public HttpRequest setMethod(HttpMethod method) {
        return httpRequest.setMethod(method);
    }

    @Override
    public String getUri() {
        return httpRequest.getUri();
    }

    @Override
    public String uri() {
        return httpRequest.uri();
    }

    @Override
    public HttpRequest setUri(String uri) {
        return httpRequest.setUri(uri);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return httpRequest.getProtocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
        return httpRequest.protocolVersion();
    }

    @Override
    public HttpRequest setProtocolVersion(HttpVersion version) {
        httpRequest.setProtocolVersion(version);
        return this;
    }

    @Override
    public HttpHeaders headers() {
        return httpRequest.headers();
    }

    @Override
    public DecoderResult getDecoderResult() {
        return httpRequest.getDecoderResult();
    }

    @Override
    public DecoderResult decoderResult() {
        return httpRequest.decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        httpRequest.setDecoderResult(result);
    }
}
