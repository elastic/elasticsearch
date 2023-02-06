/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

public class ValidatedFullHttpRequest implements FullHttpRequest {

    private final FullHttpRequest fullHttpRequest;

    private final HeaderValidationResult validationResult;
    private HttpHeaders trailingHeaders;

    public ValidatedFullHttpRequest(FullHttpRequest fullHttpRequest, HeaderValidationResult validationResult) {
        this.fullHttpRequest = fullHttpRequest;
        this.validationResult = validationResult;
        this.trailingHeaders = fullHttpRequest.trailingHeaders();
    }

    public HeaderValidationResult validationResult() {
        return validationResult;
    }

    void setTrailingHeaders(HttpHeaders trailingHeaders) {
        this.trailingHeaders = trailingHeaders;
    }

    @Override
    public ByteBuf content() {
        return this.fullHttpRequest.content();
    }

    @Override
    public HttpHeaders trailingHeaders() {
        HttpHeaders trailingHeaders = this.trailingHeaders;
        if (trailingHeaders == null) {
            return EmptyHttpHeaders.INSTANCE;
        } else {
            return trailingHeaders;
        }
    }

    @Override
    public FullHttpRequest copy() {
        return new ValidatedFullHttpRequest(this.fullHttpRequest.copy(), this.validationResult);
    }

    @Override
    public FullHttpRequest duplicate() {
        return new ValidatedFullHttpRequest(this.fullHttpRequest.duplicate(), this.validationResult);
    }

    @Override
    public FullHttpRequest retainedDuplicate() {
        return new ValidatedFullHttpRequest(this.fullHttpRequest.retainedDuplicate(), this.validationResult);
    }

    @Override
    public FullHttpRequest replace(ByteBuf content) {
        return new ValidatedFullHttpRequest(this.fullHttpRequest.replace(content), this.validationResult);
    }

    @Override
    public FullHttpRequest retain(int increment) {
        this.fullHttpRequest.retain(increment);
        return this;
    }

    @Override
    public int refCnt() {
        return this.fullHttpRequest.refCnt();
    }

    @Override
    public FullHttpRequest retain() {
        this.fullHttpRequest.retain();
        return this;
    }

    @Override
    public FullHttpRequest touch() {
        this.fullHttpRequest.touch();
        return this;
    }

    @Override
    public FullHttpRequest touch(Object hint) {
        this.fullHttpRequest.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return this.fullHttpRequest.release();
    }

    @Override
    public boolean release(int decrement) {
        return this.fullHttpRequest.release(decrement);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return this.fullHttpRequest.getProtocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
        return this.fullHttpRequest.protocolVersion();
    }

    @Override
    public FullHttpRequest setProtocolVersion(HttpVersion version) {
        this.fullHttpRequest.setProtocolVersion(version);
        return this;
    }

    @Override
    public HttpHeaders headers() {
        return this.fullHttpRequest.headers();
    }

    @Override
    public HttpMethod getMethod() {
        return this.fullHttpRequest.getMethod();
    }

    @Override
    public HttpMethod method() {
        return this.fullHttpRequest.method();
    }

    @Override
    public FullHttpRequest setMethod(HttpMethod method) {
        this.fullHttpRequest.setMethod(method);
        return this;
    }

    @Override
    public String getUri() {
        return this.fullHttpRequest.getUri();
    }

    @Override
    public String uri() {
        return this.fullHttpRequest.uri();
    }

    @Override
    public FullHttpRequest setUri(String uri) {
        this.fullHttpRequest.setUri(uri);
        return this;
    }

    @Override
    public DecoderResult getDecoderResult() {
        return this.fullHttpRequest.getDecoderResult();
    }

    @Override
    public DecoderResult decoderResult() {
        return this.fullHttpRequest.decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        this.fullHttpRequest.setDecoderResult(result);
    }
}
