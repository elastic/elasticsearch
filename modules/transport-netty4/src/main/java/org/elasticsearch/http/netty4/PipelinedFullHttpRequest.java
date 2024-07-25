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
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

/**
 * A {@link FullHttpRequest} with pipeline sequence number
 */
public record PipelinedFullHttpRequest(FullHttpRequest request, int sequence) implements PipelinedHttpObject, FullHttpRequest {

    public PipelinedFullHttpRequest withRequest(FullHttpRequest request) {
        return new PipelinedFullHttpRequest(request, sequence);
    }

    @Override
    public ByteBuf content() {
        return request.content();
    }

    @Override
    public HttpHeaders trailingHeaders() {
        return request.headers();
    }

    @Override
    public FullHttpRequest copy() {
        return withRequest(request.copy());
    }

    @Override
    public FullHttpRequest duplicate() {
        return withRequest(request.duplicate());
    }

    @Override
    public FullHttpRequest retainedDuplicate() {
        return withRequest(request.retainedDuplicate());
    }

    @Override
    public FullHttpRequest replace(ByteBuf content) {
        return withRequest(request.replace(content));
    }

    @Override
    public FullHttpRequest retain(int increment) {
        request.retain();
        return this;
    }

    @Override
    public int refCnt() {
        return request.refCnt();
    }

    @Override
    public FullHttpRequest retain() {
        request.retain();
        return this;
    }

    @Override
    public FullHttpRequest touch() {
        request.touch();
        return this;
    }

    @Override
    public FullHttpRequest touch(Object hint) {
        request.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return request.release();
    }

    @Override
    public boolean release(int decrement) {
        return request.release(decrement);
    }

    @Override
    public HttpVersion getProtocolVersion() {
        return request.protocolVersion();
    }

    @Override
    public HttpVersion protocolVersion() {
        return request.protocolVersion();
    }

    @Override
    public FullHttpRequest setProtocolVersion(HttpVersion version) {
        request.setProtocolVersion(version);
        return this;
    }

    @Override
    public HttpHeaders headers() {
        return request.headers();
    }

    @Override
    public HttpMethod getMethod() {
        return request.method();
    }

    @Override
    public HttpMethod method() {
        return request.method();
    }

    @Override
    public FullHttpRequest setMethod(HttpMethod method) {
        request.setMethod(method);
        return this;
    }

    @Override
    public String getUri() {
        return request.uri();
    }

    @Override
    public String uri() {
        return request.uri();
    }

    @Override
    public FullHttpRequest setUri(String uri) {
        request.setUri(uri);
        return this;
    }

    @Override
    public DecoderResult getDecoderResult() {
        return request.decoderResult();
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
