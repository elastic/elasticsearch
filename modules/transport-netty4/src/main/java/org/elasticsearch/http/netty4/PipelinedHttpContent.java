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
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.HttpContent;

/**
 * A {@link HttpContent} with pipeline sequence number
 */
public record PipelinedHttpContent(HttpContent httpContent, int sequence)
    implements
        PipelinedHttpObject,
        HttpContent,
        PipelinedHttpRequestPart {

    public PipelinedHttpContent(ByteBuf buf, int sequence) {
        this(new DefaultHttpContent(buf), sequence);
    }

    public PipelinedHttpContent withContent(HttpContent httpContent) {
        return new PipelinedHttpContent(httpContent, sequence);
    }

    @Override
    public ByteBuf content() {
        return httpContent.content();
    }

    @Override
    public HttpContent copy() {
        return withContent(httpContent.copy());
    }

    @Override
    public HttpContent duplicate() {
        return withContent(httpContent.duplicate());
    }

    @Override
    public HttpContent retainedDuplicate() {
        return withContent(httpContent.retainedDuplicate());
    }

    @Override
    public HttpContent replace(ByteBuf content) {
        return withContent(httpContent.replace(content));
    }

    @Override
    public int refCnt() {
        return httpContent.refCnt();
    }

    @Override
    public HttpContent retain() {
        httpContent.retain();
        return this;
    }

    @Override
    public HttpContent retain(int increment) {
        httpContent.retain(increment);
        return this;
    }

    @Override
    public HttpContent touch() {
        httpContent.touch();
        return this;
    }

    @Override
    public HttpContent touch(Object hint) {
        httpContent.touch(hint);
        return this;
    }

    @Override
    public boolean release() {
        return httpContent.release();
    }

    @Override
    public boolean release(int decrement) {
        return httpContent.release(decrement);
    }

    @Override
    public DecoderResult getDecoderResult() {
        return httpContent.decoderResult();
    }

    @Override
    public void setDecoderResult(DecoderResult result) {
        httpContent.setDecoderResult(result);
    }

    @Override
    public DecoderResult decoderResult() {
        return httpContent.decoderResult();
    }
}
