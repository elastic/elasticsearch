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
import io.netty.handler.codec.http.DefaultLastHttpContent;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.LastHttpContent;

/**
 * A {@link LastHttpContent} with pipeline sequence number
 */
public record PipelinedLastHttpContent(LastHttpContent httpContent, int sequence)
    implements
        PipelinedHttpObject,
        LastHttpContent,
        PipelinedHttpRequestPart {

    public PipelinedLastHttpContent(ByteBuf buf, int sequence) {
        this(new DefaultLastHttpContent(buf), sequence);
    }

    public PipelinedLastHttpContent withContent(LastHttpContent httpContent) {
        return new PipelinedLastHttpContent(httpContent, sequence);
    }

    @Override
    public HttpHeaders trailingHeaders() {
        return httpContent.trailingHeaders();
    }

    @Override
    public ByteBuf content() {
        return httpContent.content();
    }

    @Override
    public LastHttpContent copy() {
        return withContent(httpContent.copy());
    }

    @Override
    public LastHttpContent duplicate() {
        return withContent(httpContent.duplicate());
    }

    @Override
    public LastHttpContent retainedDuplicate() {
        return withContent(httpContent.retainedDuplicate());
    }

    @Override
    public LastHttpContent replace(ByteBuf content) {
        return withContent(httpContent.replace(content));
    }

    @Override
    public LastHttpContent retain(int increment) {
        httpContent.retain(increment);
        return this;
    }

    @Override
    public int refCnt() {
        return httpContent.refCnt();
    }

    @Override
    public LastHttpContent retain() {
        httpContent.retain();
        return this;
    }

    @Override
    public LastHttpContent touch() {
        httpContent.touch();
        return this;
    }

    @Override
    public LastHttpContent touch(Object hint) {
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
