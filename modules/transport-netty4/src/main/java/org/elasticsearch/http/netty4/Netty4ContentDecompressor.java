/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpContentDecompressor;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;

import java.util.List;

/**
 * A wrapper around {@link HttpContentDecompressor} that consumes and produces {@link PipelinedHttpRequest} and
 * {@link PipelinedHttpContent}.
 */
public class Netty4ContentDecompressor extends HttpContentDecompressor {

    @Override
    public boolean acceptInboundMessage(Object msg) throws Exception {
        return msg instanceof PipelinedHttpObject;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, HttpObject msg, List<Object> out) throws Exception {
        super.decode(ctx, msg, out);
        final var sequence = ((PipelinedHttpObject) msg).sequence();
        out.replaceAll(obj -> {
            if (obj instanceof PipelinedHttpObject) {
                return obj;
            } else if (obj instanceof FullHttpRequest request) {
                return new PipelinedFullHttpRequest(request, sequence);
            } else if (obj instanceof HttpRequest request) {
                return new PipelinedHttpRequest(request, sequence);
            } else if (obj instanceof LastHttpContent lastContent) {
                return new PipelinedLastHttpContent(lastContent, sequence);
            } else if (obj instanceof HttpContent content) {
                return new PipelinedHttpContent(content, sequence);
            } else {
                throw new IllegalArgumentException();
            }
        });
    }
}
