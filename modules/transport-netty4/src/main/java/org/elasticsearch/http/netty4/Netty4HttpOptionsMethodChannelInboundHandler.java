/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.util.ReferenceCountUtil;

public final class Netty4HttpOptionsMethodChannelInboundHandler extends SimpleChannelInboundHandler<HttpObject> {

    private boolean dropContent;
    public Netty4HttpOptionsMethodChannelInboundHandler() {
        super(false);
        dropContent = false;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
        if (msg instanceof HttpRequest httpRequest) {
            if (httpRequest.decoderResult().isSuccess() && httpRequest.method() == HttpMethod.OPTIONS) {
                dropContent = true;
            } else {
                dropContent = false;
            }
        }
        if (dropContent) {
            if (msg instanceof HttpRequest httpRequest) {
                httpRequest.headers()
                    .remove(HttpHeaderNames.CONTENT_LENGTH)
                    .remove(HttpHeaderNames.TRANSFER_ENCODING)
                    .remove(HttpHeaderNames.CONTENT_TYPE);
            }
            if (msg instanceof HttpContent toReplace) {
                // release content
                ReferenceCountUtil.release(msg);
                // replace the released buffer
                msg = toReplace.replace(Unpooled.EMPTY_BUFFER);
            }
        }
        ctx.fireChannelRead(msg);
    }
}
