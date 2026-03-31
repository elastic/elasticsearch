/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpUtil;
import io.netty.handler.codec.http.LastHttpContent;

public class Netty4EmptyChunkHandler extends ChannelInboundHandlerAdapter {

    private HttpRequest currentRequest;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        switch (msg) {
            case HttpRequest request -> {
                if (request.decoderResult().isSuccess() && HttpUtil.isTransferEncodingChunked(request)) {
                    currentRequest = request;
                    ctx.read();
                } else {
                    currentRequest = null;
                    ctx.fireChannelRead(request);
                }
            }
            case HttpContent content -> {
                if (currentRequest != null) {
                    if (content instanceof LastHttpContent && content.content().readableBytes() == 0) {
                        HttpUtil.setTransferEncodingChunked(currentRequest, false);
                    }
                    ctx.fireChannelRead(currentRequest);
                    ctx.fireChannelRead(content);
                    currentRequest = null;
                } else {
                    ctx.fireChannelRead(content);
                }
            }
            default -> ctx.fireChannelRead(msg);
        }
    }
}
