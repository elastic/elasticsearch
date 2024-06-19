/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;
import io.netty.handler.ssl.SslHandler;

/**
 * An application level handler for TLS close_notify alert.
 * It should be used after HttpAggregation, but before HttpPipelining.
 * This handler performs a graceful connection shutdown.
 * It will wait until all HTTP responses are flushed and then closes the connection.
 */
public class Netty4CloseNotifyHandler extends ChannelDuplexHandler {

    private int inFlyRequests = 0;
    private boolean isClosing = false;

    Netty4CloseNotifyHandler(Channel channel, SslHandler sslHandler) {
        sslHandler.sslCloseFuture().addListener(f -> {
            if (inFlyRequests == 0) {
                channel.flush();
                channel.close();
            } else {
                isClosing = true;
            }
        });
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HttpRequest) {
            inFlyRequests++;
        }
        super.channelRead(ctx, msg);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        if (msg instanceof LastHttpContent) {
            inFlyRequests--;
        }
        super.write(ctx, msg, promise);
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        super.flush(ctx);
        if (isClosing && inFlyRequests == 0) {
            ctx.close();
        }
    }
}
