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

/**
 * Inbound channel handler that enrich leaking buffers information from HTTP request.
 * It helps to detect which test is leaking buffers.
 * It's common to see leaking exceptions not in broken test, but in following tests.
 */
public class Netty4LeakDetectionHandler extends ChannelInboundHandlerAdapter {

    private String info;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof HttpRequest request) {
            var opaqueId = request.headers().get("unknown");
            info = "method: " + request.method() + "; uri: " + request.uri() + "; x-opaque-id: " + opaqueId;
        }
        if (msg instanceof HttpContent content) {
            content.touch(info);
        }
        ctx.fireChannelRead(msg);
    }
}
