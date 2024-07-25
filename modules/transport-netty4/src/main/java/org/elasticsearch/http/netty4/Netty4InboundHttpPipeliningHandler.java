/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.LastHttpContent;

/**
 * Inbound HTTP pipelining handler that marks all incoming HTTP messages with a sequence number.
 */
public class Netty4InboundHttpPipeliningHandler extends ChannelInboundHandlerAdapter {

    private int sequence = -1;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert msg instanceof HttpRequest || msg instanceof HttpContent;
        if (msg instanceof FullHttpRequest request) {
            sequence++;
            ctx.fireChannelRead(new PipelinedFullHttpRequest(request, sequence));
        } else if (msg instanceof LastHttpContent content) {
            ctx.fireChannelRead(new PipelinedLastHttpContent(content, sequence));
        } else if (msg instanceof HttpContent content) {
            ctx.fireChannelRead(new PipelinedHttpContent(content, sequence));
        } else {
            var request = (HttpRequest) msg;
            sequence++;
            ctx.fireChannelRead(new PipelinedHttpRequest(request, sequence));
        }
    }

}
