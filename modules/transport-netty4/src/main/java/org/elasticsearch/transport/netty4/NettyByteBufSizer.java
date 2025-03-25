/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

@ChannelHandler.Sharable
public class NettyByteBufSizer extends ChannelInboundHandlerAdapter {

    public static final NettyByteBufSizer INSTANCE = new NettyByteBufSizer();

    private NettyByteBufSizer() {
        // sharable singleton
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf buf && buf.capacity() >= 1024) {
            int readableBytes = buf.readableBytes();
            buf = buf.discardReadBytes().capacity(readableBytes);
            assert buf.readableBytes() == readableBytes;
        }
        ctx.fireChannelRead(msg);
    }
}
