/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport.netty5;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.api.adaptor.ByteBufAdaptor;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

@ChannelHandler.Sharable
public class NettyByteBufSizer extends MessageToMessageDecoder<ByteBuf> {

    public static final NettyByteBufSizer INSTANCE = new NettyByteBufSizer();

    private NettyByteBufSizer() {
        // sharable singleton
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf msg) {
        ctx.fireChannelRead(ByteBufAdaptor.extractOrCopy(ctx.bufferAllocator(), msg.retain()));
    }
}
