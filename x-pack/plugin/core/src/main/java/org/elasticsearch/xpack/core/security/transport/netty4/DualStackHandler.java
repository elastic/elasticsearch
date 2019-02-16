/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport.netty4;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.transport.netty4.Netty4Utils;
import org.elasticsearch.xpack.core.security.transport.ESMessageHelper;

import javax.net.ssl.SSLEngine;

public class DualStackHandler extends ChannelDuplexHandler {


    static final String HANDLER_NAME = "dual_tls_stack_handler";
    private final SSLEngine engine;

    DualStackHandler(SSLEngine engine) {
        this.engine = engine;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof ByteBuf) {
            ByteBuf in = (ByteBuf) msg;
            if (in.readableBytes() < 2) {
                return;
            } else if (ESMessageHelper.isPlaintextElasticsearchMessage(Netty4Utils.toBytesReference(in))) {
                ctx.pipeline().remove(this);
            } else {
                ctx.pipeline().addAfter(HANDLER_NAME,"sslhandler", new SslHandler(engine));
                ctx.pipeline().remove(this);
            }

            ctx.fireChannelRead(in);
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise) throws Exception {
        assert false : "Should not write before receiving read";
    }

    @Override
    public void flush(ChannelHandlerContext ctx) throws Exception {
        assert false : "Should not flush before receiving read";
    }
}
