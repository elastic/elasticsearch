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

public class Netty4HttpHeaderValidator extends ChannelInboundHandlerAdapter {

    private STATE state = STATE.START;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (state == STATE.START) {
            state = STATE.WAITING;

        } else if (state == STATE.WAITING) {

        } else {
            
        }
    }

    private void handlePendingMessages() {

    }

    private void pauseData(ChannelHandlerContext ctx) {
        assert ctx.channel().eventLoop().inEventLoop();
        ctx.channel().config().setAutoRead(false);
    }

    private void resumeData(ChannelHandlerContext ctx) {
        assert ctx.channel().eventLoop().inEventLoop();
        ctx.channel().eventLoop().submit(new Runnable() {
            @Override
            public void run() {
                ctx.channel().config().setAutoRead(true);
                handlePendingMessages();
            }
        });
    }

    private enum STATE {
        START,
        WAITING,
        VALIDATED
    }
}
