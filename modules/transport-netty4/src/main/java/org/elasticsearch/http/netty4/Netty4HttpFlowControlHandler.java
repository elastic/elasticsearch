/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http.netty4;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.HttpObject;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayDeque;

/**
 * Forwards one inbound HTTP message at a time. Handlers above emit one-to-many: the HTTP decoder splits a socket read
 * into several messages, and the content decompressor expands any one of those into several more. Handlers below accept
 * a single message at a time.
 * <p>
 * Serves the purpose of netty's {@link io.netty.handler.flow.FlowControlHandler}, and differs from it deliberately in
 * two ways. It forwards from {@code read()} and {@code channelReadComplete} rather than as a message arrives, so a
 * message leaves on a later stack than the one that delivered it. And it keeps at most one read outstanding, so
 * repeated reads collapse into a single read upstream rather than accumulating.
 */
class Netty4HttpFlowControlHandler extends ChannelDuplexHandler {

    private final ArrayDeque<HttpObject> queue = new ArrayDeque<>(4);

    private boolean readPending;

    @Override
    public void read(ChannelHandlerContext ctx) {
        if (readPending) {
            return;
        }
        readPending = true;
        if (emit(ctx) == false) {
            ctx.read();
        }
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof HttpObject : "unexpected inbound message [" + msg.getClass() + "]";
        queue.addLast((HttpObject) msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (readPending && emit(ctx) == false) {
            ctx.read();
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        releaseQueued();
        ctx.fireChannelInactive();
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) {
        releaseQueued();
    }

    private boolean emit(ChannelHandlerContext ctx) {
        final HttpObject msg = queue.pollFirst();
        if (msg == null) {
            return false;
        }
        readPending = false;
        ctx.fireChannelRead(msg);
        ctx.fireChannelReadComplete();
        return true;
    }

    private void releaseQueued() {
        HttpObject queued;
        while ((queued = queue.pollFirst()) != null) {
            ReferenceCountUtil.release(queued);
        }
    }
}
