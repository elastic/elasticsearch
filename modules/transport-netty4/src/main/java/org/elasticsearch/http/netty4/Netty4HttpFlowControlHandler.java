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
 * Forwards one queued inbound HTTP message per {@code read()}, for handlers above that emit several messages per read
 * and handlers below that accept one at a time.
 * <p>
 * Forwards from an event loop task, so a message leaves on a stack of its own rather than on the one that requested or
 * delivered it. Two guarantees follow: draining a long queue costs a constant stack, and the call that delivered a
 * message has returned before that message is forwarded, so any reference that call held is already dropped.
 * <p>
 * Keeps at most one read outstanding, so repeated reads collapse into a single read upstream, unlike netty's
 * {@link io.netty.handler.flow.FlowControlHandler} which counts them and forwards inline.
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
        dispatch(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        assert msg instanceof HttpObject : "unexpected inbound message [" + msg.getClass() + "]";
        queue.addLast((HttpObject) msg);
    }

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        if (readPending) {
            dispatch(ctx);
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

    private void dispatch(ChannelHandlerContext ctx) {
        if (queue.isEmpty()) {
            ctx.read();
        } else {
            ctx.channel().eventLoop().execute(() -> forward(ctx));
        }
    }

    private void forward(ChannelHandlerContext ctx) {
        if (readPending == false) {
            return;
        }
        final HttpObject msg = queue.pollFirst();
        if (msg == null) {
            ctx.read();
            return;
        }
        readPending = false;
        ctx.fireChannelRead(msg);
        ctx.fireChannelReadComplete();
    }

    private void releaseQueued() {
        HttpObject queued;
        while ((queued = queue.pollFirst()) != null) {
            ReferenceCountUtil.release(queued);
        }
    }
}
