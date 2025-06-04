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
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.common.network.ThreadWatchdog;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.transport.InboundPipeline;
import org.elasticsearch.transport.Transports;

/**
 * A handler (must be the last one!) that does size based frame decoding and forwards the actual message
 * to the relevant action.
 */
public class Netty4MessageInboundHandler extends ChannelInboundHandlerAdapter {

    private final Netty4Transport transport;

    private final InboundPipeline pipeline;

    private final ThreadWatchdog.ActivityTracker activityTracker;

    public Netty4MessageInboundHandler(
        Netty4Transport transport,
        InboundPipeline inboundPipeline,
        ThreadWatchdog.ActivityTracker activityTracker
    ) {
        this.transport = transport;
        this.pipeline = inboundPipeline;
        this.activityTracker = activityTracker;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        assert Transports.assertTransportThread();
        assert msg instanceof ByteBuf : "Expected message type ByteBuf, found: " + msg.getClass();

        final ByteBuf buffer = (ByteBuf) msg;
        Netty4TcpChannel channel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        activityTracker.startActivity();
        try {
            pipeline.handleBytes(channel, Netty4Utils.toReleasableBytesReference(buffer));
        } finally {
            activityTracker.stopActivity();
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        assert Transports.assertDefaultThreadContext(transport.getThreadPool().getThreadContext());
        ExceptionsHelper.maybeDieOnAnotherThread(cause);
        final Throwable unwrapped = ExceptionsHelper.unwrap(cause, ElasticsearchException.class);
        final Throwable newCause = unwrapped != null ? unwrapped : cause;
        Netty4TcpChannel tcpChannel = ctx.channel().attr(Netty4Transport.CHANNEL_KEY).get();
        if (newCause instanceof Error) {
            transport.onException(tcpChannel, new Exception(newCause));
        } else {
            transport.onException(tcpChannel, (Exception) newCause);
        }
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        Releasables.closeExpectNoException(pipeline);
        super.channelInactive(ctx);
    }

}
