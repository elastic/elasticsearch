/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.logging.ESLogger;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.DownstreamMessageEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.handler.ssl.SslHandler;

import java.util.LinkedList;
import java.util.Queue;

/**
 * Netty requires that nothing be written to the channel prior to the handshake. Writing before the handshake
 * completes, results in odd SSLExceptions being thrown. Channel writes can happen from any thread that
 * can access the channel and Netty does not provide a way to ensure the handshake has occurred before the
 * application writes to the channel. This handler will queue up writes until the handshake has occurred and
 * then will pass the writes through the pipeline. After all writes have been completed, this handler removes
 * itself from the pipeline.
 *
 * NOTE: This class assumes that the transport will not use a closed channel again or attempt to reconnect, which
 * is the way that NettyTransport currently works
 */
public class HandshakeWaitingHandler extends SimpleChannelHandler {

    private final ESLogger logger;

    private boolean handshaken = false;
    private Queue<MessageEvent> pendingWrites = new LinkedList<>();

    /**
     * @param logger    We pass a context aware logger here (logger that is aware of the node name &amp; env)
     */
    public HandshakeWaitingHandler(ESLogger logger) {
        this.logger = logger;
    }

    @Override
    public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
        SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);

        final ChannelFuture handshakeFuture = sslHandler.handshake();
        handshakeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture channelFuture) throws Exception {
                if (handshakeFuture.isSuccess()) {
                    logger.debug("SSL/TLS handshake completed for channel");

                    // We synchronize here to allow all pending writes to be processed prior to any writes coming from
                    // another thread
                    synchronized (HandshakeWaitingHandler.this) {
                        handshaken = true;
                        while (!pendingWrites.isEmpty()) {
                            MessageEvent event = pendingWrites.remove();
                            ctx.sendDownstream(event);
                        }
                        ctx.getPipeline().remove(HandshakeWaitingHandler.class);
                    }

                    ctx.sendUpstream(e);
                } else {
                    Throwable cause = handshakeFuture.getCause();
                    if (logger.isDebugEnabled()) {
                        logger.debug("SSL/TLS handshake failed, closing channel: {}", cause, cause.getMessage());
                    } else {
                        logger.error("SSL/TLS handshake failed, closing channel: {}", cause.getMessage());
                    }

                    synchronized (HandshakeWaitingHandler.this) {
                        // Set failure on the futures of each message so that listeners are called
                        while (!pendingWrites.isEmpty()) {
                            DownstreamMessageEvent event = (DownstreamMessageEvent) pendingWrites.remove();
                            event.getFuture().setFailure(cause);
                        }

                        // Some writes may be waiting to acquire lock, if so the SetFailureOnAddQueue will set
                        // failure on their futures
                        pendingWrites = new SetFailureOnAddQueue(cause);
                        handshakeFuture.getChannel().close();
                    }
                }
            }
        });
    }

    @Override
    public synchronized void writeRequested(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
        // Writes can come from any thread so we need to ensure that we do not let any through
        // until handshake has completed
        if (!handshaken) {
            pendingWrites.add(e);
            return;
        }
        ctx.sendDownstream(e);
    }

    synchronized boolean hasPendingWrites() {
        return !pendingWrites.isEmpty();
    }

    private static class SetFailureOnAddQueue extends LinkedList<MessageEvent> {
        private final Throwable cause;

        SetFailureOnAddQueue(Throwable cause) {
            super();
            this.cause = cause;
        }

        @Override
        public boolean add(MessageEvent messageEvent) {
            DownstreamMessageEvent event = (DownstreamMessageEvent) messageEvent;
            event.getFuture().setFailure(cause);
            return false;
        }

    }
}
