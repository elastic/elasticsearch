/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.netty.channel.*;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ActionNotFoundTransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.netty.MessageChannelHandler;
import org.elasticsearch.transport.netty.NettyTransport;
import org.elasticsearch.transport.netty.NettyTransportChannel;

import java.io.IOException;
import java.net.InetSocketAddress;

public class SecuredMessageChannelHandler extends MessageChannelHandler {

    private final String profileName;

    public SecuredMessageChannelHandler(NettyTransport nettyTransport, String profileName, ESLogger logger) {
        super(nettyTransport, logger);
        this.profileName = profileName;
    }

    @Override
    public void channelConnected(final ChannelHandlerContext ctx, final ChannelStateEvent e) throws Exception {
        SslHandler sslHandler = ctx.getPipeline().get(SslHandler.class);

        // Make sure handler is present and we are the client
        if (sslHandler == null || !sslHandler.getEngine().getUseClientMode()) {
            return;
        }

        final ChannelFuture handshakeFuture = sslHandler.handshake();

        // Get notified when SSL handshake is done.
        handshakeFuture.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) throws Exception {
                if (future.isSuccess()) {
                    logger.debug("SSL / TLS handshake completed for channel");
                    ctx.sendUpstream(e);
                } else {
                    if (logger.isDebugEnabled()) {
                        logger.debug("SSL / TLS handshake failed, closing channel: {}", future.getCause(), future.getCause().getMessage());
                    } else {
                        logger.error("SSL / TLS handshake failed, closing channel: {}", future.getCause().getMessage());
                    }
                    future.getChannel().close();
                }
            }
        });
    }

    // TODO ADD PREPROCESSING

    /**
     * This is just here to create VisibleNettyTransportChannel() and should be removed after core refactoring
     */
    @Override
    protected String handleRequest(Channel channel, StreamInput buffer, long requestId, Version version) throws IOException {
        final String action = buffer.readString();

        final VisibleNettyTransportChannel transportChannel = new VisibleNettyTransportChannel(profileName, transport, action, channel, requestId, version);
        try {
            final TransportRequestHandler handler = transportServiceAdapter.handler(action, version);
            if (handler == null) {
                throw new ActionNotFoundTransportException(action);
            }
            final TransportRequest request = handler.newInstance();
            request.remoteAddress(new InetSocketTransportAddress((InetSocketAddress) channel.getRemoteAddress()));
            request.readFrom(buffer);
            if (handler.executor() == ThreadPool.Names.SAME) {
                //noinspection unchecked
                handler.messageReceived(request, transportChannel);
            } else {
                threadPool.executor(handler.executor()).execute(new RequestHandler(handler, request, transportChannel, action));
            }
        } catch (Throwable e) {
            try {
                transportChannel.sendResponse(e);
            } catch (IOException e1) {
                logger.warn("Failed to send error message back to client for action [" + action + "]", e);
                logger.warn("Actual Exception", e1);
            }
        }
        return action;
    }

    /**
     * should be removed after upgrade
     */
    public static class VisibleNettyTransportChannel extends NettyTransportChannel {

        private final String profile;

        public VisibleNettyTransportChannel(String profile, NettyTransport transport, String action, Channel channel, long requestId, Version version) {
            super(transport, action, channel, requestId, version);
            this.profile = profile;
        }

        public String getProfile() {
            return profile;
        }
    }

    /**
     * This is just here to make this class visible
     */
    class RequestHandler extends AbstractRunnable {
        private final TransportRequestHandler handler;
        private final TransportRequest request;
        private final NettyTransportChannel transportChannel;
        private final String action;

        public RequestHandler(TransportRequestHandler handler, TransportRequest request, NettyTransportChannel transportChannel, String action) {
            this.handler = handler;
            this.request = request;
            this.transportChannel = transportChannel;
            this.action = action;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public void run() {
            try {
                handler.messageReceived(request, transportChannel);
            } catch (Throwable e) {
                if (transport.lifecycleState() == Lifecycle.State.STARTED) {
                    // we can only send a response transport is started....
                    try {
                        transportChannel.sendResponse(e);
                    } catch (Throwable e1) {
                        logger.warn("Failed to send error message back to client for action [" + action + "]", e1);
                        logger.warn("Actual Exception", e);
                    }
                }
            }
        }

        @Override
        public boolean isForceExecution() {
            return handler.isForceExecution();
        }
    }
}
