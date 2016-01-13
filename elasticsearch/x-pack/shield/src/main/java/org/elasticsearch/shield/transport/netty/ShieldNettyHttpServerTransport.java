/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.shield.ssl.ServerSSLService;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.threadpool.ThreadPool;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;

import static org.elasticsearch.shield.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.shield.transport.SSLExceptionHelper.isNotSslRecordException;

/**
 *
 */
public class ShieldNettyHttpServerTransport extends NettyHttpServerTransport {

    public static final String HTTP_SSL_SETTING = "shield.http.ssl";
    public static final boolean HTTP_SSL_DEFAULT = false;
    public static final String HTTP_CLIENT_AUTH_SETTING = "shield.http.ssl.client.auth";
    public static final SSLClientAuth HTTP_CLIENT_AUTH_DEFAULT = SSLClientAuth.NO;

    private final IPFilter ipFilter;
    private final ServerSSLService sslService;
    private final boolean ssl;

    @Inject
    public ShieldNettyHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays,
                                          IPFilter ipFilter, ServerSSLService sslService, ThreadPool threadPool) {
        super(settings, networkService, bigArrays, threadPool);
        this.ipFilter = ipFilter;
        this.ssl = settings.getAsBoolean(HTTP_SSL_SETTING, HTTP_SSL_DEFAULT);
        this.sslService =  sslService;
    }

    @Override
    protected void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (!lifecycle.started()) {
            return;
        }

        Throwable t = e.getCause();
        if (isNotSslRecordException(t)) {
            if (logger.isTraceEnabled()) {
                logger.trace("received plaintext http traffic on a https channel, closing connection {}", t, ctx.getChannel());
            } else {
                logger.warn("received plaintext http traffic on a https channel, closing connection {}", ctx.getChannel());
            }
            ctx.getChannel().close();
        } else if (isCloseDuringHandshakeException(t)) {
            if (logger.isTraceEnabled()) {
                logger.trace("connection {} closed during handshake", t, ctx.getChannel());
            } else {
                logger.warn("connection {} closed during handshake", ctx.getChannel());
            }
            ctx.getChannel().close();
        } else {
            super.exceptionCaught(ctx, e);
        }
    }

    @Override
    protected void doStart() {
        super.doStart();
        ipFilter.setBoundHttpTransportAddress(this.boundAddress());
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory() {
        return new HttpSslChannelPipelineFactory(this);
    }

    private class HttpSslChannelPipelineFactory extends HttpChannelPipelineFactory {

        private final SSLClientAuth clientAuth;

        public HttpSslChannelPipelineFactory(NettyHttpServerTransport transport) {
            super(transport, detailedErrorsEnabled, threadPool.getThreadContext());
            clientAuth = SSLClientAuth.parse(settings.get(HTTP_CLIENT_AUTH_SETTING), HTTP_CLIENT_AUTH_DEFAULT);
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (ssl) {
                SSLEngine engine = sslService.createSSLEngine();
                engine.setUseClientMode(false);
                clientAuth.configure(engine);

                pipeline.addFirst("ssl", new SslHandler(engine));
            }
            pipeline.addFirst("ipfilter", new IPFilterNettyUpstreamHandler(ipFilter, IPFilter.HTTP_PROFILE_NAME));
            return pipeline;
        }
    }
}
