/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.netty.channel.ChannelPipeline;
import org.elasticsearch.common.netty.channel.ChannelPipelineFactory;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.shield.ssl.SSLService;
import org.elasticsearch.shield.ssl.SSLServiceProvider;
import org.elasticsearch.shield.transport.n2n.IPFilteringN2NAuthenticator;

import javax.net.ssl.SSLEngine;

/**
 *
 */
public class NettySecuredHttpServerTransport extends NettyHttpServerTransport {

    private final IPFilteringN2NAuthenticator authenticator;
    private final @Nullable SSLService sslService;

    @Inject
    public NettySecuredHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays,
                                           IPFilteringN2NAuthenticator authenticator, SSLServiceProvider sslServiceProvider) {
        super(settings, networkService, bigArrays);
        this.authenticator = authenticator;
        this.sslService = settings.getAsBoolean("shield.http.ssl", false) ? sslServiceProvider.get() : null;
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory() {
        return new HttpSslChannelPipelineFactory(this);
    }

    private class HttpSslChannelPipelineFactory extends HttpChannelPipelineFactory {

        public HttpSslChannelPipelineFactory(NettyHttpServerTransport transport) {
            super(transport);
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (sslService != null) {
                SSLEngine engine = sslService.createSSLEngine();
                engine.setUseClientMode(false);
                engine.setNeedClientAuth(false);

                pipeline.addFirst("ssl", new SslHandler(engine));
            }
            pipeline.addFirst("ipfilter", new N2NNettyUpstreamHandler(authenticator, "default"));
            return pipeline;
        }
    }
}
