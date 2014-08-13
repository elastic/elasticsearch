/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.channel.ChannelPipeline;
import org.elasticsearch.common.netty.channel.ChannelPipelineFactory;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.shield.n2n.N2NNettyUpstreamHandler;
import org.elasticsearch.shield.transport.ssl.SSLConfig;

import javax.net.ssl.SSLEngine;

/**
 *
 */
public class NettySecuredHttpServerTransport extends NettyHttpServerTransport {

    private final boolean ssl;
    private final N2NNettyUpstreamHandler shieldUpstreamHandler;

    @Inject
    public NettySecuredHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays,
                                           N2NNettyUpstreamHandler shieldUpstreamHandler) {
        super(settings, networkService, bigArrays);
        this.ssl = settings.getAsBoolean("shield.http.ssl", false);
        this.shieldUpstreamHandler = shieldUpstreamHandler;
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory() {
        return new HttpSslChannelPipelineFactory(this);
    }

    private class HttpSslChannelPipelineFactory extends HttpChannelPipelineFactory {

        private final SSLConfig sslConfig;

        public HttpSslChannelPipelineFactory(NettyHttpServerTransport transport) {
            super(transport);
            if (ssl) {
                sslConfig = new SSLConfig(settings.getByPrefix("shield.http.ssl."), settings.getByPrefix("shield.ssl."));
                // try to create an SSL engine, so that exceptions lead to early exit
                sslConfig.createSSLEngine();
            } else {
                sslConfig = null;
            }
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            pipeline.addFirst("ipfilter", shieldUpstreamHandler);
            if (ssl) {
                SSLEngine engine = sslConfig.createSSLEngine();
                engine.setUseClientMode(false);
                pipeline.addFirst("ssl", new SslHandler(engine));
            }
            return pipeline;
        }
    }
}
