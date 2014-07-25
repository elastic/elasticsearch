/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.channel.ChannelPipeline;
import org.elasticsearch.common.netty.channel.ChannelPipelineFactory;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.shield.n2n.N2NNettyUpstreamHandler;
import org.elasticsearch.shield.ssl.SSLConfig;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;

import javax.net.ssl.SSLEngine;

/**
 *
 */
public class NettySSLTransport extends NettyTransport {

    private final boolean ssl;
    private final N2NNettyUpstreamHandler shieldUpstreamHandler;

    @Inject
    public NettySSLTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays, Version version,
                             N2NNettyUpstreamHandler shieldUpstreamHandler) {
        super(settings, threadPool, networkService, bigArrays, version);
        this.shieldUpstreamHandler = shieldUpstreamHandler;
        this.ssl = settings.getAsBoolean("shield.transport.ssl", false);
    }

    @Override
    public ChannelPipelineFactory configureClientChannelPipelineFactory() {
        return new SslClientChannelPipelineFactory(this);
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory() {
        return new SslServerChannelPipelineFactory(this);
    }

    private class SslServerChannelPipelineFactory extends ServerChannelPipeFactory {

        private final SSLConfig sslConfig;

        public SslServerChannelPipelineFactory(NettyTransport nettyTransport) {
            super(nettyTransport);
            if (ssl) {
                sslConfig = new SSLConfig(settings.getByPrefix("shield.transport.ssl."), settings.getByPrefix("shield.ssl."));
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
                SSLEngine serverEngine = sslConfig.createSSLEngine();
                serverEngine.setUseClientMode(false);

                pipeline.addFirst("ssl", new SslHandler(serverEngine));
                pipeline.replace("dispatcher", "dispatcher", new SecureMessageChannelHandler(nettyTransport, logger));
            }
            return pipeline;
        }
    }

    private class SslClientChannelPipelineFactory extends ClientChannelPipelineFactory {

        private final SSLConfig sslConfig;

        public SslClientChannelPipelineFactory(NettyTransport transport) {
            super(transport);
            if (ssl) {
                sslConfig = new SSLConfig(settings.getByPrefix("shield.transport.ssl."), settings.getByPrefix("shield.ssl."));
                // try to create an SSL engine, so that exceptions lead to early exit
                sslConfig.createSSLEngine();
            } else {
               sslConfig = null;
            }
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (ssl) {
                SSLEngine clientEngine = sslConfig.createSSLEngine();
                clientEngine.setUseClientMode(true);

                pipeline.addFirst("ssl", new SslHandler(clientEngine));
                pipeline.replace("dispatcher", "dispatcher", new SecureMessageChannelHandler(nettyTransport, logger));
            }
            return pipeline;
        }
    }
}
