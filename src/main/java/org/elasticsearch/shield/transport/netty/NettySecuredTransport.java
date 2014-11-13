/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.netty.channel.ChannelPipeline;
import org.elasticsearch.common.netty.channel.ChannelPipelineFactory;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.shield.ssl.SSLService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;

import javax.net.ssl.SSLEngine;

/**
 *
 */
public class NettySecuredTransport extends NettyTransport {

    private final boolean ssl;
    private final N2NNettyUpstreamHandler shieldUpstreamHandler;
    private final SSLService sslService;

    @Inject
    public NettySecuredTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays, Version version,
                                 @Nullable N2NNettyUpstreamHandler shieldUpstreamHandler, SSLService sslService) {
        super(settings, threadPool, networkService, bigArrays, version);
        this.shieldUpstreamHandler = shieldUpstreamHandler;
        this.ssl = settings.getAsBoolean("shield.transport.ssl", false);
        this.sslService = sslService;
    }

    @Override
    public ChannelPipelineFactory configureClientChannelPipelineFactory() {
        return new SslClientChannelPipelineFactory(this);
    }

    @Override
    public ChannelPipelineFactory configureServerChannelPipelineFactory(String name, Settings profileSettings) {
        return new SslServerChannelPipelineFactory(this, name, settings, profileSettings);
    }

    private class SslServerChannelPipelineFactory extends ServerChannelPipelineFactory {

        public SslServerChannelPipelineFactory(NettyTransport nettyTransport, String name, Settings settings, Settings profileSettings) {
            super(nettyTransport, name, settings);
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (ssl) {
                SSLEngine serverEngine = sslService.createSSLEngine();
                serverEngine.setUseClientMode(false);
                serverEngine.setNeedClientAuth(true);

                pipeline.addFirst("ssl", new SslHandler(serverEngine));
                pipeline.replace("dispatcher", "dispatcher", new SecuredMessageChannelHandler(nettyTransport, logger));
            }
            if (shieldUpstreamHandler != null) {
                pipeline.addFirst("ipfilter", shieldUpstreamHandler);
            }
            return pipeline;
        }
    }

    private class SslClientChannelPipelineFactory extends ClientChannelPipelineFactory {

        public SslClientChannelPipelineFactory(NettyTransport transport) {
            super(transport);
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            if (ssl) {
                SSLEngine clientEngine = sslService.createSSLEngine();
                clientEngine.setUseClientMode(true);

                pipeline.addFirst("ssl", new SslHandler(clientEngine));
                pipeline.replace("dispatcher", "dispatcher", new SecuredMessageChannelHandler(nettyTransport, logger));
            }
            return pipeline;
        }
    }
}
