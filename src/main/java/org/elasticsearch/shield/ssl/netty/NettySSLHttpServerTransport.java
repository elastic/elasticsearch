/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl.netty;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.netty.channel.ChannelPipeline;
import org.elasticsearch.common.netty.channel.ChannelPipelineFactory;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.http.netty.NettyHttpServerTransport;
import org.elasticsearch.shield.ssl.SSLConfig;

import javax.net.ssl.SSLEngine;

/**
 *
 */
public class NettySSLHttpServerTransport extends NettyHttpServerTransport {

    private final boolean ssl;

    @Inject
    public NettySSLHttpServerTransport(Settings settings, NetworkService networkService, BigArrays bigArrays) {
        super(settings, networkService, bigArrays);
        this.ssl = settings.getAsBoolean("http.ssl", false);
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
            if (ssl) {
                SSLConfig sslConfig = new SSLConfig(settings.getByPrefix("http.ssl."));
                SSLEngine engine = sslConfig.createSSLEngine();
                engine.setUseClientMode(false);
                // TODO MAKE ME CONFIGURABLE
                engine.setNeedClientAuth(false);
                pipeline.addFirst("ssl", new SslHandler(engine));
            }
            return pipeline;
        }
    }
}
