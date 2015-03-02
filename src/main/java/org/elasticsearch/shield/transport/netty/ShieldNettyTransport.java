/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.netty.channel.*;
import org.elasticsearch.common.netty.handler.ssl.SslHandler;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.ServerSSLService;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;

/**
 *
 */
public class ShieldNettyTransport extends NettyTransport {

    public static final String HOSTNAME_VERIFICATION_SETTING = "shield.ssl.hostname_verification";
    public static final String HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING = "shield.ssl.hostname_verification.resolve_name";

    private final ServerSSLService serverSslService;
    private final ClientSSLService clientSSLService;
    private final ShieldSettingsFilter settingsFilter;
    private final @Nullable IPFilter authenticator;
    private final boolean ssl;

    @Inject
    public ShieldNettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays, Version version,
                                @Nullable IPFilter authenticator, @Nullable ServerSSLService serverSSLService, ClientSSLService clientSSLService,
                                ShieldSettingsFilter settingsFilter) {
        super(settings, threadPool, networkService, bigArrays, version);
        this.authenticator = authenticator;
        this.ssl = settings.getAsBoolean("shield.transport.ssl", false);
        this.serverSslService = serverSSLService;
        this.clientSSLService = clientSSLService;
        this.settingsFilter = settingsFilter;
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

        private final Settings profileSettings;

        public SslServerChannelPipelineFactory(NettyTransport nettyTransport, String name, Settings settings, Settings profileSettings) {
            super(nettyTransport, name, settings);
            this.profileSettings = profileSettings;
            settingsFilter.filterOut("transport.profiles." + name + ".shield.*");
        }

        @Override
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = super.getPipeline();
            boolean profileSsl = profileSettings.getAsBoolean("shield.ssl", ssl);
            if (profileSsl) {
                SSLEngine serverEngine;
                if (profileSettings.get("shield.truststore.path") != null) {
                    serverEngine = serverSslService.createSSLEngine(profileSettings.getByPrefix("shield."));
                } else {
                    serverEngine = serverSslService.createSSLEngine();
                }
                serverEngine.setUseClientMode(false);
                serverEngine.setNeedClientAuth(profileSettings.getAsBoolean("shield.ssl.client.auth", settings.getAsBoolean("shield.transport.ssl.client.auth", true)));

                pipeline.addFirst("ssl", new SslHandler(serverEngine));
            }
            if (authenticator != null) {
                pipeline.addFirst("ipfilter", new IPFilterNettyUpstreamHandler(authenticator, name));
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
                pipeline.addFirst("sslInitializer", new ClientSslHandlerInitializer());
            }
            return pipeline;
        }

        /**
         * Handler that waits until connect is called to create a SSLEngine with the proper parameters in order to
         * perform hostname verification
         */
        private class ClientSslHandlerInitializer extends SimpleChannelHandler {

            @Override
            public void connectRequested(ChannelHandlerContext ctx, ChannelStateEvent e) {
                SSLEngine sslEngine;
                if (settings.getAsBoolean(HOSTNAME_VERIFICATION_SETTING, true)) {
                    InetSocketAddress inetSocketAddress = (InetSocketAddress) e.getValue();
                    sslEngine = clientSSLService.createSSLEngine(ImmutableSettings.EMPTY, getHostname(inetSocketAddress), inetSocketAddress.getPort());

                    // By default, a SSLEngine will not perform hostname verification. In order to perform hostname verification
                    // we need to specify a EndpointIdentificationAlgorithm. We use the HTTPS algorithm to prevent against
                    // man in the middle attacks for transport connections
                    SSLParameters parameters = new SSLParameters();
                    parameters.setEndpointIdentificationAlgorithm("HTTPS");
                    sslEngine.setSSLParameters(parameters);
                } else {
                    sslEngine = clientSSLService.createSSLEngine();
                }

                sslEngine.setUseClientMode(true);
                ctx.getPipeline().replace(this, "ssl", new SslHandler(sslEngine));
                ctx.getPipeline().addAfter("ssl", "handshake", new HandshakeWaitingHandler(logger));

                ctx.sendDownstream(e);
            }

            private String getHostname(InetSocketAddress inetSocketAddress) {
                String hostname;
                if (settings.getAsBoolean(HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING, true)) {
                    hostname = inetSocketAddress.getHostName();
                } else {
                    hostname = inetSocketAddress.getHostString();
                }

                if (logger.isTraceEnabled()) {
                    logger.trace("resolved hostname [{}] for address [{}] to be used in ssl hostname verification", hostname, inetSocketAddress);
                }
                return hostname;
            }
        }
    }
}
