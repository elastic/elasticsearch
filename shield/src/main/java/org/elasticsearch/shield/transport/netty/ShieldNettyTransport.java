/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.transport.netty;

import org.elasticsearch.Version;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.shield.ShieldSettingsFilter;
import org.elasticsearch.shield.ssl.ClientSSLService;
import org.elasticsearch.shield.ssl.ServerSSLService;
import org.elasticsearch.shield.transport.SSLClientAuth;
import org.elasticsearch.shield.transport.filter.IPFilter;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty.NettyTransport;
import org.jboss.netty.channel.*;
import org.jboss.netty.handler.ssl.SslHandler;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;

import static org.elasticsearch.shield.transport.SSLExceptionHelper.isCloseDuringHandshakeException;
import static org.elasticsearch.shield.transport.SSLExceptionHelper.isNotSslRecordException;

/**
 *
 */
public class ShieldNettyTransport extends NettyTransport {

    public static final String HOSTNAME_VERIFICATION_SETTING = "shield.ssl.hostname_verification";
    public static final String HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING = "shield.ssl.hostname_verification.resolve_name";
    public static final String TRANSPORT_SSL_SETTING = "shield.transport.ssl";
    public static final boolean TRANSPORT_SSL_DEFAULT = false;
    public static final String TRANSPORT_CLIENT_AUTH_SETTING = "shield.transport.ssl.client.auth";
    public static final SSLClientAuth TRANSPORT_CLIENT_AUTH_DEFAULT = SSLClientAuth.REQUIRED;
    public static final String TRANSPORT_PROFILE_SSL_SETTING = "shield.ssl";
    public static final String TRANSPORT_PROFILE_CLIENT_AUTH_SETTING = "shield.ssl.client.auth";

    private final ServerSSLService serverSslService;
    private final ClientSSLService clientSSLService;
    private final ShieldSettingsFilter settingsFilter;
    private final @Nullable IPFilter authenticator;
    private final boolean ssl;

    @Inject
    public ShieldNettyTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays, Version version,
                                @Nullable IPFilter authenticator, @Nullable ServerSSLService serverSSLService, ClientSSLService clientSSLService,
                                ShieldSettingsFilter settingsFilter, NamedWriteableRegistry namedWriteableRegistry) {
        super(settings, threadPool, networkService, bigArrays, version, namedWriteableRegistry);
        this.authenticator = authenticator;
        this.ssl = settings.getAsBoolean(TRANSPORT_SSL_SETTING, TRANSPORT_SSL_DEFAULT);
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

    @Override
    protected void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
        if (!lifecycle.started()) {
            return;
        }

        Throwable t = e.getCause();
        if (isNotSslRecordException(t)) {
            if (logger.isTraceEnabled()) {
                logger.trace("received plaintext traffic on a encrypted channel, closing connection {}", t, ctx.getChannel());
            } else {
                logger.warn("received plaintext traffic on a encrypted channel, closing connection {}", ctx.getChannel());
            }
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else if (isCloseDuringHandshakeException(t)) {
            if (logger.isTraceEnabled()) {
                logger.trace("connection {} closed during handshake", t, ctx.getChannel());
            } else {
                logger.warn("connection {} closed during handshake", ctx.getChannel());
            }
            ctx.getChannel().close();
            disconnectFromNodeChannel(ctx.getChannel(), e.getCause());
        } else {
            super.exceptionCaught(ctx, e);
        }
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
            final boolean profileSsl = profileSettings.getAsBoolean(TRANSPORT_PROFILE_SSL_SETTING, ssl);
            final SSLClientAuth clientAuth = SSLClientAuth.parse(profileSettings.get(TRANSPORT_PROFILE_CLIENT_AUTH_SETTING, settings.get(TRANSPORT_CLIENT_AUTH_SETTING)), TRANSPORT_CLIENT_AUTH_DEFAULT);
            if (profileSsl) {
                SSLEngine serverEngine;
                if (profileSettings.get("shield.truststore.path") != null) {
                    serverEngine = serverSslService.createSSLEngine(profileSettings.getByPrefix("shield."));
                } else {
                    serverEngine = serverSslService.createSSLEngine();
                }
                serverEngine.setUseClientMode(false);
                clientAuth.configure(serverEngine);

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
                    sslEngine = clientSSLService.createSSLEngine(Settings.EMPTY, getHostname(inetSocketAddress), inetSocketAddress.getPort());

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

            @SuppressForbidden(reason = "need to use getHostName to resolve DNS name for SSL connections and hostname verification")
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
