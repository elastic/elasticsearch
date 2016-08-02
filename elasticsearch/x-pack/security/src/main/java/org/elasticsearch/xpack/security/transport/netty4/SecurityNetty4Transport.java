/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.xpack.security.ssl.ClientSSLService;
import org.elasticsearch.xpack.security.ssl.ServerSSLService;
import org.elasticsearch.xpack.security.transport.SSLClientAuth;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.security.Security.settingPrefix;
import static org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport.CLIENT_AUTH_SETTING;
import static org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport.DEPRECATED_PROFILE_SSL_SETTING;
import static org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport.HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING;
import static org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport.HOSTNAME_VERIFICATION_SETTING;
import static org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport.PROFILE_CLIENT_AUTH_SETTING;
import static org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport.PROFILE_SSL_SETTING;
import static org.elasticsearch.xpack.security.transport.netty3.SecurityNetty3Transport.SSL_SETTING;

/**
 * Implementation of a transport that extends the {@link Netty4Transport} to add SSL and IP Filtering
 */
public class SecurityNetty4Transport extends Netty4Transport {

    private final ServerSSLService serverSslService;
    private final ClientSSLService clientSSLService;
    @Nullable private final IPFilter authenticator;
    private final SSLClientAuth clientAuth;
    private final boolean ssl;

    @Inject
    public SecurityNetty4Transport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                                   NamedWriteableRegistry namedWriteableRegistry, CircuitBreakerService circuitBreakerService,
                                   @Nullable IPFilter authenticator, @Nullable ServerSSLService serverSSLService,
                                   ClientSSLService clientSSLService) {
        super(settings, threadPool, networkService, bigArrays, namedWriteableRegistry, circuitBreakerService);
        this.authenticator = authenticator;
        this.ssl = SSL_SETTING.get(settings);
        this.clientAuth = CLIENT_AUTH_SETTING.get(settings);
        this.serverSslService = serverSSLService;
        this.clientSSLService = clientSSLService;
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (authenticator != null) {
            authenticator.setBoundTransportAddress(boundAddress(), profileBoundAddresses());
        }
    }

    @Override
    protected ChannelInitializer<SocketChannel> getServerChannelInitializer(String name, Settings settings) {
        return new SecurityServerChannelInitializer(name, settings);
    }

    @Override
    protected ChannelInitializer<SocketChannel> getClientChannelInitializer() {
        return new SecurityClientChannelInitializer();
    }

    /**
     * This method ensures that all channels have their SSL handshakes completed. This is necessary to prevent the application from
     * writing data while the handshake is in progress which could cause the handshake to fail.
     */
    @Override
    protected void onAfterChannelsConnected(NodeChannels nodeChannels) {
        for (Channel channel : nodeChannels.allChannels) {
            SslHandler handler = channel.pipeline().get(SslHandler.class);
            if (handler != null) {
                handler.handshakeFuture().awaitUninterruptibly(30L, TimeUnit.SECONDS);
                if (!handler.handshakeFuture().isSuccess()) {
                    throw new ElasticsearchException("handshake failed for channel [{}]", channel);
                }
            }
        }
    }

    class SecurityServerChannelInitializer extends ServerChannelInitializer {

        private final boolean sslEnabled;

        protected SecurityServerChannelInitializer(String name, Settings settings) {
            super(name, settings);
            this.sslEnabled = profileSSL(settings, ssl);
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            super.initChannel(ch);
            if (sslEnabled) {
                Settings securityProfileSettings = settings.getByPrefix(settingPrefix());
                SSLEngine serverEngine = serverSslService.createSSLEngine(securityProfileSettings);
                serverEngine.setUseClientMode(false);
                final SSLClientAuth profileClientAuth = profileClientAuth(settings, clientAuth);
                profileClientAuth.configure(serverEngine);
                ch.pipeline().addFirst(new SslHandler(serverEngine));
            }
            if (authenticator != null) {
                ch.pipeline().addFirst(new IpFilterRemoteAddressFilter(authenticator, name));
            }
        }
    }

    class SecurityClientChannelInitializer extends ClientChannelInitializer {
        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            super.initChannel(ch);
            if (ssl) {
                ch.pipeline().addFirst(new ClientSslHandlerInitializer());
            }
        }
    }

    private class ClientSslHandlerInitializer extends ChannelOutboundHandlerAdapter {

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final SSLEngine sslEngine;
            if (HOSTNAME_VERIFICATION_SETTING.get(settings)) {
                InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
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
            ctx.pipeline().replace(this, "ssl", new SslHandler(sslEngine));
            super.connect(ctx, remoteAddress, localAddress, promise);
        }

        @SuppressForbidden(reason = "need to use getHostName to resolve DNS name for SSL connections and hostname verification")
        private String getHostname(InetSocketAddress inetSocketAddress) {
            String hostname;
            if (HOSTNAME_VERIFICATION_RESOLVE_NAME_SETTING.get(settings)) {
                hostname = inetSocketAddress.getHostName();
            } else {
                hostname = inetSocketAddress.getHostString();
            }

            if (logger.isTraceEnabled()) {
                logger.trace("resolved hostname [{}] for address [{}] to be used in ssl hostname verification", hostname,
                        inetSocketAddress);
            }
            return hostname;
        }
    }

    static boolean profileSSL(Settings profileSettings, boolean defaultSSL) {
        if (PROFILE_SSL_SETTING.exists(profileSettings)) {
            return PROFILE_SSL_SETTING.get(profileSettings);
        } else if (DEPRECATED_PROFILE_SSL_SETTING.exists(profileSettings)) {
            return DEPRECATED_PROFILE_SSL_SETTING.get(profileSettings);
        } else {
            return defaultSSL;
        }
    }

    static SSLClientAuth profileClientAuth(Settings settings, SSLClientAuth clientAuth) {
        if (PROFILE_CLIENT_AUTH_SETTING.exists(settings)) {
            return PROFILE_CLIENT_AUTH_SETTING.get(settings);
        }
        return clientAuth;
    }

}
