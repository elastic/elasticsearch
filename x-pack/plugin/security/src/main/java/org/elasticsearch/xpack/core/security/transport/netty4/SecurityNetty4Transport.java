/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.transport.ProfileConfigurations;
import org.elasticsearch.xpack.core.security.transport.SecurityTransportExceptionHandler;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.Map;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PORT_ENABLED;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_SSL_ENABLED;

/**
 * Implementation of a transport that extends the {@link Netty4Transport} to add SSL and IP Filtering
 */
public class SecurityNetty4Transport extends Netty4Transport {
    private static final Logger logger = LogManager.getLogger(SecurityNetty4Transport.class);

    private final SecurityTransportExceptionHandler exceptionHandler;
    private final SSLService sslService;
    private final SslConfiguration defaultSslConfiguration;
    private final Map<String, SslConfiguration> profileConfigurations;
    private final boolean transportSslEnabled;
    private final boolean remoteClusterPortEnabled;
    private final boolean remoteClusterSslEnabled;
    private final SslConfiguration remoteClusterClientSslConfiguration;

    public SecurityNetty4Transport(
        final Settings settings,
        final TransportVersion version,
        final ThreadPool threadPool,
        final NetworkService networkService,
        final PageCacheRecycler pageCacheRecycler,
        final NamedWriteableRegistry namedWriteableRegistry,
        final CircuitBreakerService circuitBreakerService,
        final SSLService sslService,
        final SharedGroupFactory sharedGroupFactory
    ) {
        super(
            settings,
            version,
            threadPool,
            networkService,
            pageCacheRecycler,
            namedWriteableRegistry,
            circuitBreakerService,
            sharedGroupFactory
        );
        this.exceptionHandler = new SecurityTransportExceptionHandler(logger, lifecycle, (c, e) -> super.onException(c, e));
        this.sslService = sslService;
        this.transportSslEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        this.remoteClusterPortEnabled = REMOTE_CLUSTER_PORT_ENABLED.get(settings);
        this.remoteClusterSslEnabled = REMOTE_CLUSTER_SSL_ENABLED.get(settings);

        this.profileConfigurations = Collections.unmodifiableMap(ProfileConfigurations.get(settings, sslService, true));
        this.defaultSslConfiguration = this.profileConfigurations.get(TransportSettings.DEFAULT_PROFILE);
        assert this.transportSslEnabled == false || this.defaultSslConfiguration != null;

        // Client configuration does not depend on whether the remote access port is enabled
        if (remoteClusterSslEnabled) {
            this.remoteClusterClientSslConfiguration = sslService.getSSLConfiguration(XPackSettings.REMOTE_CLUSTER_SSL_PREFIX);
            assert this.remoteClusterClientSslConfiguration != null;
        } else {
            this.remoteClusterClientSslConfiguration = null;
        }
    }

    @Override
    protected void doStart() {
        super.doStart();
    }

    @Override
    public final ChannelHandler getServerChannelInitializer(String name) {
        if (remoteClusterPortEnabled && REMOTE_CLUSTER_PROFILE.equals(name)) {
            if (remoteClusterSslEnabled) {
                final SslConfiguration remoteClusterSslConfiguration = profileConfigurations.get(name);
                if (remoteClusterSslConfiguration == null) {
                    throw new IllegalStateException("remote cluster SSL is enabled but no configuration is found");
                }
                return getSslChannelInitializer(name, remoteClusterSslConfiguration);
            } else {
                return getNoSslChannelInitializer(name);
            }
        } else if (transportSslEnabled) {
            SslConfiguration configuration = profileConfigurations.get(name);
            if (configuration == null) {
                throw new IllegalStateException("unknown profile: " + name);
            }
            return getSslChannelInitializer(name, configuration);
        } else {
            return getNoSslChannelInitializer(name);
        }
    }

    protected ChannelHandler getNoSslChannelInitializer(final String name) {
        return super.getServerChannelInitializer(name);
    }

    @Override
    protected ChannelHandler getClientChannelInitializer(DiscoveryNode node, ConnectionProfile connectionProfile) {
        return new SecurityClientChannelInitializer(node, connectionProfile);
    }

    @Override
    public void onException(TcpChannel channel, Exception e) {
        exceptionHandler.accept(channel, e);
    }

    public class SslChannelInitializer extends ServerChannelInitializer {
        private final SslConfiguration configuration;

        public SslChannelInitializer(String name, SslConfiguration configuration) {
            super(name);
            this.configuration = configuration;
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            SSLEngine serverEngine = sslService.createSSLEngine(configuration, null, -1);
            serverEngine.setUseClientMode(false);
            final SslHandler sslHandler = new SslHandler(serverEngine);
            ch.pipeline().addFirst("sslhandler", sslHandler);
            super.initChannel(ch);
            assert ch.pipeline().first() == sslHandler : "SSL handler must be first handler in pipeline";
        }
    }

    protected ServerChannelInitializer getSslChannelInitializer(final String name, final SslConfiguration configuration) {
        return new SslChannelInitializer(name, configuration);
    }

    @Override
    public boolean isSecure() {
        return this.transportSslEnabled;
    }

    private class SecurityClientChannelInitializer extends ClientChannelInitializer {

        private final boolean hostnameVerificationEnabled;
        private final SNIHostName serverName;
        private final SslConfiguration channelSslConfiguration;

        SecurityClientChannelInitializer(DiscoveryNode node, ConnectionProfile connectionProfile) {
            final String transportProfile = connectionProfile.getTransportProfile();
            logger.trace("initiating security client channel with transport profile [{}]", transportProfile);
            // Only client connections to a new RCS remote cluster can have transport profile of _remote_cluster
            // All other client connections use the default transport profile regardless of the transport profile used on the server side.
            if (REMOTE_CLUSTER_PROFILE.equals(transportProfile)) {
                this.channelSslConfiguration = remoteClusterClientSslConfiguration;
            } else {
                assert TransportSettings.DEFAULT_PROFILE.equals(transportProfile);
                this.channelSslConfiguration = defaultSslConfiguration;
            }
            if (this.channelSslConfiguration != null) {
                this.hostnameVerificationEnabled = this.channelSslConfiguration.verificationMode().isHostnameVerificationEnabled();
            } else {
                this.hostnameVerificationEnabled = false;
            }
            String configuredServerName = node.getAttributes().get("server_name");
            if (configuredServerName != null) {
                try {
                    serverName = new SNIHostName(configuredServerName);
                } catch (IllegalArgumentException e) {
                    throw new ConnectTransportException(node, "invalid DiscoveryNode server_name [" + configuredServerName + "]", e);
                }
            } else {
                serverName = null;
            }
        }

        @Override
        protected void initChannel(Channel ch) throws Exception {
            super.initChannel(ch);
            if (channelSslConfiguration != null) {
                ch.pipeline()
                    .addFirst(
                        new ClientSslHandlerInitializer(channelSslConfiguration, sslService, hostnameVerificationEnabled, serverName)
                    );
            }
        }
    }

    private static class ClientSslHandlerInitializer extends ChannelOutboundHandlerAdapter {

        private final boolean hostnameVerificationEnabled;
        private final SslConfiguration sslConfiguration;
        private final SSLService sslService;
        private final SNIServerName serverName;

        private ClientSslHandlerInitializer(
            SslConfiguration sslConfiguration,
            SSLService sslService,
            boolean hostnameVerificationEnabled,
            SNIServerName serverName
        ) {
            this.sslConfiguration = sslConfiguration;
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            this.sslService = sslService;
            this.serverName = serverName;
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise)
            throws Exception {
            final SSLEngine sslEngine;
            if (hostnameVerificationEnabled) {
                InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
                // we create the socket based on the name given. don't reverse DNS
                sslEngine = sslService.createSSLEngine(sslConfiguration, inetSocketAddress.getHostString(), inetSocketAddress.getPort());
            } else {
                sslEngine = sslService.createSSLEngine(sslConfiguration, null, -1);
            }

            sslEngine.setUseClientMode(true);
            if (serverName != null) {
                SSLParameters sslParameters = sslEngine.getSSLParameters();
                sslParameters.setServerNames(Collections.singletonList(serverName));
                sslEngine.setSSLParameters(sslParameters);
            }
            final ChannelPromise connectPromise = ctx.newPromise();
            final SslHandler sslHandler = new SslHandler(sslEngine);
            ctx.pipeline().replace(this, "ssl", sslHandler);
            final Future<?> handshakePromise = sslHandler.handshakeFuture();
            connectPromise.addListener(result -> {
                if (result.isSuccess() == false) {
                    promise.tryFailure(result.cause());
                } else {
                    handshakePromise.addListener(handshakeResult -> {
                        if (handshakeResult.isSuccess()) {
                            promise.setSuccess();
                        } else {
                            promise.tryFailure(handshakeResult.cause());
                        }
                    });
                }
            });
            super.connect(ctx, remoteAddress, localAddress, connectPromise);
        }
    }
}
