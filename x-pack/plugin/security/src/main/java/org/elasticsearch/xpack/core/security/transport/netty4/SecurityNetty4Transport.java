/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOption;
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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteClusterPortSettings;
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

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_ENABLED;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_PREFIX;
import static org.elasticsearch.xpack.core.XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED;

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
    private final boolean remoteClusterServerSslEnabled;
    private final SslConfiguration remoteClusterClientSslConfiguration;
    private final RemoteClusterClientBootstrapOptions remoteClusterClientBootstrapOptions;

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
        this.remoteClusterPortEnabled = REMOTE_CLUSTER_SERVER_ENABLED.get(settings);
        this.remoteClusterServerSslEnabled = REMOTE_CLUSTER_SERVER_SSL_ENABLED.get(settings);
        this.profileConfigurations = Collections.unmodifiableMap(ProfileConfigurations.get(settings, sslService, true));
        this.defaultSslConfiguration = this.profileConfigurations.get(TransportSettings.DEFAULT_PROFILE);
        assert this.transportSslEnabled == false || this.defaultSslConfiguration != null;

        // Client configuration does not depend on whether the remote access port is enabled
        if (REMOTE_CLUSTER_CLIENT_SSL_ENABLED.get(settings)) {
            this.remoteClusterClientSslConfiguration = sslService.getSSLConfiguration(REMOTE_CLUSTER_CLIENT_SSL_PREFIX);
            assert this.remoteClusterClientSslConfiguration != null;
        } else {
            this.remoteClusterClientSslConfiguration = null;
        }
        this.remoteClusterClientBootstrapOptions = RemoteClusterClientBootstrapOptions.fromSettings(settings);
    }

    @Override
    protected void doStart() {
        super.doStart();
    }

    @Override
    public final ChannelHandler getServerChannelInitializer(String name) {
        if (remoteClusterPortEnabled && REMOTE_CLUSTER_PROFILE.equals(name)) {
            if (remoteClusterServerSslEnabled) {
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
    protected Bootstrap getClientBootstrap(ConnectionProfile connectionProfile) {
        final Bootstrap bootstrap = super.getClientBootstrap(connectionProfile);
        if (false == REMOTE_CLUSTER_PROFILE.equals(connectionProfile.getTransportProfile())
            || remoteClusterClientBootstrapOptions.isEmpty()) {
            return bootstrap;
        }

        logger.trace("reconfiguring client bootstrap for remote cluster client connection");
        // Only client connections to a new RCS remote cluster can have transport profile of _remote_cluster
        // All other client connections use the default transport profile regardless of the transport profile used on the server side.
        remoteClusterClientBootstrapOptions.configure(bootstrap);
        return bootstrap;
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

    // This class captures the differences of client side TCP network settings between default and _remote_cluster transport profiles.
    // A field will be null if there is no difference between associated settings of the two profiles. It has a non-null value only
    // when the _remote_cluster profile has a different value from the default profile.
    record RemoteClusterClientBootstrapOptions(
        Boolean tcpNoDelay,
        Boolean tcpKeepAlive,
        Integer tcpKeepIdle,
        Integer tcpKeepInterval,
        Integer tcpKeepCount,
        ByteSizeValue tcpSendBufferSize,
        ByteSizeValue tcpReceiveBufferSize,
        Boolean tcpReuseAddress
    ) {

        boolean isEmpty() {
            return tcpNoDelay == null
                && tcpKeepAlive == null
                && tcpKeepIdle == null
                && tcpKeepInterval == null
                && tcpKeepCount == null
                && tcpSendBufferSize == null
                && tcpReceiveBufferSize == null
                && tcpReuseAddress == null;
        }

        void configure(Bootstrap bootstrap) {
            if (tcpNoDelay != null) {
                bootstrap.option(ChannelOption.TCP_NODELAY, tcpNoDelay);
            }

            if (tcpKeepAlive != null) {
                bootstrap.option(ChannelOption.SO_KEEPALIVE, tcpKeepAlive);
                if (tcpKeepAlive) {
                    // Note that Netty logs a warning if it can't set the option
                    if (tcpKeepIdle != null) {
                        if (tcpKeepIdle >= 0) {
                            bootstrap.option(OPTION_TCP_KEEP_IDLE, tcpKeepIdle);
                        } else {
                            bootstrap.option(OPTION_TCP_KEEP_IDLE, null);
                        }
                    }
                    if (tcpKeepInterval != null) {
                        if (tcpKeepInterval >= 0) {
                            bootstrap.option(OPTION_TCP_KEEP_INTERVAL, tcpKeepInterval);
                        } else {
                            bootstrap.option(OPTION_TCP_KEEP_INTERVAL, null);
                        }
                    }
                    if (tcpKeepCount != null) {
                        if (tcpKeepCount >= 0) {
                            bootstrap.option(OPTION_TCP_KEEP_COUNT, tcpKeepCount);
                        } else {
                            bootstrap.option(OPTION_TCP_KEEP_COUNT, null);
                        }
                    }
                } else {
                    bootstrap.option(OPTION_TCP_KEEP_IDLE, null);
                    bootstrap.option(OPTION_TCP_KEEP_INTERVAL, null);
                    bootstrap.option(OPTION_TCP_KEEP_COUNT, null);
                }
            }

            if (tcpSendBufferSize != null) {
                if (tcpSendBufferSize.getBytes() > 0) {
                    bootstrap.option(ChannelOption.SO_SNDBUF, Math.toIntExact(tcpSendBufferSize.getBytes()));
                } else {
                    bootstrap.option(ChannelOption.SO_SNDBUF, null);
                }
            }

            if (tcpReceiveBufferSize != null) {
                if (tcpReceiveBufferSize.getBytes() > 0) {
                    bootstrap.option(ChannelOption.SO_RCVBUF, Math.toIntExact(tcpReceiveBufferSize.getBytes()));
                } else {
                    bootstrap.option(ChannelOption.SO_RCVBUF, null);
                }
            }

            if (tcpReuseAddress != null) {
                bootstrap.option(ChannelOption.SO_REUSEADDR, tcpReuseAddress);
            }
        }

        static RemoteClusterClientBootstrapOptions fromSettings(Settings settings) {
            Boolean tcpNoDelay = RemoteClusterPortSettings.TCP_NO_DELAY.get(settings);
            if (tcpNoDelay == TransportSettings.TCP_NO_DELAY.get(settings)) {
                tcpNoDelay = null;
            }

            // It is possible that both default and _remote_cluster enable keepAlive but have different
            // values for either keepIdle, keepInterval or keepCount. In this case, we need have a
            // non-null value for keepAlive even it is the same between default and _remote_cluster.
            Boolean tcpKeepAlive = RemoteClusterPortSettings.TCP_KEEP_ALIVE.get(settings);
            Integer tcpKeepIdle = RemoteClusterPortSettings.TCP_KEEP_IDLE.get(settings);
            Integer tcpKeepInterval = RemoteClusterPortSettings.TCP_KEEP_INTERVAL.get(settings);
            Integer tcpKeepCount = RemoteClusterPortSettings.TCP_KEEP_COUNT.get(settings);
            final Boolean defaultTcpKeepAlive = TransportSettings.TCP_KEEP_ALIVE.get(settings);

            if (tcpKeepAlive) {
                if (defaultTcpKeepAlive) {
                    // Both profiles have keepAlive enabled, we need to check whether any keepIdle, keepInterval, keepCount is different
                    if (tcpKeepIdle.equals(TransportSettings.TCP_KEEP_IDLE.get(settings))) {
                        tcpKeepIdle = null;
                    }
                    if (tcpKeepInterval.equals(TransportSettings.TCP_KEEP_INTERVAL.get(settings))) {
                        tcpKeepInterval = null;
                    }
                    if (tcpKeepCount.equals(TransportSettings.TCP_KEEP_COUNT.get(settings))) {
                        tcpKeepCount = null;
                    }
                    if (tcpKeepIdle == null && tcpKeepInterval == null && tcpKeepCount == null) {
                        // If keepIdle, keepInterval, keepCount are all identical, keepAlive can be null as well.
                        // That is no need to update anything keepXxx related
                        tcpKeepAlive = null;
                    }
                }
            } else {
                if (false == defaultTcpKeepAlive) {
                    tcpKeepAlive = null;
                }
                // _remote_cluster has keepAlive disabled, all other keepXxx has no reason to exist
                tcpKeepIdle = null;
                tcpKeepInterval = null;
                tcpKeepCount = null;
            }

            assert (tcpKeepAlive == null && tcpKeepIdle == null && tcpKeepInterval == null && tcpKeepCount == null)
                || (tcpKeepAlive == false && tcpKeepIdle == null && tcpKeepInterval == null && tcpKeepCount == null)
                || (tcpKeepAlive && (tcpKeepIdle != null || tcpKeepInterval != null || tcpKeepCount != null))
                : "keepAlive == true must be accompanied with either keepIdle, keepInterval or keepCount change";

            ByteSizeValue tcpSendBufferSize = RemoteClusterPortSettings.TCP_SEND_BUFFER_SIZE.get(settings);
            if (tcpSendBufferSize.equals(TransportSettings.TCP_SEND_BUFFER_SIZE.get(settings))) {
                tcpSendBufferSize = null;
            }

            ByteSizeValue tcpReceiveBufferSize = RemoteClusterPortSettings.TCP_RECEIVE_BUFFER_SIZE.get(settings);
            if (tcpReceiveBufferSize.equals(TransportSettings.TCP_RECEIVE_BUFFER_SIZE.get(settings))) {
                tcpReceiveBufferSize = null;
            }

            Boolean tcpReuseAddress = RemoteClusterPortSettings.TCP_REUSE_ADDRESS.get(settings);
            if (tcpReuseAddress == TransportSettings.TCP_REUSE_ADDRESS.get(settings)) {
                tcpReuseAddress = null;
            }

            return new RemoteClusterClientBootstrapOptions(
                tcpNoDelay,
                tcpKeepAlive,
                tcpKeepIdle,
                tcpKeepInterval,
                tcpKeepCount,
                tcpSendBufferSize,
                tcpReceiveBufferSize,
                tcpReuseAddress
            );
        }
    }
}
