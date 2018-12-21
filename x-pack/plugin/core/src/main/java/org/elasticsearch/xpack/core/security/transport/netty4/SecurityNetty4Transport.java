/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.transport.netty4;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelOutboundHandlerAdapter;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslHandler;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.CloseableChannel;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.transport.SSLExceptionHelper;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * Implementation of a transport that extends the {@link Netty4Transport} to add SSL and IP Filtering
 */
public class SecurityNetty4Transport extends Netty4Transport {
    private static final Logger logger = LogManager.getLogger(SecurityNetty4Transport.class);

    private final SSLService sslService;
    private final SSLConfiguration sslConfiguration;
    private final Map<String, SSLConfiguration> profileConfiguration;
    private final boolean sslEnabled;

    public SecurityNetty4Transport(
            final Settings settings,
            final Version version,
            final ThreadPool threadPool,
            final NetworkService networkService,
            final PageCacheRecycler pageCacheRecycler,
            final NamedWriteableRegistry namedWriteableRegistry,
            final CircuitBreakerService circuitBreakerService,
            final SSLService sslService) {
        super(settings, version, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService);
        this.sslService = sslService;
        this.sslEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        if (sslEnabled) {
            this.sslConfiguration = sslService.getSSLConfiguration(setting("transport.ssl."));
            Map<String, SSLConfiguration> profileConfiguration = getTransportProfileConfigurations(settings, sslService, sslConfiguration);
            this.profileConfiguration = Collections.unmodifiableMap(profileConfiguration);
        } else {
            this.profileConfiguration = Collections.emptyMap();
            this.sslConfiguration = null;
        }
    }

    public static Map<String, SSLConfiguration> getTransportProfileConfigurations(Settings settings, SSLService sslService,
                                                                                  SSLConfiguration defaultConfiguration) {
        Set<String> profileNames = settings.getGroups("transport.profiles.", true).keySet();
        Map<String, SSLConfiguration> profileConfiguration = new HashMap<>(profileNames.size() + 1);
        for (String profileName : profileNames) {
            SSLConfiguration configuration = sslService.getSSLConfiguration("transport.profiles." + profileName + "." + setting("ssl"));
            profileConfiguration.put(profileName, configuration);
        }

        if (profileConfiguration.containsKey(TransportSettings.DEFAULT_PROFILE) == false) {
            profileConfiguration.put(TransportSettings.DEFAULT_PROFILE, defaultConfiguration);
        }
        return profileConfiguration;
    }

    @Override
    protected void doStart() {
        super.doStart();
    }

    @Override
    public final ChannelHandler getServerChannelInitializer(String name) {
        if (sslEnabled) {
            SSLConfiguration configuration = profileConfiguration.get(name);
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
    protected ChannelHandler getClientChannelInitializer(DiscoveryNode node) {
        return new SecurityClientChannelInitializer(node);
    }

    @Override
    public void onException(TcpChannel channel, Exception e) {
        if (!lifecycle.started()) {
            // just close and ignore - we are already stopped and just need to make sure we release all resources
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isNotSslRecordException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                        new ParameterizedMessage("received plaintext traffic on an encrypted channel, closing connection {}", channel), e);
            } else {
                logger.warn("received plaintext traffic on an encrypted channel, closing connection {}", channel);
            }
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isCloseDuringHandshakeException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace(new ParameterizedMessage("connection {} closed during ssl handshake", channel), e);
            } else {
                logger.warn("connection {} closed during handshake", channel);
            }
            CloseableChannel.closeChannel(channel);
        } else if (SSLExceptionHelper.isReceivedCertificateUnknownException(e)) {
            if (logger.isTraceEnabled()) {
                logger.trace(new ParameterizedMessage("client did not trust server's certificate, closing connection {}", channel), e);
            } else {
                logger.warn("client did not trust this server's certificate, closing connection {}", channel);
            }
            CloseableChannel.closeChannel(channel);
        } else {
            super.onException(channel, e);
        }
    }

    public class SslChannelInitializer extends ServerChannelInitializer {
        private final SSLConfiguration configuration;

        public SslChannelInitializer(String name, SSLConfiguration configuration) {
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

    protected ServerChannelInitializer getSslChannelInitializer(final String name, final SSLConfiguration configuration) {
        return new SslChannelInitializer(name, sslConfiguration);
    }

    private class SecurityClientChannelInitializer extends ClientChannelInitializer {

        private final boolean hostnameVerificationEnabled;
        private final SNIHostName serverName;

        SecurityClientChannelInitializer(DiscoveryNode node) {
            this.hostnameVerificationEnabled = sslEnabled && sslConfiguration.verificationMode().isHostnameVerificationEnabled();
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
            if (sslEnabled) {
                ch.pipeline().addFirst(new ClientSslHandlerInitializer(sslConfiguration, sslService, hostnameVerificationEnabled,
                    serverName));
            }
        }
    }

    private static class ClientSslHandlerInitializer extends ChannelOutboundHandlerAdapter {

        private final boolean hostnameVerificationEnabled;
        private final SSLConfiguration sslConfiguration;
        private final SSLService sslService;
        private final SNIServerName serverName;

        private ClientSslHandlerInitializer(SSLConfiguration sslConfiguration, SSLService sslService, boolean hostnameVerificationEnabled,
                                            SNIServerName serverName) {
            this.sslConfiguration = sslConfiguration;
            this.hostnameVerificationEnabled = hostnameVerificationEnabled;
            this.sslService = sslService;
            this.serverName = serverName;
        }

        @Override
        public void connect(ChannelHandlerContext ctx, SocketAddress remoteAddress,
                            SocketAddress localAddress, ChannelPromise promise) throws Exception {
            final SSLEngine sslEngine;
            if (hostnameVerificationEnabled) {
                InetSocketAddress inetSocketAddress = (InetSocketAddress) remoteAddress;
                // we create the socket based on the name given. don't reverse DNS
                sslEngine = sslService.createSSLEngine(sslConfiguration, inetSocketAddress.getHostString(),
                        inetSocketAddress.getPort());
            } else {
                sslEngine = sslService.createSSLEngine(sslConfiguration, null, -1);
            }

            sslEngine.setUseClientMode(true);
            if (serverName != null) {
                SSLParameters sslParameters = sslEngine.getSSLParameters();
                sslParameters.setServerNames(Collections.singletonList(serverName));
                sslEngine.setSSLParameters(sslParameters);
            }
            ctx.pipeline().replace(this, "ssl", new SslHandler(sslEngine));
            super.connect(ctx, remoteAddress, localAddress, promise);
        }
    }
}
