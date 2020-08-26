/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.BytesChannelContext;
import org.elasticsearch.nio.Config;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioChannelHandler;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.nio.SocketChannelContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.nio.NioGroupFactory;
import org.elasticsearch.transport.nio.NioTcpChannel;
import org.elasticsearch.transport.nio.NioTcpServerChannel;
import org.elasticsearch.transport.nio.NioTransport;
import org.elasticsearch.transport.nio.TcpReadWriteHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.transport.ProfileConfigurations;
import org.elasticsearch.xpack.core.security.transport.SecurityTransportExceptionHandler;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * This transport provides a transport based on nio that is secured by SSL/TLS. SSL/TLS is a communications
 * protocol that allows two channels to go through a handshake process prior to application data being
 * exchanged. The handshake process enables the channels to exchange parameters that will allow them to
 * encrypt the application data they exchange.
 * <p>
 * The specific SSL/TLS parameters and configurations are setup in the {@link SSLService} class. The actual
 * implementation of the SSL/TLS layer is in the {@link SSLChannelContext} and {@link SSLDriver} classes.
 */
public class SecurityNioTransport extends NioTransport {
    private static final Logger logger = LogManager.getLogger(SecurityNioTransport.class);

    private final SecurityTransportExceptionHandler exceptionHandler;
    private final IPFilter ipFilter;
    private final SSLService sslService;
    private final Map<String, SSLConfiguration> profileConfiguration;
    private final boolean sslEnabled;

    public SecurityNioTransport(Settings settings, Version version, ThreadPool threadPool, NetworkService networkService,
                                PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                                CircuitBreakerService circuitBreakerService, @Nullable final IPFilter ipFilter,
                                SSLService sslService, NioGroupFactory groupFactory) {
        super(settings, version, threadPool, networkService, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService,
            groupFactory);
        this.exceptionHandler = new SecurityTransportExceptionHandler(logger, lifecycle, (c, e) -> super.onException(c, e));
        this.ipFilter = ipFilter;
        this.sslService = sslService;
        this.sslEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        if (sslEnabled) {
            final SSLConfiguration transportConfiguration = sslService.getSSLConfiguration(setting("transport.ssl."));
            Map<String, SSLConfiguration> profileConfiguration = ProfileConfigurations.get(settings, sslService, transportConfiguration);
            this.profileConfiguration = Collections.unmodifiableMap(profileConfiguration);
        } else {
            profileConfiguration = Collections.emptyMap();
        }
    }

    @Override
    protected void doStart() {
        super.doStart();
        if (ipFilter != null) {
            ipFilter.setBoundTransportAddress(boundAddress(), profileBoundAddresses());
        }
    }

    @Override
    public void onException(TcpChannel channel, Exception e) {
        exceptionHandler.accept(channel, e);
    }

    @Override
    protected TcpChannelFactory serverChannelFactory(ProfileSettings profileSettings) {
        return new SecurityTcpChannelFactory(profileSettings, false);
    }

    @Override
    protected Function<DiscoveryNode, TcpChannelFactory> clientChannelFactoryFunction(ProfileSettings profileSettings) {
        return (node) -> {
            SNIHostName serverName;
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
            return new SecurityClientTcpChannelFactory(profileSettings, serverName);
        };
    }

    @Override
    public boolean isSecure() {
        return this.sslEnabled;
    }

    private class SecurityTcpChannelFactory extends TcpChannelFactory {

        private final String profileName;
        private final boolean isClient;

        private SecurityTcpChannelFactory(ProfileSettings profileSettings, boolean isClient) {
            super(profileSettings);
            this.profileName = profileSettings.profileName;
            this.isClient = isClient;
        }

        @Override
        public NioTcpChannel createChannel(NioSelector selector, SocketChannel channel, Config.Socket socketConfig) throws IOException {
            NioTcpChannel nioChannel = new NioTcpChannel(isClient == false, profileName, channel);
            TcpReadWriteHandler readWriteHandler = new TcpReadWriteHandler(nioChannel, pageCacheRecycler, SecurityNioTransport.this);
            final NioChannelHandler handler;
            if (ipFilter != null) {
                handler = new NioIPFilter(readWriteHandler, socketConfig.getRemoteAddress(), ipFilter, profileName);
            } else {
                handler = readWriteHandler;
            }
            InboundChannelBuffer networkBuffer = new InboundChannelBuffer(pageAllocator);
            Consumer<Exception> exceptionHandler = (e) -> onException(nioChannel, e);

            SocketChannelContext context;
            if (sslEnabled) {
                SSLDriver sslDriver = new SSLDriver(createSSLEngine(socketConfig), pageAllocator, isClient);
                InboundChannelBuffer applicationBuffer = new InboundChannelBuffer(pageAllocator);
                context = new SSLChannelContext(nioChannel, selector, socketConfig, exceptionHandler, sslDriver, handler, networkBuffer,
                    applicationBuffer);
            } else {
                context = new BytesChannelContext(nioChannel, selector, socketConfig, exceptionHandler, handler, networkBuffer);
            }
            nioChannel.setContext(context);

            return nioChannel;
        }

        @Override
        public NioTcpServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel,
                                                       Config.ServerSocket socketConfig) {
            NioTcpServerChannel nioChannel = new NioTcpServerChannel(channel);
            Consumer<Exception> exceptionHandler = (e) -> onServerException(nioChannel, e);
            Consumer<NioSocketChannel> acceptor = SecurityNioTransport.this::acceptChannel;
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, socketConfig, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }

        protected SSLEngine createSSLEngine(Config.Socket socketConfig) throws IOException {
            SSLEngine sslEngine;
            SSLConfiguration defaultConfig = profileConfiguration.get(TransportSettings.DEFAULT_PROFILE);
            SSLConfiguration sslConfig = profileConfiguration.getOrDefault(profileName, defaultConfig);
            boolean hostnameVerificationEnabled = sslConfig.verificationMode().isHostnameVerificationEnabled();
            if (hostnameVerificationEnabled && socketConfig.isAccepted() == false) {
                InetSocketAddress remoteAddress = socketConfig.getRemoteAddress();
                // we create the socket based on the name given. don't reverse DNS
                sslEngine = sslService.createSSLEngine(sslConfig, remoteAddress.getHostString(), remoteAddress.getPort());
            } else {
                sslEngine = sslService.createSSLEngine(sslConfig, null, -1);
            }
            return sslEngine;
        }
    }

    private class SecurityClientTcpChannelFactory extends SecurityTcpChannelFactory {

        private final SNIHostName serverName;

        private SecurityClientTcpChannelFactory(ProfileSettings profileSettings, SNIHostName serverName) {
            super(profileSettings, true);
            this.serverName = serverName;
        }

        @Override
        public NioTcpServerChannel createServerChannel(NioSelector selector, ServerSocketChannel channel,
                                                       Config.ServerSocket socketConfig) {
            throw new AssertionError("Cannot create TcpServerChannel with client factory");
        }

        @Override
        protected SSLEngine createSSLEngine(Config.Socket socketConfig) throws IOException {
            SSLEngine sslEngine = super.createSSLEngine(socketConfig);
            if (serverName != null) {
                SSLParameters sslParameters = sslEngine.getSSLParameters();
                sslParameters.setServerNames(Collections.singletonList(serverName));
                sslEngine.setSSLParameters(sslParameters);
            }
            return sslEngine;
        }
    }
}
