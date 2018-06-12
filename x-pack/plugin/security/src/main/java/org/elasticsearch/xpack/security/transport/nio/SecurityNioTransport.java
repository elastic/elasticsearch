/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.nio;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.nio.InboundChannelBuffer;
import org.elasticsearch.nio.NioSocketChannel;
import org.elasticsearch.nio.ServerChannelContext;
import org.elasticsearch.nio.NioSelector;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.nio.NioTransport;
import org.elasticsearch.transport.nio.TcpNioServerSocketChannel;
import org.elasticsearch.transport.nio.TcpNioSocketChannel;
import org.elasticsearch.transport.nio.TcpReadWriteHandler;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;

/**
 * This transport provides a transport based on nio that is secured by SSL/TLS. SSL/TLS is a communications
 * protocol that allows two channels to go through a handshake process prior to application data being
 * exchanged. The handshake process enables the channels to exchange parameters that will allow them to
 * encrypt the application data they exchange.
 *
 * The specific SSL/TLS parameters and configurations are setup in the {@link SSLService} class. The actual
 * implementation of the SSL/TLS layer is in the {@link SSLChannelContext} and {@link SSLDriver} classes.
 */
public class SecurityNioTransport extends NioTransport {

    private final SSLConfiguration sslConfiguration;
    private final SSLService sslService;
    private final Map<String, SSLConfiguration> profileConfiguration;
    private final boolean sslEnabled;

    SecurityNioTransport(Settings settings, ThreadPool threadPool, NetworkService networkService, BigArrays bigArrays,
                         PageCacheRecycler pageCacheRecycler, NamedWriteableRegistry namedWriteableRegistry,
                         CircuitBreakerService circuitBreakerService, SSLService sslService) {
        super(settings, threadPool, networkService, bigArrays, pageCacheRecycler, namedWriteableRegistry, circuitBreakerService);
        this.sslService = sslService;
        this.sslEnabled = XPackSettings.TRANSPORT_SSL_ENABLED.get(settings);
        final Settings transportSSLSettings = settings.getByPrefix(setting("transport.ssl."));
        if (sslEnabled) {
            this.sslConfiguration = sslService.sslConfiguration(transportSSLSettings, Settings.EMPTY);
            Map<String, Settings> profileSettingsMap = settings.getGroups("transport.profiles.", true);
            Map<String, SSLConfiguration> profileConfiguration = new HashMap<>(profileSettingsMap.size() + 1);
            for (Map.Entry<String, Settings> entry : profileSettingsMap.entrySet()) {
                Settings profileSettings = entry.getValue();
                final Settings profileSslSettings = SecurityNetty4Transport.profileSslSettings(profileSettings);
                SSLConfiguration configuration =  sslService.sslConfiguration(profileSslSettings, transportSSLSettings);
                profileConfiguration.put(entry.getKey(), configuration);
            }

            if (profileConfiguration.containsKey(TcpTransport.DEFAULT_PROFILE) == false) {
                profileConfiguration.put(TcpTransport.DEFAULT_PROFILE, sslConfiguration);
            }

            this.profileConfiguration = Collections.unmodifiableMap(profileConfiguration);
        } else {
            throw new IllegalArgumentException("Currently only support SSL enabled.");
        }
    }

    @Override
    protected TcpChannelFactory channelFactory(ProfileSettings profileSettings, boolean isClient) {
        return new SecurityTcpChannelFactory(profileSettings, isClient);
    }

    @Override
    protected void acceptChannel(NioSocketChannel channel) {
        super.acceptChannel(channel);
    }

    @Override
    protected void exceptionCaught(NioSocketChannel channel, Exception exception) {
        super.exceptionCaught(channel, exception);
    }

    private class SecurityTcpChannelFactory extends TcpChannelFactory {

        private final String profileName;
        private final boolean isClient;

        private SecurityTcpChannelFactory(ProfileSettings profileSettings, boolean isClient) {
            super(new RawChannelFactory(profileSettings.tcpNoDelay,
                    profileSettings.tcpKeepAlive,
                    profileSettings.reuseAddress,
                    Math.toIntExact(profileSettings.sendBufferSize.getBytes()),
                    Math.toIntExact(profileSettings.receiveBufferSize.getBytes())));
            this.profileName = profileSettings.profileName;
            this.isClient = isClient;
        }

        @Override
        public TcpNioSocketChannel createChannel(NioSelector selector, SocketChannel channel) throws IOException {
            SSLConfiguration defaultConfig = profileConfiguration.get(TcpTransport.DEFAULT_PROFILE);
            SSLEngine sslEngine = sslService.createSSLEngine(profileConfiguration.getOrDefault(profileName, defaultConfig), null, -1);
            SSLDriver sslDriver = new SSLDriver(sslEngine, isClient);
            TcpNioSocketChannel nioChannel = new TcpNioSocketChannel(profileName, channel);
            Supplier<InboundChannelBuffer.Page> pageSupplier = () -> {
                Recycler.V<byte[]> bytes = pageCacheRecycler.bytePage(false);
                return new InboundChannelBuffer.Page(ByteBuffer.wrap(bytes.v()), bytes::close);
            };

            TcpReadWriteHandler readWriteHandler = new TcpReadWriteHandler(nioChannel, SecurityNioTransport.this);
            InboundChannelBuffer buffer = new InboundChannelBuffer(pageSupplier);
            Consumer<Exception> exceptionHandler = (e) -> exceptionCaught(nioChannel, e);
            SSLChannelContext context = new SSLChannelContext(nioChannel, selector, exceptionHandler, sslDriver, readWriteHandler, buffer);
            nioChannel.setContext(context);
            return nioChannel;
        }

        @Override
        public TcpNioServerSocketChannel createServerChannel(NioSelector selector, ServerSocketChannel channel) throws IOException {
            TcpNioServerSocketChannel nioChannel = new TcpNioServerSocketChannel(profileName, channel);
            Consumer<Exception> exceptionHandler = (e) -> logger.error(() ->
                new ParameterizedMessage("exception from server channel caught on transport layer [{}]", channel), e);
            Consumer<NioSocketChannel> acceptor = SecurityNioTransport.this::acceptChannel;
            ServerChannelContext context = new ServerChannelContext(nioChannel, this, selector, acceptor, exceptionHandler);
            nioChannel.setContext(context);
            return nioChannel;
        }
    }
}
