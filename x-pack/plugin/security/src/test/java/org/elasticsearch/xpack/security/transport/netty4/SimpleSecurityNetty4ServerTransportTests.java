/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.ssl.SslHandler;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.AbstractSimpleSecurityTransportTestCase;

import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.hamcrest.Matchers.containsString;

public class SimpleSecurityNetty4ServerTransportTests extends AbstractSimpleSecurityTransportTestCase {

    private static final ConnectionProfile SINGLE_CHANNEL_PROFILE;

    static {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);
        SINGLE_CHANNEL_PROFILE = builder.build();
    }

    public MockTransportService nettyFromThreadPool(Settings settings, ThreadPool threadPool, final Version version,
                                                    ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Settings settings1 = Settings.builder()
            .put(settings)
            .put("xpack.security.transport.ssl.enabled", true).build();
        Transport transport = new SecurityNetty4ServerTransport(settings1, threadPool,
            networkService, BigArrays.NON_RECYCLING_INSTANCE, namedWriteableRegistry,
            new NoneCircuitBreakerService(), null, createSSLService(settings1)) {

            @Override
            public Version executeHandshake(DiscoveryNode node, TcpChannel channel, TimeValue timeout) throws IOException,
                InterruptedException {
                if (doHandshake) {
                    return super.executeHandshake(node, channel, timeout);
                } else {
                    return version.minimumCompatibilityVersion();
                }
            }

            @Override
            protected Version getCurrentVersion() {
                return version;
            }

        };
        MockTransportService mockTransportService =
            MockTransportService.createNewService(Settings.EMPTY, transport, version, threadPool, clusterSettings,
                Collections.emptySet());
        mockTransportService.start();
        return mockTransportService;
    }

    @Override
    protected MockTransportService build(Settings settings, Version version, ClusterSettings clusterSettings, boolean doHandshake) {
        if (TcpTransport.PORT.exists(settings) == false) {
            settings = Settings.builder().put(settings)
                .put(TcpTransport.PORT.getKey(), "0")
                .build();
        }
        MockTransportService transportService = nettyFromThreadPool(settings, threadPool, version, clusterSettings, doHandshake);
        transportService.start();
        return transportService;
    }

    public void testSNIServerNameIsPropagated() throws Exception {
        SSLService sslService = createSSLService();
        final ServerBootstrap serverBootstrap = new ServerBootstrap();
        boolean success = false;
        try {
            serverBootstrap.group(new NioEventLoopGroup(1));
            serverBootstrap.channel(NioServerSocketChannel.class);

            final String sniIp = "sni-hostname";
            final SNIHostName sniHostName = new SNIHostName(sniIp);
            final CountDownLatch latch = new CountDownLatch(2);
            serverBootstrap.childHandler(new ChannelInitializer<Channel>() {

                @Override
                protected void initChannel(Channel ch) {
                    SSLEngine serverEngine = sslService.createSSLEngine(sslService.getSSLConfiguration(setting("transport.ssl.")),
                        null, -1);
                    serverEngine.setUseClientMode(false);
                    SSLParameters sslParameters = serverEngine.getSSLParameters();
                    sslParameters.setSNIMatchers(Collections.singletonList(new SNIMatcher(0) {
                        @Override
                        public boolean matches(SNIServerName sniServerName) {
                            if (sniHostName.equals(sniServerName)) {
                                latch.countDown();
                                return true;
                            } else {
                                return false;
                            }
                        }
                    }));
                    serverEngine.setSSLParameters(sslParameters);
                    final SslHandler sslHandler = new SslHandler(serverEngine);
                    sslHandler.handshakeFuture().addListener(future -> latch.countDown());
                    ch.pipeline().addFirst("sslhandler", sslHandler);
                }
            });
            serverBootstrap.validate();
            ChannelFuture serverFuture = serverBootstrap.bind(getLocalEphemeral());
            serverFuture.await();
            InetSocketAddress serverAddress = (InetSocketAddress) serverFuture.channel().localAddress();

            try (MockTransportService serviceC = build(
                Settings.builder()
                    .put("name", "TS_TEST")
                    .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), "")
                    .put(TransportService.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
                    .build(),
                version0,
                null, true)) {
                serviceC.acceptIncomingRequests();

                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = new DiscoveryNode("server_node_id", new TransportAddress(serverAddress), attributes,
                    EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);

                new Thread(() -> {
                    try {
                        serviceC.connectToNode(node, SINGLE_CHANNEL_PROFILE);
                    } catch (ConnectTransportException ex) {
                        // Ignore. The other side is not setup to do the ES handshake. So this will fail.
                    }
                }).start();

                latch.await();
                serverBootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS);
                success = true;
            }
        } finally {
            if (success == false) {
                serverBootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS);
            }
        }
    }

    public void testInvalidSNIServerName() throws Exception {
        SSLService sslService = createSSLService();
        final ServerBootstrap serverBootstrap = new ServerBootstrap();
        boolean success = false;
        try {
            serverBootstrap.group(new NioEventLoopGroup(1));
            serverBootstrap.channel(NioServerSocketChannel.class);

            final String sniIp = "invalid_hostname";
            serverBootstrap.childHandler(new ChannelInitializer<Channel>() {

                @Override
                protected void initChannel(Channel ch) {
                    SSLEngine serverEngine = sslService.createSSLEngine(sslService.getSSLConfiguration(setting("transport.ssl.")),
                        null, -1);
                    serverEngine.setUseClientMode(false);
                    final SslHandler sslHandler = new SslHandler(serverEngine);
                    ch.pipeline().addFirst("sslhandler", sslHandler);
                }
            });
            serverBootstrap.validate();
            ChannelFuture serverFuture = serverBootstrap.bind(getLocalEphemeral());
            serverFuture.await();
            InetSocketAddress serverAddress = (InetSocketAddress) serverFuture.channel().localAddress();

            try (MockTransportService serviceC = build(
                Settings.builder()
                    .put("name", "TS_TEST")
                    .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), "")
                    .put(TransportService.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
                    .build(),
                version0,
                null, true)) {
                serviceC.acceptIncomingRequests();

                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = new DiscoveryNode("server_node_id", new TransportAddress(serverAddress), attributes,
                    EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);

                ConnectTransportException connectException = expectThrows(ConnectTransportException.class,
                    () -> serviceC.connectToNode(node, SINGLE_CHANNEL_PROFILE));

                assertThat(connectException.getMessage(), containsString("invalid DiscoveryNode server_name [invalid_hostname]"));

                serverBootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS);
                success = true;
            }
        } finally {
            if (success == false) {
                serverBootstrap.config().group().shutdownGracefully(0, 5, TimeUnit.SECONDS);
            }
        }
    }
}
