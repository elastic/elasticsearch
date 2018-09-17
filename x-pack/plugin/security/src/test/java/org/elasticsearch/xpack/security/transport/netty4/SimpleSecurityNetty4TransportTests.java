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
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SimpleSecurityNetty4TransportTests extends AbstractSimpleTransportTestCase {

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

    private SSLService createSSLService() {
        Path testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.ssl.key", testnodeKey)
            .put("xpack.ssl.certificate", testnodeCert)
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();
        try {
            return new SSLService(settings, TestEnvironment.newEnvironment(settings));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public MockTransportService nettyFromThreadPool(Settings settings, ThreadPool threadPool, final Version version,
                                                    ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Settings settings1 = Settings.builder()
            .put(settings)
            .put("xpack.security.transport.ssl.enabled", true).build();
        Transport transport = new SecurityNetty4Transport(settings1, threadPool,
            networkService, BigArrays.NON_RECYCLING_INSTANCE, namedWriteableRegistry,
            new NoneCircuitBreakerService(), createSSLService()) {

            @Override
            protected Version executeHandshake(DiscoveryNode node, TcpChannel channel, TimeValue timeout) throws IOException,
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
        settings = Settings.builder().put(settings)
            .put(TcpTransport.PORT.getKey(), "0")
            .build();
        MockTransportService transportService = nettyFromThreadPool(settings, threadPool, version, clusterSettings, doHandshake);
        transportService.start();
        return transportService;
    }

    public void testConnectException() throws UnknownHostException {
        try {
            serviceA.connectToNode(new DiscoveryNode("C", new TransportAddress(InetAddress.getByName("localhost"), 9876),
                emptyMap(), emptySet(), Version.CURRENT));
            fail("Expected ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("connect_exception"));
            assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(IOException.class));
        }
    }

    public void testBindUnavailableAddress() {
        // this is on a lower level since it needs access to the TransportService before it's started
        int port = serviceA.boundAddress().publishAddress().getPort();
        Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), "foobar")
            .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), "")
            .put(TransportService.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
            .put("transport.tcp.port", port)
            .build();
        ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        BindTransportException bindTransportException = expectThrows(BindTransportException.class, () -> {
            MockTransportService transportService = nettyFromThreadPool(settings, threadPool, Version.CURRENT, clusterSettings, true);
            try {
                transportService.start();
            } finally {
                transportService.stop();
                transportService.close();
            }
        });
        assertEquals("Failed to bind to [" + port + "]", bindTransportException.getMessage());
    }

    @SuppressForbidden(reason = "Need to open socket connection")
    public void testRenegotiation() throws Exception {
        SSLService sslService = createSSLService();
        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.ssl");
        SocketFactory factory = sslService.sslSocketFactory(sslConfiguration);
        try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
            SocketAccess.doPrivileged(() -> socket.connect(serviceA.boundAddress().publishAddress().address()));

            CountDownLatch handshakeLatch = new CountDownLatch(1);
            HandshakeCompletedListener firstListener = event -> handshakeLatch.countDown();
            socket.addHandshakeCompletedListener(firstListener);
            socket.startHandshake();
            handshakeLatch.await();
            socket.removeHandshakeCompletedListener(firstListener);

            OutputStreamStreamOutput stream = new OutputStreamStreamOutput(socket.getOutputStream());
            stream.writeByte((byte) 'E');
            stream.writeByte((byte) 'S');
            stream.writeInt(-1);
            stream.flush();

            socket.startHandshake();
            CountDownLatch renegotiationLatch = new CountDownLatch(1);
            HandshakeCompletedListener secondListener = event -> renegotiationLatch.countDown();
            socket.addHandshakeCompletedListener(secondListener);

            AtomicReference<Exception> error = new AtomicReference<>();
            CountDownLatch catchReadErrorsLatch = new CountDownLatch(1);
            Thread renegotiationThread = new Thread(() -> {
                try {
                    socket.setSoTimeout(50);
                    socket.getInputStream().read();
                } catch (SocketTimeoutException e) {
                    // Ignore. We expect a timeout.
                } catch (IOException e) {
                    error.set(e);
                } finally {
                    catchReadErrorsLatch.countDown();
                }
            });
            renegotiationThread.start();
            renegotiationLatch.await();
            socket.removeHandshakeCompletedListener(secondListener);
            catchReadErrorsLatch.await();

            assertNull(error.get());

            stream.writeByte((byte) 'E');
            stream.writeByte((byte) 'S');
            stream.writeInt(-1);
            stream.flush();
        }
    }

    // TODO: These tests currently rely on plaintext transports

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33285")
    public void testTcpHandshake() {
    }

    // TODO: These tests as configured do not currently work with the security transport

    @Override
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33285")
    public void testTransportProfilesWithPortAndHost() {
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
