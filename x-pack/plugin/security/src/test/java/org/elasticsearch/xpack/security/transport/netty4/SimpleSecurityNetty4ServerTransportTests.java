/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.socket.nio.NioChannelOption;
import io.netty.handler.ssl.SslHandshakeTimeoutException;

import org.apache.lucene.util.Constants;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TestProfiles;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.transport.netty4.Netty4TcpChannel;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.SSLEngineUtils;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.transport.netty4.Netty4Transport.OPTION_TCP_KEEP_COUNT;
import static org.elasticsearch.transport.netty4.Netty4Transport.OPTION_TCP_KEEP_IDLE;
import static org.elasticsearch.transport.netty4.Netty4Transport.OPTION_TCP_KEEP_INTERVAL;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class SimpleSecurityNetty4ServerTransportTests extends AbstractSimpleTransportTestCase {
    @Override
    protected Transport build(Settings settings, TransportVersion version, ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Settings settings1 = Settings.builder().put(settings).put("xpack.security.transport.ssl.enabled", true).build();
        return new TestSecurityNetty4ServerTransport(
            settings1,
            version,
            threadPool,
            networkService,
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            namedWriteableRegistry,
            new NoneCircuitBreakerService(),
            null,
            createSSLService(settings1),
            new SharedGroupFactory(settings1),
            doHandshake
        );
    }

    private SSLService createSSLService() {
        return createSSLService(Settings.EMPTY);
    }

    protected SSLService createSSLService(Settings settings) {
        Path testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
        // Some tests use a client profile. Put the passphrase in the secure settings for the profile (secure settings cannot be set twice)
        secureSettings.setString("transport.profiles.client.xpack.security.ssl.secure_key_passphrase", "testnode");
        // For test that enables remote cluster port
        secureSettings.setString("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "testnode");
        Settings settings1 = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.key", testnodeKey)
            .put("xpack.security.transport.ssl.certificate", testnodeCert)
            .put("path.home", createTempDir())
            .put(settings)
            .setSecureSettings(secureSettings)
            .build();
        try {
            return new SSLService(TestEnvironment.newEnvironment(settings1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Set<Setting<?>> getSupportedSettings() {
        HashSet<Setting<?>> availableSettings = new HashSet<>(super.getSupportedSettings());
        availableSettings.addAll(XPackSettings.getAllSettings());
        return availableSettings;
    }

    public void testConnectException() throws UnknownHostException {
        try {
            connectToNode(
                serviceA,
                DiscoveryNodeUtils.create("C", new TransportAddress(InetAddress.getByName("localhost"), 9876), emptyMap(), emptySet())
            );
            fail("Expected ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("connect_exception"));
            assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
            Throwable cause = ExceptionsHelper.unwrap(e, IOException.class);
            assertThat(cause, instanceOf(IOException.class));
        }

    }

    @Override
    public void testTcpHandshake() {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        TcpTransport originalTransport = (TcpTransport) serviceA.getOriginalTransport();

        ConnectionProfile connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        try (TransportService service = buildService("TS_TPC", Version.CURRENT, TransportVersion.current(), Settings.EMPTY)) {
            DiscoveryNode node = new DiscoveryNode(
                "TS_TPC",
                "TS_TPC",
                service.boundAddress().publishAddress(),
                emptyMap(),
                emptySet(),
                version0
            );
            PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
            originalTransport.openConnection(node, connectionProfile, future);
            try (TcpTransport.NodeChannels connection = (TcpTransport.NodeChannels) future.actionGet()) {
                assertEquals(TransportVersion.current(), connection.getTransportVersion());
            }
        }
    }

    @SuppressForbidden(reason = "Need to open socket connection")
    public void testRenegotiation() throws Exception {
        assumeFalse("BCTLS doesn't support renegotiation: https://github.com/bcgit/bc-java/issues/593#issuecomment-533518845", inFipsJvm());
        // force TLSv1.2 since renegotiation is not supported by 1.3
        SSLService sslService = createSSLService(
            Settings.builder().put("xpack.security.transport.ssl.supported_protocols", "TLSv1.2").build()
        );
        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.security.transport.ssl");
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

            CountDownLatch renegotiationLatch = new CountDownLatch(1);
            HandshakeCompletedListener secondListener = event -> renegotiationLatch.countDown();
            socket.addHandshakeCompletedListener(secondListener);
            socket.startHandshake();
            AtomicBoolean stopped = new AtomicBoolean(false);
            socket.setSoTimeout(10);
            Thread emptyReader = new Thread(() -> {
                while (stopped.get() == false) {
                    try {
                        socket.getInputStream().read();
                    } catch (SocketTimeoutException e) {
                        // Ignore. We expect a timeout.
                    } catch (IOException e) {
                        throw new AssertionError(e);
                    }
                }
            });
            emptyReader.start();
            renegotiationLatch.await();
            stopped.set(true);
            emptyReader.join();
            socket.removeHandshakeCompletedListener(secondListener);

            stream.writeByte((byte) 'E');
            stream.writeByte((byte) 'S');
            stream.writeInt(-1);
            stream.flush();
        }
    }

    public void testSNIServerNameIsPropagated() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, TrustAllConfig is not a SunJSSE TrustManagers", inFipsJvm());
        SSLService sslService = createSSLService();

        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        SSLContext sslContext = sslService.sslContext(sslConfiguration);
        final SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
        final String sniIp = "sni-hostname";
        final SNIHostName sniHostName = new SNIHostName(sniIp);
        final CountDownLatch latch = new CountDownLatch(2);

        try (SSLServerSocket sslServerSocket = (SSLServerSocket) serverSocketFactory.createServerSocket()) {
            SSLParameters sslParameters = sslServerSocket.getSSLParameters();
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
            sslServerSocket.setSSLParameters(sslParameters);

            SocketAccess.doPrivileged(() -> sslServerSocket.bind(getLocalEphemeral()));

            new Thread(() -> {
                try {
                    SSLSocket acceptedSocket = (SSLSocket) SocketAccess.doPrivileged(sslServerSocket::accept);

                    // A read call will execute the handshake
                    int byteRead = acceptedSocket.getInputStream().read();
                    assertEquals('E', byteRead);
                    latch.countDown();
                    IOUtils.closeWhileHandlingException(acceptedSocket);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }).start();

            InetSocketAddress serverAddress = (InetSocketAddress) SocketAccess.doPrivileged(sslServerSocket::getLocalSocketAddress);

            Settings settings = Settings.builder().put("xpack.security.transport.ssl.verification_mode", "none").build();
            try (MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, settings)) {
                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = DiscoveryNodeUtils.create(
                    "server_node_id",
                    new TransportAddress(serverAddress),
                    attributes,
                    DiscoveryNodeRole.roles()
                );

                new Thread(() -> {
                    try {
                        connectToNode(serviceC, node, TestProfiles.LIGHT_PROFILE);
                    } catch (ConnectTransportException ex) {
                        // Ignore. The other side is not setup to do the ES handshake. So this will fail.
                    }
                }).start();

                latch.await();
            }
        }
    }

    public void testInvalidSNIServerName() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, TrustAllConfig is not a SunJSSE TrustManagers", inFipsJvm());
        SSLService sslService = createSSLService();

        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        SSLContext sslContext = sslService.sslContext(sslConfiguration);
        final SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
        final String sniIp = "invalid_hostname";

        try (SSLServerSocket sslServerSocket = (SSLServerSocket) serverSocketFactory.createServerSocket()) {
            SocketAccess.doPrivileged(() -> sslServerSocket.bind(getLocalEphemeral()));

            new Thread(() -> {
                try {
                    SocketAccess.doPrivileged(sslServerSocket::accept);
                } catch (IOException e) {
                    // We except an IOException from the `accept` call because the server socket will be
                    // closed before the call returns.
                }
            }).start();

            InetSocketAddress serverAddress = (InetSocketAddress) SocketAccess.doPrivileged(sslServerSocket::getLocalSocketAddress);

            Settings settings = Settings.builder().put("xpack.security.transport.ssl.verification_mode", "none").build();
            try (MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, settings)) {
                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = DiscoveryNodeUtils.create(
                    "server_node_id",
                    new TransportAddress(serverAddress),
                    attributes,
                    DiscoveryNodeRole.roles()
                );

                ConnectTransportException connectException = expectThrows(
                    ConnectTransportException.class,
                    () -> connectToNode(serviceC, node, TestProfiles.LIGHT_PROFILE)
                );

                assertThat(connectException.getMessage(), containsString("invalid DiscoveryNode server_name [invalid_hostname]"));
            }
        }
    }

    public void testSecurityClientAuthenticationConfigs() throws Exception {
        Path testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");

        Transport.Connection connection1 = serviceA.getConnection(serviceB.getLocalNode());
        SSLEngine sslEngine = getSSLEngine(connection1);
        assertThat(sslEngine, notNullValue());
        // test client authentication is default
        assertThat(sslEngine.getNeedClientAuth(), is(true));
        assertThat(sslEngine.getWantClientAuth(), is(false));

        // test required client authentication
        String value = randomCapitalization(SslClientAuthenticationMode.REQUIRED);
        Settings settings = Settings.builder().put("xpack.security.transport.ssl.client_authentication", value).build();
        try (MockTransportService service = buildService("TS_REQUIRED_CLIENT_AUTH", Version.CURRENT, TransportVersion.current(), settings)) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            try (Transport.Connection connection2 = openConnection(serviceA, service.getLocalNode(), TestProfiles.LIGHT_PROFILE)) {
                sslEngine = getEngineFromAcceptedChannel(originalTransport, connection2);
                assertThat(sslEngine.getNeedClientAuth(), is(true));
                assertThat(sslEngine.getWantClientAuth(), is(false));
            }
        }

        // test no client authentication
        value = randomCapitalization(SslClientAuthenticationMode.NONE);
        settings = Settings.builder().put("xpack.security.transport.ssl.client_authentication", value).build();
        try (MockTransportService service = buildService("TS_NO_CLIENT_AUTH", Version.CURRENT, TransportVersion.current(), settings)) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            try (Transport.Connection connection2 = openConnection(serviceA, service.getLocalNode(), TestProfiles.LIGHT_PROFILE)) {
                sslEngine = getEngineFromAcceptedChannel(originalTransport, connection2);
                assertThat(sslEngine.getNeedClientAuth(), is(false));
                assertThat(sslEngine.getWantClientAuth(), is(false));
            }
        }

        // test optional client authentication
        value = randomCapitalization(SslClientAuthenticationMode.OPTIONAL);
        settings = Settings.builder().put("xpack.security.transport.ssl.client_authentication", value).build();
        try (MockTransportService service = buildService("TS_OPTIONAL_CLIENT_AUTH", Version.CURRENT, TransportVersion.current(), settings)) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            try (Transport.Connection connection2 = openConnection(serviceA, service.getLocalNode(), TestProfiles.LIGHT_PROFILE)) {
                sslEngine = getEngineFromAcceptedChannel(originalTransport, connection2);
                assertThat(sslEngine.getNeedClientAuth(), is(false));
                assertThat(sslEngine.getWantClientAuth(), is(true));
            }
        }

        // test profile required client authentication
        value = randomCapitalization(SslClientAuthenticationMode.REQUIRED);
        settings = Settings.builder()
            .put("transport.profiles.client.port", "8000-9000")
            .put("transport.profiles.client.xpack.security.ssl.enabled", true)
            .put("transport.profiles.client.xpack.security.ssl.certificate", testnodeCert)
            .put("transport.profiles.client.xpack.security.ssl.key", testnodeKey)
            .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
            .build();
        try (
            MockTransportService service = buildService(
                "TS_PROFILE_REQUIRE_CLIENT_AUTH",
                Version.CURRENT,
                    TransportVersion.current(),
                settings
            )
        ) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            TransportAddress clientAddress = originalTransport.profileBoundAddresses().get("client").publishAddress();
            DiscoveryNode node = DiscoveryNodeUtils.create(
                service.getLocalNode().getId(),
                clientAddress,
                service.getLocalNode().getVersion()
            );
            try (Transport.Connection connection2 = openConnection(serviceA, node, TestProfiles.LIGHT_PROFILE)) {
                sslEngine = getEngineFromAcceptedChannel(originalTransport, connection2);
                assertEquals("client", getAcceptedChannel(originalTransport, connection2).getProfile());
                assertThat(sslEngine.getNeedClientAuth(), is(true));
                assertThat(sslEngine.getWantClientAuth(), is(false));
            }
        }

        // test profile no client authentication
        value = randomCapitalization(SslClientAuthenticationMode.NONE);
        settings = Settings.builder()
            .put("transport.profiles.client.port", "8000-9000")
            .put("transport.profiles.client.xpack.security.ssl.enabled", true)
            .put("transport.profiles.client.xpack.security.ssl.certificate", testnodeCert)
            .put("transport.profiles.client.xpack.security.ssl.key", testnodeKey)
            .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
            .build();
        try (
            MockTransportService service = buildService("TS_PROFILE_NO_CLIENT_AUTH", Version.CURRENT, TransportVersion.current(), settings)
        ) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            TransportAddress clientAddress = originalTransport.profileBoundAddresses().get("client").publishAddress();
            DiscoveryNode node = DiscoveryNodeUtils.create(
                service.getLocalNode().getId(),
                clientAddress,
                service.getLocalNode().getVersion()
            );
            try (Transport.Connection connection2 = openConnection(serviceA, node, TestProfiles.LIGHT_PROFILE)) {
                sslEngine = getEngineFromAcceptedChannel(originalTransport, connection2);
                assertEquals("client", getAcceptedChannel(originalTransport, connection2).getProfile());
                assertThat(sslEngine.getNeedClientAuth(), is(false));
                assertThat(sslEngine.getWantClientAuth(), is(false));
            }
        }

        // test profile optional client authentication
        value = randomCapitalization(SslClientAuthenticationMode.OPTIONAL);
        settings = Settings.builder()
            .put("transport.profiles.client.port", "8000-9000")
            .put("transport.profiles.client.xpack.security.ssl.enabled", true)
            .put("transport.profiles.client.xpack.security.ssl.certificate", testnodeCert)
            .put("transport.profiles.client.xpack.security.ssl.key", testnodeKey)
            .put("transport.profiles.client.xpack.security.ssl.client_authentication", value)
            .build();
        try (
            MockTransportService service = buildService(
                "TS_PROFILE_OPTIONAL_CLIENT_AUTH",
                Version.CURRENT,
                    TransportVersion.current(),
                settings
            )
        ) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            TransportAddress clientAddress = originalTransport.profileBoundAddresses().get("client").publishAddress();
            DiscoveryNode node = DiscoveryNodeUtils.create(
                service.getLocalNode().getId(),
                clientAddress,
                service.getLocalNode().getVersion()
            );
            try (Transport.Connection connection2 = openConnection(serviceA, node, TestProfiles.LIGHT_PROFILE)) {
                sslEngine = getEngineFromAcceptedChannel(originalTransport, connection2);
                assertEquals("client", getAcceptedChannel(originalTransport, connection2).getProfile());
                assertThat(sslEngine.getNeedClientAuth(), is(false));
                assertThat(sslEngine.getWantClientAuth(), is(true));
            }
        }
    }

    public void testClientChannelUsesSeparateSslConfigurationForRemoteCluster() throws Exception {
        final Path testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.crt");
        final Path testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode_updated.pem");

        final ConnectionProfile connectionProfile = ConnectionProfile.resolveConnectionProfile(
            new ConnectionProfile.Builder().setTransportProfile("_remote_cluster")
                .addConnections(
                    1,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.PING,
                    TransportRequestOptions.Type.RECOVERY,
                    TransportRequestOptions.Type.REG,
                    TransportRequestOptions.Type.STATE
                )
                .build(),
            TestProfiles.LIGHT_PROFILE
        );

        final Settings fcSettings = Settings.builder()
            .put("remote_cluster_server.enabled", "true")
            .put("remote_cluster.port", "0")
            .put("xpack.security.remote_cluster_server.ssl.key", testnodeKey)
            .put("xpack.security.remote_cluster_server.ssl.certificate", testnodeCert)
            .put("xpack.security.remote_cluster_server.ssl.client_authentication", "none")
            .build();

        try (MockTransportService fcService = buildService("FC", Version.CURRENT, TransportVersion.current(), fcSettings)) {
            final TcpTransport originalTransport = (TcpTransport) fcService.getOriginalTransport();
            final TransportAddress remoteAccessAddress = originalTransport.profileBoundAddresses().get("_remote_cluster").publishAddress();
            final DiscoveryNode node = DiscoveryNodeUtils.create(
                fcService.getLocalNode().getId(),
                remoteAccessAddress,
                fcService.getLocalNode().getVersion()
            );

            // 1. Connection will fail because FC server certificate is not trusted by default
            final Settings qcSettings1 = Settings.builder().build();
            try (MockTransportService qcService = buildService("QC", Version.CURRENT, TransportVersion.current(), qcSettings1)) {
                final ConnectTransportException e = expectThrows(
                    ConnectTransportException.class,
                    () -> openConnection(qcService, node, connectionProfile)
                );
                assertThat(
                    e.getRootCause().getMessage(),
                    anyOf(containsString("unable to find valid certification path"), containsString("Unable to find certificate chain"))
                );
            }

            // 2. Connection will success because QC does not verify FC server certificate
            final Settings qcSettings2 = Settings.builder()
                .put("xpack.security.remote_cluster_client.ssl.verification_mode", "none")
                .put("remote_cluster.tcp.keep_alive", "false")
                .build();
            try (
                    MockTransportService qcService = buildService("QC", Version.CURRENT, TransportVersion.current(), qcSettings2);
                    Transport.Connection connection = openConnection(qcService, node, connectionProfile)
            ) {
                assertThat(connection, instanceOf(StubbableTransport.WrappedConnection.class));
                Transport.Connection conn = ((StubbableTransport.WrappedConnection) connection).getConnection();
                assertThat(conn, instanceOf(TcpTransport.NodeChannels.class));
                TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) conn;
                for (TcpChannel channel : nodeChannels.getChannels()) {
                    assertFalse(channel.isServerChannel());
                    assertThat(channel.getProfile(), equalTo("_remote_cluster"));
                    final SSLEngine sslEngine = SSLEngineUtils.getSSLEngine(channel);
                    assertThat(sslEngine.getUseClientMode(), is(true));
                    assertThat(channel, instanceOf(Netty4TcpChannel.class));
                    final Map<String, Object> options = ((Netty4TcpChannel) channel).getNettyChannel()
                        .config()
                        .getOptions()
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getKey() instanceof NioChannelOption<?>)
                        .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey().name(), Map.Entry::getValue));
                    assertThat(options.get(ChannelOption.SO_KEEPALIVE.name()), is(false));
                }

                final TcpChannel acceptedChannel = getAcceptedChannel(originalTransport, connection);
                assertThat(acceptedChannel.getProfile(), equalTo("_remote_cluster"));
            }

            // 3. Connection will success because QC is explicitly configured to trust FC server certificate
            final Settings qcSettings3 = Settings.builder()
                .put("xpack.security.remote_cluster_client.ssl.certificate_authorities", testnodeCert)
                .put("xpack.security.remote_cluster_client.ssl.verification_mode", "full")
                .put("remote_cluster.tcp.keep_idle", 100)
                .put("remote_cluster.tcp.keep_interval", 101)
                .put("remote_cluster.tcp.keep_count", 102)
                .build();
            try (
                    MockTransportService qcService = buildService("QC", Version.CURRENT, TransportVersion.current(), qcSettings3);
                    Transport.Connection connection = openConnection(qcService, node, connectionProfile)
            ) {
                assertThat(connection, instanceOf(StubbableTransport.WrappedConnection.class));
                Transport.Connection conn = ((StubbableTransport.WrappedConnection) connection).getConnection();
                assertThat(conn, instanceOf(TcpTransport.NodeChannels.class));
                TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) conn;
                for (TcpChannel channel : nodeChannels.getChannels()) {
                    assertFalse(channel.isServerChannel());
                    assertThat(channel.getProfile(), equalTo("_remote_cluster"));
                    final SSLEngine sslEngine = SSLEngineUtils.getSSLEngine(channel);
                    assertThat(sslEngine.getUseClientMode(), is(true));
                    assertThat(channel, instanceOf(Netty4TcpChannel.class));
                    final Map<String, Object> options = ((Netty4TcpChannel) channel).getNettyChannel()
                        .config()
                        .getOptions()
                        .entrySet()
                        .stream()
                        .filter(entry -> entry.getKey() instanceof NioChannelOption<?>)
                        .collect(Collectors.toUnmodifiableMap(entry -> entry.getKey().name(), Map.Entry::getValue));
                    assertThat(options.get(ChannelOption.SO_KEEPALIVE.name()), is(true));
                    if (false == Constants.WINDOWS) {
                        assertThat(options.get(OPTION_TCP_KEEP_IDLE.name()), equalTo(100));
                        assertThat(options.get(OPTION_TCP_KEEP_INTERVAL.name()), equalTo(101));
                        assertThat(options.get(OPTION_TCP_KEEP_COUNT.name()), equalTo(102));
                    }
                }

                final TcpChannel acceptedChannel = getAcceptedChannel(originalTransport, connection);
                assertThat(acceptedChannel.getProfile(), equalTo("_remote_cluster"));
            }
        }
    }

    public void testRemoteClusterCanWorkWithoutSSL() throws Exception {
        final ConnectionProfile connectionProfile = ConnectionProfile.resolveConnectionProfile(
            new ConnectionProfile.Builder().setTransportProfile("_remote_cluster")
                .addConnections(
                    1,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.PING,
                    TransportRequestOptions.Type.RECOVERY,
                    TransportRequestOptions.Type.REG,
                    TransportRequestOptions.Type.STATE
                )
                .build(),
            TestProfiles.LIGHT_PROFILE
        );

        final Settings fcSettings = Settings.builder()
            .put("remote_cluster_server.enabled", "true")
            .put("remote_cluster.port", "0")
            .put("xpack.security.remote_cluster_server.ssl.enabled", "false")
            .build();

        try (MockTransportService fcService = buildService("FC", Version.CURRENT, TransportVersion.current(), fcSettings)) {
            final TcpTransport originalTransport = (TcpTransport) fcService.getOriginalTransport();
            final TransportAddress remoteAccessAddress = originalTransport.profileBoundAddresses().get("_remote_cluster").publishAddress();
            final DiscoveryNode node = DiscoveryNodeUtils.create(
                fcService.getLocalNode().getId(),
                remoteAccessAddress,
                fcService.getLocalNode().getVersion()
            );
            final Settings qcSettings = Settings.builder().put("xpack.security.remote_cluster_client.ssl.enabled", "false").build();
            try (
                    MockTransportService qcService = buildService("QC", Version.CURRENT, TransportVersion.current(), qcSettings);
                    Transport.Connection connection = openConnection(qcService, node, connectionProfile)
            ) {
                assertThat(connection, instanceOf(StubbableTransport.WrappedConnection.class));
                Transport.Connection conn = ((StubbableTransport.WrappedConnection) connection).getConnection();
                assertThat(conn, instanceOf(TcpTransport.NodeChannels.class));
                TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) conn;
                for (TcpChannel channel : nodeChannels.getChannels()) {
                    assertFalse(channel.isServerChannel());
                    assertThat(channel.getProfile(), equalTo("_remote_cluster"));
                }

                final TcpChannel acceptedChannel = getAcceptedChannel(originalTransport, connection);
                assertThat(acceptedChannel.getProfile(), equalTo("_remote_cluster"));
            }
        }
    }

    public void testGetClientBootstrap() {
        final ConnectionProfile connectionProfile = ConnectionProfile.resolveConnectionProfile(
            new ConnectionProfile.Builder().setTransportProfile("_remote_cluster")
                .addConnections(
                    1,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.BULK,
                    TransportRequestOptions.Type.PING,
                    TransportRequestOptions.Type.RECOVERY,
                    TransportRequestOptions.Type.REG,
                    TransportRequestOptions.Type.STATE
                )
                .build(),
            TestProfiles.LIGHT_PROFILE
        );

        // 1. Configuration for default profile only
        final Settings.Builder builder1 = Settings.builder();
        if (randomBoolean()) {
            builder1.put(TransportSettings.TCP_NO_DELAY.getKey(), randomBoolean())
                .put(TransportSettings.TCP_KEEP_ALIVE.getKey(), randomBoolean())
                .put(TransportSettings.TCP_KEEP_IDLE.getKey(), randomIntBetween(-1, 300))
                .put(TransportSettings.TCP_KEEP_INTERVAL.getKey(), randomIntBetween(-1, 300))
                .put(TransportSettings.TCP_KEEP_COUNT.getKey(), randomIntBetween(-1, 300))
                .put(TransportSettings.TCP_SEND_BUFFER_SIZE.getKey(), ByteSizeValue.ofBytes(randomIntBetween(-1, 1000)))
                .put(TransportSettings.TCP_RECEIVE_BUFFER_SIZE.getKey(), ByteSizeValue.ofBytes(randomIntBetween(-1, 1000)))
                .put(TransportSettings.TCP_REUSE_ADDRESS.getKey(), randomBoolean());
        }
        final Settings qcSettings1 = builder1.build();
        try (MockTransportService qcService = buildService("QC", Version.CURRENT, TransportVersion.current(), qcSettings1)) {
            final var transport = (TestSecurityNetty4ServerTransport) qcService.getOriginalTransport();
            // RCS remote cluster client
            final Bootstrap rcsBootstrap = transport.getClientBootstrap(connectionProfile);
            // Legacy remote cluster client
            final Bootstrap legacyBootstrap = transport.getClientBootstrap(TestProfiles.LIGHT_PROFILE);
            // identical
            assertThat(rcsBootstrap.config().options(), equalTo(legacyBootstrap.config().options()));

            // The following attempts to ensure the super class's createClientBootstrap method does not change.
            // It does that by checking all configured options are known and expected, i.e. no option is added or removed.
            // The check is to approximately ensure SecurityNetty4Transport#getClientBootstrap does not become stale without notice
            final HashSet<ChannelOption<?>> expectedChannelOptions = new HashSet<>(
                Set.of(
                    ChannelOption.ALLOCATOR,
                    ChannelOption.TCP_NODELAY,
                    ChannelOption.SO_KEEPALIVE,
                    ChannelOption.RCVBUF_ALLOCATOR,
                    ChannelOption.SO_REUSEADDR
                )
            );
            if (TransportSettings.TCP_KEEP_ALIVE.get(qcSettings1)) {
                if (TransportSettings.TCP_KEEP_IDLE.get(qcSettings1) >= 0) {
                    expectedChannelOptions.add(OPTION_TCP_KEEP_IDLE);
                }
                if (TransportSettings.TCP_KEEP_INTERVAL.get(qcSettings1) >= 0) {
                    expectedChannelOptions.add(OPTION_TCP_KEEP_INTERVAL);
                }
                if (TransportSettings.TCP_KEEP_COUNT.get(qcSettings1) >= 0) {
                    expectedChannelOptions.add(OPTION_TCP_KEEP_COUNT);
                }
            }
            if (TransportSettings.TCP_SEND_BUFFER_SIZE.get(qcSettings1).getBytes() > 0) {
                expectedChannelOptions.add(ChannelOption.SO_SNDBUF);
            }
            if (TransportSettings.TCP_RECEIVE_BUFFER_SIZE.get(qcSettings1).getBytes() > 0) {
                expectedChannelOptions.add(ChannelOption.SO_RCVBUF);
            }
            // legacyBootstrap is the same as default clientBootstrap from the super class's createClientBootstrap method
            assertThat(legacyBootstrap.config().options().keySet(), equalTo(expectedChannelOptions));
        }

        // 2. Different settings for _remote_cluster
        final Settings.Builder builder2 = Settings.builder();
        if (randomBoolean()) {
            builder2.put(TransportSettings.TCP_NO_DELAY.getKey(), true).put(TransportSettings.TCP_KEEP_ALIVE.getKey(), true);
        }
        final Settings qcSettings2 = builder2.put(TransportSettings.TCP_KEEP_IDLE.getKey(), 200)
            .put(TransportSettings.TCP_KEEP_INTERVAL.getKey(), 201)
            .put(TransportSettings.TCP_KEEP_COUNT.getKey(), 202)
            .put(TransportSettings.TCP_SEND_BUFFER_SIZE.getKey(), ByteSizeValue.ofBytes(1))
            .put(TransportSettings.TCP_RECEIVE_BUFFER_SIZE.getKey(), ByteSizeValue.ofBytes(1))
            .put(TransportSettings.TCP_REUSE_ADDRESS.getKey(), true)
            .put(RemoteClusterPortSettings.TCP_NO_DELAY.getKey(), false)
            .put(RemoteClusterPortSettings.TCP_KEEP_ALIVE.getKey(), false)
            .put(RemoteClusterPortSettings.TCP_SEND_BUFFER_SIZE.getKey(), ByteSizeValue.ofBytes(42))
            .put(RemoteClusterPortSettings.TCP_RECEIVE_BUFFER_SIZE.getKey(), ByteSizeValue.ofBytes(99))
            .put(RemoteClusterPortSettings.TCP_REUSE_ADDRESS.getKey(), false)
            .build();

        try (MockTransportService qcService = buildService("QC", Version.CURRENT, TransportVersion.current(), qcSettings2)) {
            final var transport = (TestSecurityNetty4ServerTransport) qcService.getOriginalTransport();
            // RCS remote cluster client
            final Map<ChannelOption<?>, Object> rcsOptions = transport.getClientBootstrap(connectionProfile).config().options();
            assertThat(rcsOptions.get(ChannelOption.TCP_NODELAY), is(false));
            assertThat(rcsOptions.get(ChannelOption.SO_KEEPALIVE), is(false));
            assertThat(rcsOptions.get(OPTION_TCP_KEEP_IDLE), nullValue());
            assertThat(rcsOptions.get(OPTION_TCP_KEEP_INTERVAL), nullValue());
            assertThat(rcsOptions.get(OPTION_TCP_KEEP_COUNT), nullValue());
            assertThat(rcsOptions.get(ChannelOption.SO_SNDBUF), equalTo(42));
            assertThat(rcsOptions.get(ChannelOption.SO_RCVBUF), equalTo(99));
            assertThat(rcsOptions.get(ChannelOption.SO_REUSEADDR), is(false));

            // Legacy remote cluster client
            final Map<ChannelOption<?>, Object> legacyOptions = transport.getClientBootstrap(TestProfiles.LIGHT_PROFILE).config().options();
            assertThat(legacyOptions.get(ChannelOption.TCP_NODELAY), is(true));
            assertThat(legacyOptions.get(ChannelOption.SO_KEEPALIVE), is(true));
            assertThat(legacyOptions.get(OPTION_TCP_KEEP_IDLE), equalTo(200));
            assertThat(legacyOptions.get(OPTION_TCP_KEEP_INTERVAL), equalTo(201));
            assertThat(legacyOptions.get(OPTION_TCP_KEEP_COUNT), equalTo(202));
            assertThat(legacyOptions.get(ChannelOption.SO_SNDBUF), equalTo(1));
            assertThat(legacyOptions.get(ChannelOption.SO_RCVBUF), equalTo(1));
            assertThat(legacyOptions.get(ChannelOption.SO_REUSEADDR), is(true));
        }

        // 3. Different keep_idle, keep_interval, keep_count
        final Settings.Builder builder3 = Settings.builder();
        if (randomBoolean()) {
            builder3.put(TransportSettings.TCP_KEEP_ALIVE.getKey(), true).put(RemoteClusterPortSettings.TCP_KEEP_ALIVE.getKey(), true);
        }
        final Settings qcSettings3 = builder3.put(TransportSettings.TCP_KEEP_IDLE.getKey(), 200)
            .put(TransportSettings.TCP_KEEP_INTERVAL.getKey(), 201)
            .put(TransportSettings.TCP_KEEP_COUNT.getKey(), 202)
            .put(RemoteClusterPortSettings.TCP_KEEP_IDLE.getKey(), 100)
            .put(RemoteClusterPortSettings.TCP_KEEP_INTERVAL.getKey(), 101)
            .put(RemoteClusterPortSettings.TCP_KEEP_COUNT.getKey(), 102)
            .build();
        try (MockTransportService qcService = buildService("QC", Version.CURRENT, TransportVersion.current(), qcSettings3)) {
            final var transport = (TestSecurityNetty4ServerTransport) qcService.getOriginalTransport();
            // RCS remote cluster client
            final Map<ChannelOption<?>, Object> rcsOptions = transport.getClientBootstrap(connectionProfile).config().options();
            assertThat(rcsOptions.get(ChannelOption.SO_KEEPALIVE), is(true));
            assertThat(rcsOptions.get(OPTION_TCP_KEEP_IDLE), equalTo(100));
            assertThat(rcsOptions.get(OPTION_TCP_KEEP_INTERVAL), equalTo(101));
            assertThat(rcsOptions.get(OPTION_TCP_KEEP_COUNT), equalTo(102));

            // Legacy remote cluster client
            final Map<ChannelOption<?>, Object> legacyOptions = transport.getClientBootstrap(TestProfiles.LIGHT_PROFILE).config().options();
            assertThat(legacyOptions.get(ChannelOption.SO_KEEPALIVE), is(true));
            assertThat(legacyOptions.get(OPTION_TCP_KEEP_IDLE), equalTo(200));
            assertThat(legacyOptions.get(OPTION_TCP_KEEP_INTERVAL), equalTo(201));
            assertThat(legacyOptions.get(OPTION_TCP_KEEP_COUNT), equalTo(202));
        }
    }

    public void testTcpHandshakeTimeout() throws IOException {
        assumeFalse("Can't run in a FIPS JVM, TrustAllConfig is not a SunJSSE TrustManagers", inFipsJvm());
        SSLService sslService = createSSLService();

        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        SSLContext sslContext = sslService.sslContext(sslConfiguration);
        final SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
        // use latch to to ensure that the accepted socket below isn't closed before the handshake times out
        final CountDownLatch doneLatch = new CountDownLatch(1);
        try (ServerSocket socket = serverSocketFactory.createServerSocket()) {
            socket.bind(getLocalEphemeral(), 1);
            socket.setReuseAddress(true);
            new Thread(() -> {
                SSLSocket acceptedSocket = null;
                try {
                    acceptedSocket = (SSLSocket) SocketAccess.doPrivileged(socket::accept);
                    // A read call will execute the ssl handshake
                    int byteRead = acceptedSocket.getInputStream().read();
                    assertEquals('E', byteRead);
                    doneLatch.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                } finally {
                    IOUtils.closeWhileHandlingException(acceptedSocket);
                }
            }).start();
            DiscoveryNode dummy = DiscoveryNodeUtils.create(
                "TEST",
                new TransportAddress(socket.getInetAddress(), socket.getLocalPort()),
                emptyMap(),
                emptySet(),
                version0
            );
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            builder.setHandshakeTimeout(TimeValue.timeValueMillis(1));
            Settings settings = Settings.builder().put("xpack.security.transport.ssl.verification_mode", "none").build();
            try (MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, settings)) {
                ConnectTransportException ex = expectThrows(
                    ConnectTransportException.class,
                    () -> connectToNode(serviceC, dummy, builder.build())
                );
                assertEquals("[][" + dummy.getAddress() + "] handshake_timeout[1ms]", ex.getMessage());
            }
        } finally {
            doneLatch.countDown();
        }
    }

    public void testTlsHandshakeTimeout() throws IOException {
        final CountDownLatch doneLatch = new CountDownLatch(1);
        try (ServerSocket socket = new MockServerSocket()) {
            socket.bind(getLocalEphemeral(), 1);
            socket.setReuseAddress(true);
            new Thread(() -> {
                try (Socket ignored = socket.accept()) {
                    doneLatch.await();
                } catch (Exception e) {
                    throw new AssertionError(e);
                }
            }).start();
            DiscoveryNode dummy = DiscoveryNodeUtils.create(
                "TEST",
                new TransportAddress(socket.getInetAddress(), socket.getLocalPort()),
                emptyMap(),
                emptySet(),
                version0
            );
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            ConnectTransportException ex = expectThrows(
                ConnectTransportException.class,
                () -> connectToNode(serviceA, dummy, builder.build())
            );
            assertEquals("[][" + dummy.getAddress() + "] connect_exception", ex.getMessage());
            assertNotNull(ExceptionsHelper.unwrap(ex, SslHandshakeTimeoutException.class));
        } finally {
            doneLatch.countDown();
        }
    }

    public void testTcpHandshakeConnectionReset() throws IOException, InterruptedException {
        assumeFalse("Can't run in a FIPS JVM, TrustAllConfig is not a SunJSSE TrustManagers", inFipsJvm());
        SSLService sslService = createSSLService();

        final SslConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        SSLContext sslContext = sslService.sslContext(sslConfiguration);
        final SSLServerSocketFactory serverSocketFactory = sslContext.getServerSocketFactory();
        try (ServerSocket socket = serverSocketFactory.createServerSocket()) {
            socket.bind(getLocalEphemeral(), 1);
            socket.setReuseAddress(true);
            DiscoveryNode dummy = DiscoveryNodeUtils.create(
                "TEST",
                new TransportAddress(socket.getInetAddress(), socket.getLocalPort()),
                emptyMap(),
                emptySet(),
                version0
            );
            Thread t = new Thread(() -> {
                try (Socket accept = SocketAccess.doPrivileged(socket::accept)) {
                    // A read call will execute the ssl handshake
                    int byteRead = accept.getInputStream().read();
                    assertEquals('E', byteRead);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            t.start();
            ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
            builder.addConnections(
                1,
                TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.PING,
                TransportRequestOptions.Type.RECOVERY,
                TransportRequestOptions.Type.REG,
                TransportRequestOptions.Type.STATE
            );
            builder.setHandshakeTimeout(TimeValue.timeValueHours(1));
            Settings settings = Settings.builder().put("xpack.security.transport.ssl.verification_mode", "none").build();
            try (MockTransportService serviceC = buildService("TS_C", version0, transportVersion0, settings)) {
                ConnectTransportException ex = expectThrows(
                    ConnectTransportException.class,
                    () -> connectToNode(serviceC, dummy, builder.build())
                );
                assertEquals("[][" + dummy.getAddress() + "] general node connection failure", ex.getMessage());
                assertThat(ex.getCause().getMessage(), startsWith("handshake failed"));
            }
            t.join();
        }
    }

    public static String randomCapitalization(SslClientAuthenticationMode mode) {
        return randomFrom(mode.name(), mode.name().toLowerCase(Locale.ROOT));
    }

    private SSLEngine getEngineFromAcceptedChannel(TcpTransport transport, Transport.Connection connection) throws Exception {
        return SSLEngineUtils.getSSLEngine(getAcceptedChannel(transport, connection));
    }

    private TcpChannel getAcceptedChannel(TcpTransport transport, Transport.Connection connection) throws Exception {
        InetSocketAddress localAddress = getSingleChannel(connection).getLocalAddress();
        AtomicReference<TcpChannel> accepted = new AtomicReference<>();
        assertBusy(() -> {
            Optional<TcpChannel> maybeAccepted = getAcceptedChannels(transport).stream()
                .filter(c -> c.getRemoteAddress().equals(localAddress))
                .findFirst();
            assertTrue(maybeAccepted.isPresent());
            accepted.set(maybeAccepted.get());
        });
        return accepted.get();
    }

    private SSLEngine getSSLEngine(Transport.Connection connection) {
        return SSLEngineUtils.getSSLEngine(getSingleChannel(connection));
    }

    private TcpChannel getSingleChannel(Transport.Connection connection) {
        StubbableTransport.WrappedConnection wrappedConnection = (StubbableTransport.WrappedConnection) connection;
        TcpTransport.NodeChannels nodeChannels = (TcpTransport.NodeChannels) wrappedConnection.getConnection();
        return nodeChannels.getChannels().get(0);
    }

    static class TestSecurityNetty4ServerTransport extends SecurityNetty4ServerTransport {
        private final boolean doHandshake;

        TestSecurityNetty4ServerTransport(
            Settings settings,
            TransportVersion version,
            ThreadPool threadPool,
            NetworkService networkService,
            PageCacheRecycler pageCacheRecycler,
            NamedWriteableRegistry namedWriteableRegistry,
            CircuitBreakerService circuitBreakerService,
            IPFilter authenticator,
            SSLService sslService,
            SharedGroupFactory sharedGroupFactory,
            boolean doHandshake
        ) {
            super(
                settings,
                version,
                threadPool,
                networkService,
                pageCacheRecycler,
                namedWriteableRegistry,
                circuitBreakerService,
                authenticator,
                sslService,
                sharedGroupFactory
            );
            this.doHandshake = doHandshake;
        }

        @Override
        public void executeHandshake(
            DiscoveryNode node,
            TcpChannel channel,
            ConnectionProfile profile,
            ActionListener<TransportVersion> listener
        ) {
            if (doHandshake) {
                super.executeHandshake(node, channel, profile, listener);
            } else {
                assert getVersion().equals(TransportVersion.current());
                listener.onResponse(TransportVersion.MINIMUM_COMPATIBLE);
            }
        }

        @Override
        public Bootstrap getClientBootstrap(ConnectionProfile connectionProfile) {
            return super.getClientBootstrap(connectionProfile);
        }
    }
}
