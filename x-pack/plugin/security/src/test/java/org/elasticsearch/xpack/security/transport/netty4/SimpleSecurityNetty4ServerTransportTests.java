/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import io.netty.handler.ssl.SslHandshakeTimeoutException;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
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
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockServerSocket;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TestProfiles;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.SSLEngineUtils;

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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

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
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class SimpleSecurityNetty4ServerTransportTests extends AbstractSimpleTransportTestCase {
    @Override
    protected Transport build(Settings settings, TransportVersion version, ClusterSettings clusterSettings, boolean doHandshake) {
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(Collections.emptyList());
        NetworkService networkService = new NetworkService(Collections.emptyList());
        Settings settings1 = Settings.builder().put(settings).put("xpack.security.transport.ssl.enabled", true).build();
        return new SecurityNetty4ServerTransport(
            settings1,
            version,
            threadPool,
            networkService,
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            namedWriteableRegistry,
            new NoneCircuitBreakerService(),
            null,
            createSSLService(settings1),
            new SharedGroupFactory(settings1)
        ) {

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
                    listener.onResponse(version.calculateMinimumCompatVersion());
                }
            }
        };
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
        secureSettings.setString("xpack.security.remote_cluster.ssl.secure_key_passphrase", "testnode");
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
                new DiscoveryNode(
                    "C",
                    new TransportAddress(InetAddress.getByName("localhost"), 9876),
                    emptyMap(),
                    emptySet(),
                    Version.CURRENT
                )
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
        try (TransportService service = buildService("TS_TPC", Version.CURRENT, Settings.EMPTY)) {
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
                assertEquals(TransportVersion.CURRENT, connection.getTransportVersion());
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
            try (MockTransportService serviceC = buildService("TS_C", version0, settings)) {
                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = new DiscoveryNode(
                    "server_node_id",
                    new TransportAddress(serverAddress),
                    attributes,
                    DiscoveryNodeRole.roles(),
                    Version.CURRENT
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
            try (MockTransportService serviceC = buildService("TS_C", version0, settings)) {
                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = new DiscoveryNode(
                    "server_node_id",
                    new TransportAddress(serverAddress),
                    attributes,
                    DiscoveryNodeRole.roles(),
                    Version.CURRENT
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
        try (MockTransportService service = buildService("TS_REQUIRED_CLIENT_AUTH", Version.CURRENT, settings)) {
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
        try (MockTransportService service = buildService("TS_NO_CLIENT_AUTH", Version.CURRENT, settings)) {
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
        try (MockTransportService service = buildService("TS_OPTIONAL_CLIENT_AUTH", Version.CURRENT, settings)) {
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
        try (MockTransportService service = buildService("TS_PROFILE_REQUIRE_CLIENT_AUTH", Version.CURRENT, settings)) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            TransportAddress clientAddress = originalTransport.profileBoundAddresses().get("client").publishAddress();
            DiscoveryNode node = new DiscoveryNode(service.getLocalNode().getId(), clientAddress, service.getLocalNode().getVersion());
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
        try (MockTransportService service = buildService("TS_PROFILE_NO_CLIENT_AUTH", Version.CURRENT, settings)) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            TransportAddress clientAddress = originalTransport.profileBoundAddresses().get("client").publishAddress();
            DiscoveryNode node = new DiscoveryNode(service.getLocalNode().getId(), clientAddress, service.getLocalNode().getVersion());
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
        try (MockTransportService service = buildService("TS_PROFILE_OPTIONAL_CLIENT_AUTH", Version.CURRENT, settings)) {
            TcpTransport originalTransport = (TcpTransport) service.getOriginalTransport();
            TransportAddress clientAddress = originalTransport.profileBoundAddresses().get("client").publishAddress();
            DiscoveryNode node = new DiscoveryNode(service.getLocalNode().getId(), clientAddress, service.getLocalNode().getVersion());
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
            .put("remote_cluster.enabled", "true")
            .put("remote_cluster.port", "9999")
            .put("xpack.security.remote_cluster.ssl.key", testnodeKey)
            .put("xpack.security.remote_cluster.ssl.certificate", testnodeCert)
            .put("xpack.security.remote_cluster.ssl.client_authentication", "none")
            .build();

        try (MockTransportService fcService = buildService("FC", Version.CURRENT, fcSettings)) {
            final TcpTransport originalTransport = (TcpTransport) fcService.getOriginalTransport();
            final TransportAddress remoteAccessAddress = originalTransport.profileBoundAddresses().get("_remote_cluster").publishAddress();
            final DiscoveryNode node = new DiscoveryNode(
                fcService.getLocalNode().getId(),
                remoteAccessAddress,
                fcService.getLocalNode().getVersion()
            );

            // 1. Connection will fail because FC server certificate is not trusted by default
            final Settings qcSettings1 = Settings.builder().build();
            try (MockTransportService qcService = buildService("QC", Version.CURRENT, qcSettings1)) {
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
            final Settings qcSettings2 = Settings.builder().put("xpack.security.remote_cluster.ssl.verification_mode", "none").build();
            try (
                MockTransportService qcService = buildService("QC", Version.CURRENT, qcSettings2);
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
                }

                final TcpChannel acceptedChannel = getAcceptedChannel(originalTransport, connection);
                assertThat(acceptedChannel.getProfile(), equalTo("_remote_cluster"));
            }

            // 3. Connection will success because QC is explicitly configured to trust FC server certificate
            final Settings qcSettings3 = Settings.builder()
                .put("xpack.security.remote_cluster.ssl.certificate_authorities", testnodeCert)
                .put("xpack.security.remote_cluster.ssl.verification_mode", "full")
                .build();
            try (
                MockTransportService qcService = buildService("QC", Version.CURRENT, qcSettings3);
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
            .put("remote_cluster.enabled", "true")
            .put("remote_cluster.port", "9999")
            .put("xpack.security.remote_cluster.ssl.enabled", "false")
            .build();

        try (MockTransportService fcService = buildService("FC", Version.CURRENT, fcSettings)) {
            final TcpTransport originalTransport = (TcpTransport) fcService.getOriginalTransport();
            final TransportAddress remoteAccessAddress = originalTransport.profileBoundAddresses().get("_remote_cluster").publishAddress();
            final DiscoveryNode node = new DiscoveryNode(
                fcService.getLocalNode().getId(),
                remoteAccessAddress,
                fcService.getLocalNode().getVersion()
            );
            final Settings qcSettings = Settings.builder().put("xpack.security.remote_cluster.ssl.enabled", "false").build();
            try (
                MockTransportService qcService = buildService("QC", Version.CURRENT, qcSettings);
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
            DiscoveryNode dummy = new DiscoveryNode(
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
            try (MockTransportService serviceC = buildService("TS_C", version0, settings)) {
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
            DiscoveryNode dummy = new DiscoveryNode(
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
            DiscoveryNode dummy = new DiscoveryNode(
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
            try (MockTransportService serviceC = buildService("TS_C", version0, settings)) {
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
}
