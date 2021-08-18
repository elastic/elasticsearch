/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.TestProfiles;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLService;

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
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
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

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public abstract class AbstractSimpleSecurityTransportTestCase extends AbstractSimpleTransportTestCase {

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
            connectToNode(serviceA, new DiscoveryNode("C", new TransportAddress(InetAddress.getByName("localhost"), 9876),
                emptyMap(), emptySet(), Version.CURRENT));
            fail("Expected ConnectTransportException");
        } catch (ConnectTransportException e) {
            assertThat(e.getMessage(), containsString("connect_exception"));
            assertThat(e.getMessage(), containsString("[127.0.0.1:9876]"));
            Throwable cause = e.getCause();
            assertThat(cause, instanceOf(IOException.class));
        }

    }

    @Override
    public void testTcpHandshake() {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        TcpTransport originalTransport = (TcpTransport) serviceA.getOriginalTransport();

        ConnectionProfile connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        try (TransportService service = buildService("TS_TPC", Version.CURRENT, Settings.EMPTY)) {
            DiscoveryNode node = new DiscoveryNode("TS_TPC", "TS_TPC", service.boundAddress().publishAddress(), emptyMap(), emptySet(),
                version0);
            PlainActionFuture<Transport.Connection> future = PlainActionFuture.newFuture();
            originalTransport.openConnection(node, connectionProfile, future);
            try (TcpTransport.NodeChannels connection = (TcpTransport.NodeChannels) future.actionGet()) {
                assertEquals(connection.getVersion(), Version.CURRENT);
            }
        }
    }

    @SuppressForbidden(reason = "Need to open socket connection")
    public void testRenegotiation() throws Exception {
        assumeFalse("BCTLS doesn't support renegotiation: https://github.com/bcgit/bc-java/issues/593#issuecomment-533518845",
            inFipsJvm());
        // force TLSv1.2 since renegotiation is not supported by 1.3
        SSLService sslService =
            createSSLService(Settings.builder().put("xpack.security.transport.ssl.supported_protocols", "TLSv1.2").build());
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

            Settings settings = Settings.builder()
                .put("xpack.security.transport.ssl.verification_mode", "none")
                .build();
            try (MockTransportService serviceC = buildService("TS_C", version0, settings)) {
                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = new DiscoveryNode("server_node_id", new TransportAddress(serverAddress), attributes,
                    DiscoveryNodeRole.roles(), Version.CURRENT);

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

            Settings settings = Settings.builder()
                .put("xpack.security.transport.ssl.verification_mode", "none")
                .build();
            try (MockTransportService serviceC = buildService("TS_C", version0, settings)) {
                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = new DiscoveryNode("server_node_id", new TransportAddress(serverAddress), attributes,
                    DiscoveryNodeRole.roles(), Version.CURRENT);

                ConnectTransportException connectException = expectThrows(ConnectTransportException.class,
                    () -> connectToNode(serviceC, node, TestProfiles.LIGHT_PROFILE));

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
        value = randomCapitalization(SslClientAuthenticationMode.REQUIRED);;
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
            Optional<TcpChannel> maybeAccepted = getAcceptedChannels(transport)
                .stream().filter(c -> c.getRemoteAddress().equals(localAddress)).findFirst();
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
