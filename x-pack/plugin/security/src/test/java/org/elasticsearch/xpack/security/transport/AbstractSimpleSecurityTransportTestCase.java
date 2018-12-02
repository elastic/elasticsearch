/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SNIHostName;
import javax.net.ssl.SNIMatcher;
import javax.net.ssl.SNIServerName;
import javax.net.ssl.SSLContext;
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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public abstract class AbstractSimpleSecurityTransportTestCase extends AbstractSimpleTransportTestCase {

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

    protected SSLService createSSLService() {
        return createSSLService(Settings.EMPTY);
    }

    protected SSLService createSSLService(Settings settings) {
        Path testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.ssl.secure_key_passphrase", "testnode");
        Settings settings1 = Settings.builder()
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.ssl.key", testnodeKey)
            .put("xpack.ssl.certificate", testnodeCert)
            .put("path.home", createTempDir())
            .put(settings)
            .setSecureSettings(secureSettings)
            .build();
        try {
            return new SSLService(settings1, TestEnvironment.newEnvironment(settings1));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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
            MockTransportService transportService = build(settings, Version.CURRENT, clusterSettings, true);
            try {
                transportService.start();
            } finally {
                transportService.stop();
                transportService.close();
            }
        });
        assertEquals("Failed to bind to [" + port + "]", bindTransportException.getMessage());
    }

    @Override
    public void testTcpHandshake() {
        assumeTrue("only tcp transport has a handshake method", serviceA.getOriginalTransport() instanceof TcpTransport);
        TcpTransport originalTransport = (TcpTransport) serviceA.getOriginalTransport();

        ConnectionProfile connectionProfile = ConnectionProfile.buildDefaultConnectionProfile(Settings.EMPTY);
        try (TransportService service = buildService("TS_TPC", Version.CURRENT, null)) {
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

        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.ssl");
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

            Settings settings = Settings.builder().put("name", "TS_TEST").put("xpack.ssl.verification_mode", "none").build();
            try (MockTransportService serviceC = build(settings, version0, null, true)) {
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
            }
        }
    }

    public void testInvalidSNIServerName() throws Exception {
        assumeFalse("Can't run in a FIPS JVM, TrustAllConfig is not a SunJSSE TrustManagers", inFipsJvm());
        SSLService sslService = createSSLService();

        final SSLConfiguration sslConfiguration = sslService.getSSLConfiguration("xpack.ssl");
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

            Settings settings = Settings.builder().put("name", "TS_TEST").put("xpack.ssl.verification_mode", "none").build();
            try (MockTransportService serviceC = build(settings, version0, null, true)) {
                serviceC.acceptIncomingRequests();

                HashMap<String, String> attributes = new HashMap<>();
                attributes.put("server_name", sniIp);
                DiscoveryNode node = new DiscoveryNode("server_node_id", new TransportAddress(serverAddress), attributes,
                    EnumSet.allOf(DiscoveryNode.Role.class), Version.CURRENT);

                ConnectTransportException connectException = expectThrows(ConnectTransportException.class,
                    () -> serviceC.connectToNode(node, SINGLE_CHANNEL_PROFILE));

                assertThat(connectException.getMessage(), containsString("invalid DiscoveryNode server_name [invalid_hostname]"));
            }
        }
    }
}
