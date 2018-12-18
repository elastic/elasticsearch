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
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.TransportSettings;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public abstract class AbstractSimpleSecurityTransportTestCase extends AbstractSimpleTransportTestCase {

    protected SSLService createSSLService() {
        return createSSLService(Settings.EMPTY);
    }

    protected SSLService createSSLService(Settings settings) {
        Path testnodeCert = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        Path testnodeKey = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
        secureSettings.setString("transport.profiles.some_profile.xpack.security.ssl.secure_key_passphrase", "testnode");
        secureSettings.setString("transport.profiles.some_other_profile.xpack.security.ssl.secure_key_passphrase", "testnode");
        Settings settings1 = Settings.builder()
            .put(settings)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.key", testnodeKey)
            .put("xpack.security.transport.ssl.certificate", testnodeCert)
            .put("transport.profiles.some_profile.xpack.security.ssl.enabled", true)
            .put("transport.profiles.some_profile.xpack.security.ssl.key", testnodeKey)
            .put("transport.profiles.some_profile.xpack.security.ssl.certificate", testnodeCert)
            .put("transport.profiles.some_other_profile.xpack.security.ssl.enabled", true)
            .put("transport.profiles.some_other_profile.xpack.security.ssl.key", testnodeKey)
            .put("transport.profiles.some_other_profile.xpack.security.ssl.certificate", testnodeCert)
            .put("path.home", createTempDir())
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
            .put(TransportSettings.TRACE_LOG_INCLUDE_SETTING.getKey(), "")
            .put(TransportSettings.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
            .put(TransportSettings.PORT.getKey(), port)
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
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/33772")
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
}
