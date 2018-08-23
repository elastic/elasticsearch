/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.transport.netty4;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ExceptionsHelper;
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
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.BindTransportException;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportFuture;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.common.socket.SocketAccess;
import org.elasticsearch.xpack.core.security.transport.netty4.SecurityNetty4Transport;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.transport.nio.SecurityNioTransport;

import javax.net.SocketFactory;
import javax.net.ssl.HandshakeCompletedListener;
import javax.net.ssl.SSLSocket;
import java.io.IOException;
import java.net.InetAddress;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SimpleSecurityNetty4TransportTests extends AbstractSimpleTransportTestCase {

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
            stream.writeByte((byte)'S');
            stream.writeInt(-1);
            stream.flush();
        }
    }

    // TODO: These tests currently rely on plaintext transports

    @Override
    @AwaitsFix(bugUrl = "")
    public void testTcpHandshake() throws IOException, InterruptedException {
    }

    public void testHelloWorld2() throws Exception {
        try (MockTransportService serviceC = build(
            Settings.builder()
                .put("name", "TS_TEST")
                .put(TransportService.TRACE_LOG_INCLUDE_SETTING.getKey(), "")
                .put(TransportService.TRACE_LOG_EXCLUDE_SETTING.getKey(), "NOTHING")
                .build(),
            version0,
            null, true)) {
            DiscoveryNode nodeA = serviceA.getLocalNode();
            serviceC.acceptIncomingRequests();

            final CountDownLatch connectionLatch = new CountDownLatch(1);
            TransportConnectionListener waitForConnection = new TransportConnectionListener() {
                @Override
                public void onConnectionOpened(Transport.Connection connection) {
                    connectionLatch.countDown();
                }
            };
            final CountDownLatch requestLatch = new CountDownLatch(1);
            class TestRequestHandler implements TransportRequestHandler<TestRequest> {

                @Override
                public void messageReceived(TestRequest request, TransportChannel channel, Task task) throws Exception {
                    int i = 0;
                    requestLatch.countDown();
                }
            }

            serviceC.addConnectionListener(waitForConnection);
            HashMap<String, String> attributes = new HashMap<>();
            attributes.put("sni_server_name", "1.4.5.6");
            serviceC.connectToNode(new DiscoveryNode(nodeA.getName(), nodeA.getId(), nodeA.getEphemeralId(), nodeA.getHostName(), nodeA.getHostAddress(),
                nodeA.getAddress(), attributes, nodeA.getRoles(), nodeA.getVersion()));
            connectionLatch.await();
            serviceA.registerRequestHandler("internal:action1", TestRequest::new, ThreadPool.Names.SAME, new TestRequestHandler());
            serviceC.sendRequest(nodeA, "internal:action1", new TestRequest("REQ[1]"),
                TransportRequestOptions.builder().withCompress(randomBoolean()).build(), new TransportResponseHandler<TransportResponse>() {
                    @Override
                    public void handleResponse(TransportResponse response) {

                    }

                    @Override
                    public void handleException(TransportException exp) {
                        int i = 0;
                    }

                    @Override
                    public String executor() {
                        return null;
                    }
                });

            requestLatch.await();

            serviceC.close();
            serviceA.disconnectFromNode(serviceC.getLocalDiscoNode());
        }
    }
}
