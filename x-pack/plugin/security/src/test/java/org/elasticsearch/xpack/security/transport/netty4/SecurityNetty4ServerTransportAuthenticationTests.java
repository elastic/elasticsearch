/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.remote.RemoteClusterNodesAction;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.mocksocket.MockSocket;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.Compression;
import org.elasticsearch.transport.EmptyRequest;
import org.elasticsearch.transport.OutboundHandler;
import org.elasticsearch.transport.ProxyConnectionStrategy;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SniffConnectionStrategy;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.junit.After;
import org.junit.Before;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SecurityNetty4ServerTransportAuthenticationTests extends ESTestCase {

    private ThreadPool threadPool;
    // is non-null when authn passes successfully
    private AtomicReference<Exception> authenticationException;
    private String remoteClusterName;
    private SecurityNetty4ServerTransport remoteSecurityNetty4ServerTransport;
    private MockTransportService remoteTransportService;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        authenticationException = new AtomicReference<>();
        remoteClusterName = "test-remote_cluster_service_" + randomAlphaOfLength(8);
        Settings remoteSettings = Settings.builder()
            .put("node.name", getClass().getName())
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), remoteClusterName)
            .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_ENABLED.getKey(), "false")
            .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
            .put(RemoteClusterPortSettings.PORT.getKey(), 0)
            .put("transport.ignore_deserialization_errors", true)
            .build();
        remoteSettings = NodeRoles.nonRemoteClusterClientNode(remoteSettings);
        CrossClusterAccessAuthenticationService remoteCrossClusterAccessAuthenticationService = mock(
            CrossClusterAccessAuthenticationService.class
        );
        doAnswer(invocation -> {
            Exception authnException = authenticationException.get();
            if (authnException != null) {
                ((ActionListener<Void>) invocation.getArguments()[1]).onFailure(authnException);
            } else {
                ((ActionListener<Void>) invocation.getArguments()[1]).onResponse(null);
            }
            return null;
        }).when(remoteCrossClusterAccessAuthenticationService).tryAuthenticate(any(Map.class), anyActionListener());
        remoteSecurityNetty4ServerTransport = new SecurityNetty4ServerTransport(
            remoteSettings,
            TransportVersion.current(),
            threadPool,
            new NetworkService(List.of()),
            PageCacheRecycler.NON_RECYCLING_INSTANCE,
            new NamedWriteableRegistry(List.of()),
            new NoneCircuitBreakerService(),
            null,
            mock(SSLService.class),
            new SharedGroupFactory(remoteSettings),
            remoteCrossClusterAccessAuthenticationService
        );
        remoteTransportService = MockTransportService.createNewService(
            remoteSettings,
            remoteSecurityNetty4ServerTransport,
            VersionInformation.CURRENT,
            threadPool,
            null,
            Collections.emptySet(),
            // IMPORTANT: we have to mock authentication in two places: one in the "CrossClusterAccessAuthenticationService" and the
            // other before the action handler here. This is in order to accurately simulate the complete Elasticsearch node behavior.
            new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    Executor executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    return (request, channel, task) -> {
                        Exception authnException = authenticationException.get();
                        if (authnException != null) {
                            channel.sendResponse(authnException);
                        } else {
                            actualHandler.messageReceived(request, channel, task);
                        }
                    };
                }
            }
        );
        DiscoveryNode remoteNode = remoteTransportService.getLocalNode();
        remoteTransportService.registerRequestHandler(
            RemoteClusterNodesAction.TYPE.name(),
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            RemoteClusterNodesAction.Request::new,
            (request, channel, task) -> channel.sendResponse(new RemoteClusterNodesAction.Response(List.of(remoteNode)))
        );
        remoteTransportService.start();
        remoteTransportService.acceptIncomingRequests();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        logger.info("tearDown");
        super.tearDown();
        IOUtils.close(
            remoteTransportService,
            remoteSecurityNetty4ServerTransport,
            () -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        );
    }

    public void testProxyStrategyConnectionClosesWhenAuthenticatorAlwaysFails() throws Exception {
        // all requests fail authn
        authenticationException.set(new ElasticsearchSecurityException("authn failure"));
        try (
            MockTransportService localService = MockTransportService.createNewService(
                proxyLocalTransportSettings(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            localService.start();
            // all attempts to obtain a connections will fail
            for (int i = 0; i < randomIntBetween(2, 4); i++) {
                CountDownLatch connectionTestDone = new CountDownLatch(1);
                // {@code RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME} fails authn (both of them) and the connection is
                // always closed after receiving an error response
                localService.getRemoteClusterService()
                    .maybeEnsureConnectedAndGetConnection(remoteClusterName, true, ActionListener.wrap(connection -> {
                        logger.info("Unexpected: a connection is available");
                        connectionTestDone.countDown();
                        fail("No connection should be available if authn fails");
                    }, e -> {
                        logger.info("Expected: no connection could not be established");
                        try {
                            assertThat(e, instanceOf(RemoteTransportException.class));
                            assertThat(e.getCause(), instanceOf(authenticationException.get().getClass()));
                        } finally {
                            connectionTestDone.countDown();
                        }
                    }));
                assertTrue(connectionTestDone.await(10L, TimeUnit.SECONDS));
            }
        }
        // but if authn passes, valid connections are available
        authenticationException.set(null);
        try (
            MockTransportService localService = MockTransportService.createNewService(
                proxyLocalTransportSettings(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            localService.start();
            CountDownLatch connectionTestDone = new CountDownLatch(1);
            localService.getRemoteClusterService()
                .maybeEnsureConnectedAndGetConnection(remoteClusterName, true, ActionListener.wrap(connection -> {
                    logger.info("Expected: a connection is available");
                    connectionTestDone.countDown();
                }, e -> {
                    logger.info("Unexpected: no connection could be established");
                    connectionTestDone.countDown();
                    fail("connection could not be established");
                    throw new RuntimeException(e);
                }));
            assertTrue(connectionTestDone.await(10L, TimeUnit.SECONDS));
        }
    }

    public void testSniffStrategyNoConnectionWhenAuthenticatorAlwaysFails() throws Exception {
        // all requests fail authn
        authenticationException.set(new ElasticsearchSecurityException("authn failure"));
        try (
            MockTransportService localService = MockTransportService.createNewService(
                sniffLocalTransportSettings(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            localService.start();
            // obtain some connections and check that they'll be promptly closed
            for (int i = 0; i < randomIntBetween(2, 4); i++) {
                CountDownLatch connectionTestDone = new CountDownLatch(1);
                // the failed authentication during handshake must surely close the connection before
                // {@code RemoteClusterNodesAction.NAME} is executed, so node sniffing will fail
                localService.getRemoteClusterService()
                    .maybeEnsureConnectedAndGetConnection(remoteClusterName, true, ActionListener.wrap(connection -> {
                        logger.info("Unexpected: a connection is available");
                        connectionTestDone.countDown();
                        fail("No connection should be available if authn fails");
                    }, e -> {
                        logger.info("Expected: no connection could be established");
                        try {
                            assertThat(e, instanceOf(RemoteTransportException.class));
                            assertThat(e.getCause(), instanceOf(authenticationException.get().getClass()));
                        } finally {
                            connectionTestDone.countDown();
                        }
                    }));
                assertTrue(connectionTestDone.await(10L, TimeUnit.SECONDS));
            }
        }
        // but if authn passes, valid connections are available
        authenticationException.set(null);
        try (
            MockTransportService localService = MockTransportService.createNewService(
                sniffLocalTransportSettings(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            localService.start();
            CountDownLatch connectionTestDone = new CountDownLatch(1);
            localService.getRemoteClusterService()
                .maybeEnsureConnectedAndGetConnection(remoteClusterName, true, ActionListener.wrap(connection -> {
                    logger.info("Expected: a connection is available");
                    connectionTestDone.countDown();
                }, e -> {
                    logger.info("Unexpected: no connection could be established");
                    connectionTestDone.countDown();
                    fail("connection could not be established");
                    throw new RuntimeException(e);
                }));
            assertTrue(connectionTestDone.await(10L, TimeUnit.SECONDS));
        }
    }

    public void testConnectionWorksForPing() throws Exception {
        authenticationException.set(new ElasticsearchSecurityException("authn failure"));
        TransportAddress[] boundRemoteIngressAddresses = remoteSecurityNetty4ServerTransport.boundRemoteIngressAddress().boundAddresses();
        InetSocketAddress remoteIngressTransportAddress = randomFrom(boundRemoteIngressAddresses).address();
        // ping message
        final BytesStreamOutput bytesStreamOutput = new BytesStreamOutput();
        bytesStreamOutput.writeBytes(new byte[] { (byte) 'E', (byte) 'S' });
        bytesStreamOutput.writeInt(-1);
        try (Socket socket = new MockSocket(remoteIngressTransportAddress.getAddress(), remoteIngressTransportAddress.getPort())) {
            final byte[] pingBytes = Arrays.copyOfRange(bytesStreamOutput.bytes().array(), 0, 6);
            socket.getOutputStream().write(pingBytes);
            socket.getOutputStream().flush();
            // We should receive the ping back
            final byte[] responseBytes = socket.getInputStream().readNBytes(6);
            assertThat(responseBytes, equalTo(pingBytes));
            try {
                socket.setSoTimeout(1000);
                socket.getInputStream().read();
                fail("should not reach here");
            } catch (SocketTimeoutException e) {
                // timeout exception means the server is still connected. Just no data is coming which is normal
            }
        }
    }

    public void testConnectionDisconnectedWhenAuthnFails() throws Exception {
        authenticationException.set(new ElasticsearchSecurityException("authn failure"));
        TransportAddress[] boundRemoteIngressAddresses = remoteSecurityNetty4ServerTransport.boundRemoteIngressAddress().boundAddresses();
        InetSocketAddress remoteIngressTransportAddress = randomFrom(boundRemoteIngressAddresses).address();
        try (Socket socket = new MockSocket(remoteIngressTransportAddress.getAddress(), remoteIngressTransportAddress.getPort())) {
            Recycler<BytesRef> recycler = new BytesRefRecycler(PageCacheRecycler.NON_RECYCLING_INSTANCE);
            RecyclerBytesStreamOutput out = new RecyclerBytesStreamOutput(recycler);
            BytesReference bytesReference = OutboundHandler.serialize(
                OutboundHandler.MessageDirection.REQUEST,
                "internal:whatever",
                randomNonNegativeLong(),
                false,
                TransportVersion.current(),
                randomFrom(Compression.Scheme.DEFLATE, Compression.Scheme.LZ4, null),
                new EmptyRequest(),
                threadPool.getThreadContext(),
                out
            );
            socket.getOutputStream().write(Arrays.copyOfRange(bytesReference.array(), 0, bytesReference.length()));
            socket.getOutputStream().flush();

            final String response = new String(socket.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            assertThat(response, containsString("authn failure"));
            // -1 means the other side has disconnected
            assertThat(socket.getInputStream().read(), equalTo(-1));
        }
    }

    private Settings sniffLocalTransportSettings() {
        Settings localSettings = Settings.builder()
            .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
            .put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(remoteClusterName).getKey(), "sniff")
            .put(
                SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(remoteClusterName).getKey(),
                remoteTransportService.boundRemoteAccessAddress().publishAddress().toString()
            )
            .put(
                SniffConnectionStrategy.REMOTE_CONNECTIONS_PER_CLUSTER.getKey(),
                randomIntBetween(1, 3) // easier to debug with just 1 connection
            )
            .put(
                SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS.getConcreteSettingForNamespace(remoteClusterName).getKey(),
                randomIntBetween(1, 3) // easier to debug with just 1 connection
            )
            .build();
        {
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(
                RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace(remoteClusterName).getKey(),
                randomAlphaOfLength(20)
            );
            return Settings.builder().put(localSettings).setSecureSettings(secureSettings).build();
        }
    }

    private Settings proxyLocalTransportSettings() {
        Settings localSettings = Settings.builder()
            .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
            .put(RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(remoteClusterName).getKey(), "proxy")
            .put(
                ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace(remoteClusterName).getKey(),
                remoteTransportService.boundRemoteAccessAddress().publishAddress().toString()
            )
            .put(
                ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace(remoteClusterName).getKey(),
                randomIntBetween(1, 3) // easier to debug with just 1 connection
            )
            .build();
        {
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(
                RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace(remoteClusterName).getKey(),
                randomAlphaOfLength(20)
            );
            return Settings.builder().put(localSettings).setSecureSettings(secureSettings).build();
        }
    }

}
