/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport.netty4;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase.TestRequest;
import org.elasticsearch.transport.ProxyConnectionStrategy;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.RemoteConnectionStrategy;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.SharedGroupFactory;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.CrossClusterAccessAuthenticationService;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.transport.AbstractSimpleTransportTestCase.IGNORE_DESERIALIZATION_ERRORS_SETTING;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class SecurityNetty4ServerTransportAuthenticationTests extends ESTestCase {

    private ThreadPool threadPool;
    private Settings remoteSettings;
    private SecurityNetty4ServerTransport remoteSecurityNetty4ServerTransport;
    private TransportService remoteTransportService;
    private DiscoveryNode remoteNode;
    private CrossClusterAccessAuthenticationService remoteCrossClusterAccessAuthenticationService;

    @SuppressWarnings("unchecked")
    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        remoteSettings = Settings.builder()
            .put("node.name", getClass().getName())
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "remote_security_server_transport")
            .put(XPackSettings.TRANSPORT_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_ENABLED.getKey(), "false")
            .put(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_ENABLED.getKey(), "false")
            .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), "true")
            .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), "true")
            .build();
        remoteSettings = NodeRoles.nonRemoteClusterClientNode(remoteSettings);
        remoteCrossClusterAccessAuthenticationService = mock(CrossClusterAccessAuthenticationService.class);
        doAnswer(invocation -> {
            ((ActionListener<Void>) invocation.getArguments()[1]).onResponse(null);
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
            TransportService.NOOP_TRANSPORT_INTERCEPTOR // TODO maybe use transport interceptor to set credentials
        );
        remoteTransportService.start();
        remoteTransportService.acceptIncomingRequests();
        // TODO maybe use connection listeners
        remoteNode = remoteTransportService.getLocalNode();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        IOUtils.close(
            remoteTransportService,
            remoteSecurityNetty4ServerTransport,
            () -> ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS)
        );
    }

    public void testConnectWithRejectingAuthenticator() throws InterruptedException {
        Settings localSettings = Settings.builder()
            .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
            .put(
                RemoteConnectionStrategy.REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace("remote_security_server_transport").getKey(),
                "proxy"
            )
            .put(
                ProxyConnectionStrategy.PROXY_ADDRESS.getConcreteSettingForNamespace("remote_security_server_transport").getKey(),
                remoteTransportService.boundRemoteAccessAddress().publishAddress().toString()
            )
            .put(
                ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS.getConcreteSettingForNamespace("remote_security_server_transport")
                    .getKey(),
                1
            )
            .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true) // suppress assertions to test production error-handling
            .build();
        {
            final MockSecureSettings secureSettings = new MockSecureSettings();
            secureSettings.setString(
                    RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS.getConcreteSettingForNamespace("remote_security_server_transport").getKey(),
                    randomAlphaOfLength(20)
            );
            localSettings = Settings.builder().put(localSettings).setSecureSettings(secureSettings).build();
        }
        CountDownLatch responseLatch = new CountDownLatch(1);
        TransportResponseHandler<TransportResponse.Empty> transportResponseHandler = new TransportResponseHandler.Empty() {
            @Override
            public Executor executor(ThreadPool threadPool) {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse() {
                responseLatch.countDown();
            }

            @Override
            public void handleException(TransportException exp) {
                responseLatch.countDown();
            }
        };
        try (
            MockTransportService localService = MockTransportService.createNewService(
                localSettings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool
            )
        ) {
            localService.start();
            RemoteClusterService remoteClusterService = localService.getRemoteClusterService();
            remoteClusterService.maybeEnsureConnectedAndGetConnection(
                "remote_security_server_transport",
                true,
                ActionListener.wrap(connection -> {
                    localService.sendRequest(
                            connection,
                            "internal:action",
                            new TestRequest("hello world"),
                            TransportRequestOptions.EMPTY,
                            transportResponseHandler
                    );
                }, e -> {})
            );
            assertTrue(responseLatch.await(5, TimeUnit.SECONDS));
        }
    }

//                    connection.sendRequest(
//                        randomNonNegativeLong(),
//                        RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME,
//                        TransportRequest.Empty.INSTANCE,
//                        TransportRequestOptions.of(null, TransportRequestOptions.Type.REG)
//                    );
//                    connection.getNode();
//                    connection.isClosed();
//            assertTrue(remoteClusterService.isRemoteNodeConnected("remote_security_server_transport", remoteNode));
//            Client client = remoteClusterService.getRemoteClusterClient(
//                    threadPool,
//                    "test",
//                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
//                    randomBoolean()
//            );
//            client.admin();
//            Transport.Connection connection = AbstractSimpleTransportTestCase.openConnection(clientService, serverNode, null);
//            Transport.Connection connection2 = AbstractSimpleTransportTestCase.openConnection(clientService, serverNode, null);
//        final CountDownLatch connectedLatch = new CountDownLatch(1);
//        TransportConnectionListener waitForConnection = new TransportConnectionListener() {
//            @Override
//            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
//                connectedLatch.countDown();
//            }
//
//            @Override
//            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
//                fail("disconnect should not be called " + node);
//            }
//        };
//        serverTransportService.addConnectionListener(waitForConnection);
//        assertThat("failed to wait for node to connect", connectedLatch.await(5, TimeUnit.SECONDS), equalTo(true));
//        assertNumHandshakes(1, securityNetty4ServerTransport);
}
