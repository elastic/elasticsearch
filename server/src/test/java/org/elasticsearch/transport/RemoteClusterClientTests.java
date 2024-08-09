/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.TransportSearchScrollAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.VersionInformation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.elasticsearch.test.NodeRoles.removeRoles;
import static org.elasticsearch.transport.AbstractSimpleTransportTestCase.IGNORE_DESERIALIZATION_ERRORS_SETTING;
import static org.elasticsearch.transport.RemoteClusterConnectionTests.startTransport;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class RemoteClusterClientTests extends ESTestCase {

    private static final String TEST_THREAD_POOL_NAME = "test_thread_pool";

    private final ThreadPool threadPool = new TestThreadPool(
        getClass().getName(),
        new ScalingExecutorBuilder(TEST_THREAD_POOL_NAME, 1, 1, TimeValue.timeValueSeconds(60), true)
    );

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testConnectAndExecuteRequest() throws Exception {
        Settings remoteSettings = Settings.builder()
            .put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "foo_bar_cluster")
            .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true) // suppress assertions to test production error-handling
            .build();
        try (
            MockTransportService remoteTransport = startTransport(
                "remote_node",
                Collections.emptyList(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                remoteSettings
            )
        ) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();

            Settings localSettings = Settings.builder()
                .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
                .put("cluster.remote.test.seeds", remoteNode.getAddress().getAddress() + ":" + remoteNode.getAddress().getPort())
                .put(IGNORE_DESERIALIZATION_ERRORS_SETTING.getKey(), true) // suppress assertions to test production error-handling
                .build();
            try (
                MockTransportService service = MockTransportService.createNewService(
                    localSettings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                // following two log lines added to investigate #41745, can be removed once issue is closed
                logger.info("Start accepting incoming requests on local transport service");
                service.acceptIncomingRequests();
                logger.info("now accepting incoming requests on local transport");
                RemoteClusterService remoteClusterService = service.getRemoteClusterService();
                assertTrue(remoteClusterService.isRemoteNodeConnected("test", remoteNode));
                var client = remoteClusterService.getRemoteClusterClient(
                    "test",
                    threadPool.executor(TEST_THREAD_POOL_NAME),
                    randomFrom(RemoteClusterService.DisconnectedStrategy.values())
                );
                ClusterStateResponse clusterStateResponse = safeAwait(
                    listener -> client.execute(
                        ClusterStateAction.REMOTE_TYPE,
                        new ClusterStateRequest(),
                        ActionListener.runBefore(
                            listener,
                            () -> assertTrue(Thread.currentThread().getName().contains('[' + TEST_THREAD_POOL_NAME + ']'))
                        )
                    )
                );
                assertNotNull(clusterStateResponse);
                assertEquals("foo_bar_cluster", clusterStateResponse.getState().getClusterName().value());
                // also test a failure, there is no handler for scroll registered
                ActionNotFoundTransportException ex = asInstanceOf(
                    ActionNotFoundTransportException.class,
                    ExceptionsHelper.unwrapCause(
                        safeAwaitFailure(
                            SearchResponse.class,
                            listener -> client.execute(TransportSearchScrollAction.REMOTE_TYPE, new SearchScrollRequest(""), listener)
                        )
                    )
                );
                assertEquals("No handler for action [indices:data/read/scroll]", ex.getMessage());
            }
        }
    }

    @TestLogging(
        value = "org.elasticsearch.transport.SniffConnectionStrategy:TRACE,org.elasticsearch.transport.ClusterConnectionManager:TRACE",
        reason = "debug intermittent test failure"
    )
    public void testEnsureWeReconnect() throws Exception {
        Settings remoteSettings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "foo_bar_cluster").build();
        try (
            MockTransportService remoteTransport = startTransport(
                "remote_node",
                Collections.emptyList(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                remoteSettings
            )
        ) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();
            Settings localSettings = Settings.builder()
                .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
                .put("cluster.remote.test.seeds", remoteNode.getAddress().getAddress() + ":" + remoteNode.getAddress().getPort())
                .put("cluster.remote.test.skip_unavailable", "false") // ensureConnected is only true for skip_unavailable=false
                .build();
            try (
                MockTransportService service = MockTransportService.createNewService(
                    localSettings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                service.start();
                // this test is not perfect since we might reconnect concurrently but it will fail most of the time if we don't have
                // the right calls in place in the RemoteAwareClient
                service.acceptIncomingRequests();
                RemoteClusterService remoteClusterService = service.getRemoteClusterService();
                assertBusy(() -> assertTrue(remoteClusterService.isRemoteNodeConnected("test", remoteNode)));
                for (int i = 0; i < 10; i++) {
                    RemoteClusterConnection remoteClusterConnection = remoteClusterService.getRemoteClusterConnection("test");
                    assertBusy(remoteClusterConnection::assertNoRunningConnections);
                    ConnectionManager connectionManager = remoteClusterConnection.getConnectionManager();
                    Transport.Connection connection = connectionManager.getConnection(remoteNode);
                    PlainActionFuture<Void> closeFuture = new PlainActionFuture<>();
                    connection.addCloseListener(closeFuture);
                    connectionManager.disconnectFromNode(remoteNode);
                    closeFuture.get();

                    var client = remoteClusterService.getRemoteClusterClient(
                        "test",
                        EsExecutors.DIRECT_EXECUTOR_SERVICE,
                        randomFrom(
                            RemoteClusterService.DisconnectedStrategy.RECONNECT_IF_DISCONNECTED,
                            RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
                        )
                    );
                    ClusterStateResponse clusterStateResponse = safeAwait(
                        listener -> client.execute(ClusterStateAction.REMOTE_TYPE, new ClusterStateRequest(), listener)
                    );
                    assertNotNull(clusterStateResponse);
                    assertEquals("foo_bar_cluster", clusterStateResponse.getState().getClusterName().value());
                    assertTrue(remoteClusterConnection.isNodeConnected(remoteNode));
                }
            }
        }
    }

    public void testRemoteClusterServiceNotEnabled() {
        final Settings settings = removeRoles(Set.of(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE));
        try (
            MockTransportService service = MockTransportService.createNewService(
                settings,
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                null
            )
        ) {
            service.start();
            service.acceptIncomingRequests();
            final RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> remoteClusterService.getRemoteClusterClient(
                    "test",
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    randomFrom(RemoteClusterService.DisconnectedStrategy.values())
                )
            );
            assertThat(e.getMessage(), equalTo("this node does not have the remote_cluster_client role"));
        }
    }

    public void testQuicklySkipUnavailableClusters() throws Exception {
        Settings remoteSettings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "foo_bar_cluster").build();
        try (
            MockTransportService remoteTransport = startTransport(
                "remote_node",
                Collections.emptyList(),
                VersionInformation.CURRENT,
                TransportVersion.current(),
                threadPool,
                remoteSettings
            )
        ) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();

            Settings localSettings = Settings.builder()
                .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
                .put("cluster.remote.test.seeds", remoteNode.getAddress().getAddress() + ":" + remoteNode.getAddress().getPort())
                .put("cluster.remote.test.skip_unavailable", true)
                .put("cluster.remote.initial_connect_timeout", "0s")
                .build();
            try (
                MockTransportService service = MockTransportService.createNewService(
                    localSettings,
                    VersionInformation.CURRENT,
                    TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                CountDownLatch latch = new CountDownLatch(1);
                service.addConnectBehavior(remoteTransport, (transport, discoveryNode, profile, listener) -> {
                    safeAwait(latch);
                    listener.onFailure(new ConnectTransportException(discoveryNode, "simulated"));
                });
                service.start();
                service.acceptIncomingRequests();
                RemoteClusterService remoteClusterService = service.getRemoteClusterService();
                var client = remoteClusterService.getRemoteClusterClient(
                    "test",
                    EsExecutors.DIRECT_EXECUTOR_SERVICE,
                    randomFrom(
                        RemoteClusterService.DisconnectedStrategy.FAIL_IF_DISCONNECTED,
                        RemoteClusterService.DisconnectedStrategy.RECONNECT_UNLESS_SKIP_UNAVAILABLE
                    )
                );

                try {
                    assertFalse(remoteClusterService.isRemoteNodeConnected("test", remoteNode));

                    // check that we quickly fail
                    ESTestCase.assertThat(
                        safeAwaitFailure(
                            ClusterStateResponse.class,
                            listener -> client.execute(ClusterStateAction.REMOTE_TYPE, new ClusterStateRequest(), listener)
                        ),
                        instanceOf(ConnectTransportException.class)
                    );
                } finally {
                    service.clearAllRules();
                    latch.countDown();
                }

                assertBusy(() -> {
                    ClusterStateResponse ignored = safeAwait(
                        listener -> client.execute(ClusterStateAction.REMOTE_TYPE, new ClusterStateRequest(), listener)
                    );
                    // keep retrying on an exception, the goal is to check that we eventually reconnect
                });
                assertTrue(remoteClusterService.isRemoteNodeConnected("test", remoteNode));
            }
        }
    }
}
