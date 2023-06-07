/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
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

public class RemoteClusterClientTests extends ESTestCase {
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

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
                Version.CURRENT,
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
                    Version.CURRENT,
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
                Client client = remoteClusterService.getRemoteClusterClient(threadPool, "test", randomBoolean());
                ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().get();
                assertNotNull(clusterStateResponse);
                assertEquals("foo_bar_cluster", clusterStateResponse.getState().getClusterName().value());
                // also test a failure, there is no handler for scroll registered
                ActionNotFoundTransportException ex = expectThrows(
                    ActionNotFoundTransportException.class,
                    () -> client.prepareSearchScroll("").get()
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
                Version.CURRENT,
                    TransportVersion.current(),
                threadPool,
                remoteSettings
            )
        ) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();
            Settings localSettings = Settings.builder()
                .put(onlyRole(DiscoveryNodeRole.REMOTE_CLUSTER_CLIENT_ROLE))
                .put("cluster.remote.test.seeds", remoteNode.getAddress().getAddress() + ":" + remoteNode.getAddress().getPort())
                .build();
            try (
                MockTransportService service = MockTransportService.createNewService(
                    localSettings,
                    Version.CURRENT,
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
                    PlainActionFuture<Void> closeFuture = PlainActionFuture.newFuture();
                    connection.addCloseListener(closeFuture);
                    connectionManager.disconnectFromNode(remoteNode);
                    closeFuture.get();

                    Client client = remoteClusterService.getRemoteClusterClient(threadPool, "test", true);
                    ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().get();
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
                Version.CURRENT,
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
                () -> remoteClusterService.getRemoteClusterClient(threadPool, "test", randomBoolean())
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
                Version.CURRENT,
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
                    Version.CURRENT,
                        TransportVersion.current(),
                    threadPool,
                    null
                )
            ) {
                CountDownLatch latch = new CountDownLatch(1);
                service.addConnectBehavior(remoteTransport, (transport, discoveryNode, profile, listener) -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    listener.onFailure(new ConnectTransportException(discoveryNode, "simulated"));
                });
                service.start();
                service.acceptIncomingRequests();
                RemoteClusterService remoteClusterService = service.getRemoteClusterService();
                Client client = remoteClusterService.getRemoteClusterClient(threadPool, "test");

                try {
                    assertFalse(remoteClusterService.isRemoteNodeConnected("test", remoteNode));

                    // check that we quickly fail
                    expectThrows(
                        NoSuchRemoteClusterException.class,
                        () -> client.admin().cluster().prepareState().get(TimeValue.timeValueSeconds(10))
                    );
                } finally {
                    service.clearAllRules();
                    latch.countDown();
                }

                assertBusy(() -> {
                    try {
                        client.admin().cluster().prepareState().get();
                    } catch (NoSuchRemoteClusterException e) {
                        // keep retrying on this exception, the goal is to check that we eventually reconnect
                        throw new AssertionError(e);
                    }
                });
                assertTrue(remoteClusterService.isRemoteNodeConnected("test", remoteNode));
            }
        }
    }
}
