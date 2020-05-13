/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

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
        Settings remoteSettings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "foo_bar_cluster").build();
        try (MockTransportService remoteTransport = startTransport("remote_node", Collections.emptyList(), Version.CURRENT, threadPool,
            remoteSettings)) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();

            Settings localSettings = Settings.builder()
                .put(Node.NODE_REMOTE_CLUSTER_CLIENT.getKey(), true)
                .put("cluster.remote.test.seeds",
                    remoteNode.getAddress().getAddress() + ":" + remoteNode.getAddress().getPort()).build();
            try (MockTransportService service = MockTransportService.createNewService(localSettings, Version.CURRENT, threadPool, null)) {
                service.start();
                // following two log lines added to investigate #41745, can be removed once issue is closed
                logger.info("Start accepting incoming requests on local transport service");
                service.acceptIncomingRequests();
                logger.info("now accepting incoming requests on local transport");
                RemoteClusterService remoteClusterService = service.getRemoteClusterService();
                assertTrue(remoteClusterService.isRemoteNodeConnected("test", remoteNode));
                Client client = remoteClusterService.getRemoteClusterClient(threadPool, "test");
                ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().get();
                assertNotNull(clusterStateResponse);
                assertEquals("foo_bar_cluster", clusterStateResponse.getState().getClusterName().value());
                // also test a failure, there is no handler for scroll registered
                ActionNotFoundTransportException ex = expectThrows(ActionNotFoundTransportException.class,
                    () -> client.prepareSearchScroll("").get());
                assertEquals("No handler for action [indices:data/read/scroll]", ex.getMessage());
            }
        }
    }

    @TestLogging(
        value = "org.elasticsearch.transport.SniffConnectionStrategy:TRACE,org.elasticsearch.transport.ClusterConnectionManager:TRACE",
        reason = "debug intermittent test failure")
    public void testEnsureWeReconnect() throws Exception {
        Settings remoteSettings = Settings.builder().put(ClusterName.CLUSTER_NAME_SETTING.getKey(), "foo_bar_cluster").build();
        try (MockTransportService remoteTransport = startTransport("remote_node", Collections.emptyList(), Version.CURRENT, threadPool,
            remoteSettings)) {
            DiscoveryNode remoteNode = remoteTransport.getLocalDiscoNode();
            Settings localSettings = Settings.builder()
                .put(Node.NODE_REMOTE_CLUSTER_CLIENT.getKey(), true)
                .put("cluster.remote.test.seeds",
                    remoteNode.getAddress().getAddress() + ":" + remoteNode.getAddress().getPort()).build();
            try (MockTransportService service = MockTransportService.createNewService(localSettings, Version.CURRENT, threadPool, null)) {
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

                    Client client = remoteClusterService.getRemoteClusterClient(threadPool, "test");
                    ClusterStateResponse clusterStateResponse = client.admin().cluster().prepareState().execute().get();
                    assertNotNull(clusterStateResponse);
                    assertEquals("foo_bar_cluster", clusterStateResponse.getState().getClusterName().value());
                    assertTrue(remoteClusterConnection.isNodeConnected(remoteNode));
                }
            }
        }
    }

    public void testRemoteClusterServiceNotEnabled() {
        final Settings settings = Settings.builder().put(Node.NODE_REMOTE_CLUSTER_CLIENT.getKey(), false).build();
        try (MockTransportService service = MockTransportService.createNewService(settings, Version.CURRENT, threadPool, null)) {
            service.start();
            service.acceptIncomingRequests();
            final RemoteClusterService remoteClusterService = service.getRemoteClusterService();
            final IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> remoteClusterService.getRemoteClusterClient(threadPool, "test"));
            assertThat(e.getMessage(), equalTo("this node does not have the remote_cluster_client role"));
        }
    }

}
