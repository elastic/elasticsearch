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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class RemoteConnectionManagerTests extends ESTestCase {

    private Transport transport;
    private RemoteConnectionManager remoteConnectionManager;
    private ConnectionManager.ConnectionValidator validator = (connection, profile, listener) -> listener.onResponse(null);

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transport = mock(Transport.class);
        remoteConnectionManager = new RemoteConnectionManager("remote-cluster", new ClusterConnectionManager(Settings.EMPTY, transport));
    }

    @SuppressWarnings("unchecked")
    public void testGetConnection() {
        TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 1000);

        doAnswer(invocationOnMock -> {
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(new TestRemoteConnection((DiscoveryNode) invocationOnMock.getArguments()[0]));
            return null;
        }).when(transport).openConnection(any(DiscoveryNode.class), any(ConnectionProfile.class), any(ActionListener.class));

        DiscoveryNode node1 = new DiscoveryNode("node-1", address, Version.CURRENT);
        PlainActionFuture<Void> future1 = PlainActionFuture.newFuture();
        remoteConnectionManager.connectToNode(node1, null, validator, future1);
        assertTrue(future1.isDone());

        // Add duplicate connect attempt to ensure that we do not get duplicate connections in the round robin
        remoteConnectionManager.connectToNode(node1, null, validator, PlainActionFuture.newFuture());

        DiscoveryNode node2 = new DiscoveryNode("node-2", address, Version.CURRENT.minimumCompatibilityVersion());
        PlainActionFuture<Void> future2 = PlainActionFuture.newFuture();
        remoteConnectionManager.connectToNode(node2, null, validator, future2);
        assertTrue(future2.isDone());

        assertEquals(node1, remoteConnectionManager.getConnection(node1).getNode());
        assertEquals(node2, remoteConnectionManager.getConnection(node2).getNode());

        DiscoveryNode node4 = new DiscoveryNode("node-4", address, Version.CURRENT);
        assertThat(remoteConnectionManager.getConnection(node4), instanceOf(RemoteConnectionManager.ProxyConnection.class));

        // Test round robin
        Set<Version> versions = new HashSet<>();
        versions.add(remoteConnectionManager.getConnection(node4).getVersion());
        versions.add(remoteConnectionManager.getConnection(node4).getVersion());

        assertThat(versions, hasItems(Version.CURRENT, Version.CURRENT.minimumCompatibilityVersion()));

        // Test that the connection is cleared from the round robin list when it is closed
        remoteConnectionManager.getConnection(node1).close();

        versions.clear();
        versions.add(remoteConnectionManager.getConnection(node4).getVersion());
        versions.add(remoteConnectionManager.getConnection(node4).getVersion());

        assertThat(versions, hasItems(Version.CURRENT.minimumCompatibilityVersion()));
        assertEquals(1, versions.size());
    }

    private static class TestRemoteConnection extends CloseableConnection {

        private final DiscoveryNode node;

        private TestRemoteConnection(DiscoveryNode node) {
            this.node = node;
        }

        @Override
        public DiscoveryNode getNode() {
            return node;
        }

        @Override
        public Version getVersion() {
            return node.getVersion();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {
        }
    }
}
