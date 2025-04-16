/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteConnectionManager.ProxyConnection;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresentWith;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemoteConnectionManagerTests extends ESTestCase {

    private Transport transport;
    private RemoteConnectionManager remoteConnectionManager;
    private ConnectionManager.ConnectionValidator validator = (connection, profile, listener) -> listener.onResponse(null);
    private TransportAddress address = new TransportAddress(InetAddress.getLoopbackAddress(), 1000);

    @SuppressWarnings("unchecked")
    @Override
    public void setUp() throws Exception {
        super.setUp();
        transport = mock(Transport.class);
        remoteConnectionManager = new RemoteConnectionManager(
            "remote-cluster",
            RemoteClusterCredentialsManager.EMPTY,
            new ClusterConnectionManager(Settings.EMPTY, transport, new ThreadContext(Settings.EMPTY))
        );

        doAnswer(invocationOnMock -> {
            ActionListener<Transport.Connection> listener = (ActionListener<Transport.Connection>) invocationOnMock.getArguments()[2];
            listener.onResponse(new TestRemoteConnection((DiscoveryNode) invocationOnMock.getArguments()[0]));
            return null;
        }).when(transport).openConnection(any(DiscoveryNode.class), any(ConnectionProfile.class), any(ActionListener.class));
    }

    public void testGetConnection() {
        DiscoveryNode node1 = DiscoveryNodeUtils.create("node-1", address);
        PlainActionFuture<Void> future1 = new PlainActionFuture<>();
        remoteConnectionManager.connectToRemoteClusterNode(node1, validator, future1);
        assertTrue(future1.isDone());

        // Add duplicate connect attempt to ensure that we do not get duplicate connections in the round robin
        remoteConnectionManager.connectToRemoteClusterNode(node1, validator, new PlainActionFuture<>());

        DiscoveryNode node2 = DiscoveryNodeUtils.create("node-2", address);
        PlainActionFuture<Void> future2 = new PlainActionFuture<>();
        remoteConnectionManager.connectToRemoteClusterNode(node2, validator, future2);
        assertTrue(future2.isDone());

        assertEquals(node1, remoteConnectionManager.getConnection(node1).getNode());
        assertEquals(node2, remoteConnectionManager.getConnection(node2).getNode());

        DiscoveryNode node4 = DiscoveryNodeUtils.create("node-4", address);
        assertThat(remoteConnectionManager.getConnection(node4), instanceOf(ProxyConnection.class));

        // Test round robin
        Set<String> proxyNodes = new HashSet<>();
        proxyNodes.add(((ProxyConnection) remoteConnectionManager.getConnection(node4)).getConnection().getNode().getId());
        proxyNodes.add(((ProxyConnection) remoteConnectionManager.getConnection(node4)).getConnection().getNode().getId());

        assertThat(proxyNodes, containsInAnyOrder("node-1", "node-2"));

        // Test that the connection is cleared from the round robin list when it is closed
        remoteConnectionManager.getConnection(node1).close();

        proxyNodes.clear();
        proxyNodes.add(((ProxyConnection) remoteConnectionManager.getConnection(node4)).getConnection().getNode().getId());
        proxyNodes.add(((ProxyConnection) remoteConnectionManager.getConnection(node4)).getConnection().getNode().getId());

        assertThat(proxyNodes, containsInAnyOrder("node-2"));

        assertWarnings(
            "The remote cluster connection to [remote-cluster] is using the certificate-based security model. "
                + "The certificate-based security model is deprecated and will be removed in a future major version. "
                + "Migrate the remote cluster from the certificate-based to the API key-based security model."
        );
    }

    public void testDisconnectedException() {
        assertEquals(
            "Unable to connect to [remote-cluster]",
            expectThrows(ConnectTransportException.class, remoteConnectionManager::getAnyRemoteConnection).getMessage()
        );

        assertEquals(
            "Unable to connect to [remote-cluster]",
            expectThrows(
                ConnectTransportException.class,
                () -> remoteConnectionManager.getConnection(DiscoveryNodeUtils.create("node-1", address))
            ).getMessage()
        );
    }

    public void testResolveRemoteClusterAlias() throws ExecutionException, InterruptedException {
        DiscoveryNode remoteNode1 = DiscoveryNodeUtils.create("remote-node-1", address);
        PlainActionFuture<Void> future = new PlainActionFuture<>();
        remoteConnectionManager.connectToRemoteClusterNode(remoteNode1, validator, future);
        assertTrue(future.isDone());

        Transport.Connection remoteConnection = remoteConnectionManager.getConnection(remoteNode1);
        final String remoteClusterAlias = "remote-cluster";
        assertThat(RemoteConnectionManager.resolveRemoteClusterAlias(remoteConnection).get(), equalTo(remoteClusterAlias));

        Transport.Connection localConnection = mock(Transport.Connection.class);
        assertThat(RemoteConnectionManager.resolveRemoteClusterAlias(localConnection).isPresent(), equalTo(false));

        DiscoveryNode remoteNode2 = DiscoveryNodeUtils.create("remote-node-2", address);
        Transport.Connection proxyConnection = remoteConnectionManager.getConnection(remoteNode2);
        assertThat(proxyConnection, instanceOf(ProxyConnection.class));
        assertThat(RemoteConnectionManager.resolveRemoteClusterAlias(proxyConnection).get(), equalTo(remoteClusterAlias));

        PlainActionFuture<Transport.Connection> future2 = new PlainActionFuture<>();
        remoteConnectionManager.openConnection(remoteNode1, null, future2);
        assertThat(RemoteConnectionManager.resolveRemoteClusterAlias(future2.get()).get(), equalTo(remoteClusterAlias));

        assertWarnings(
            "The remote cluster connection to ["
                + remoteClusterAlias
                + "] is using the certificate-based security model. "
                + "The certificate-based security model is deprecated and will be removed in a future major version. "
                + "Migrate the remote cluster from the certificate-based to the API key-based security model."
        );
    }

    public void testRewriteHandshakeAction() throws IOException {
        final Transport.Connection connection = mock(Transport.Connection.class);
        final String clusterAlias = randomAlphaOfLengthBetween(3, 8);
        final RemoteClusterCredentialsManager credentialsResolver = mock(RemoteClusterCredentialsManager.class);
        when(credentialsResolver.resolveCredentials(clusterAlias)).thenReturn(new SecureString(randomAlphaOfLength(42)));
        final Transport.Connection wrappedConnection = RemoteConnectionManager.wrapConnectionWithRemoteClusterInfo(
            connection,
            clusterAlias,
            credentialsResolver
        );
        final long requestId = randomLong();
        final TransportRequest request = mock(TransportRequest.class);
        final TransportRequestOptions options = mock(TransportRequestOptions.class);

        wrappedConnection.sendRequest(requestId, TransportService.HANDSHAKE_ACTION_NAME, request, options);
        verify(connection).sendRequest(requestId, REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME, request, options);

        final String anotherAction = randomValueOtherThan(
            TransportService.HANDSHAKE_ACTION_NAME,
            () -> randomFrom("cluster:", "indices:", "internal:", randomAlphaOfLengthBetween(3, 10) + ":") + Strings
                .collectionToDelimitedString(randomList(1, 5, () -> randomAlphaOfLengthBetween(3, 20)), "/")
        );
        Mockito.reset(connection);
        wrappedConnection.sendRequest(requestId, anotherAction, request, options);
        verify(connection).sendRequest(requestId, anotherAction, request, options);
    }

    public void testWrapAndResolveConnectionRoundTrip() {
        final Transport.Connection connection = mock(Transport.Connection.class);
        final String clusterAlias = randomAlphaOfLengthBetween(3, 8);
        final RemoteClusterCredentialsManager credentialsResolver = mock(RemoteClusterCredentialsManager.class);
        final SecureString credentials = new SecureString(randomAlphaOfLength(42));
        // second credential will never be resolved
        when(credentialsResolver.resolveCredentials(clusterAlias)).thenReturn(credentials, (SecureString) null);
        final Transport.Connection wrappedConnection = RemoteConnectionManager.wrapConnectionWithRemoteClusterInfo(
            connection,
            clusterAlias,
            credentialsResolver
        );

        final Optional<RemoteConnectionManager.RemoteClusterAliasWithCredentials> actual = RemoteConnectionManager
            .resolveRemoteClusterAliasWithCredentials(wrappedConnection);

        assertThat(actual, isPresentWith(new RemoteConnectionManager.RemoteClusterAliasWithCredentials(clusterAlias, credentials)));
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
        public TransportVersion getTransportVersion() {
            return TransportVersion.current();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws TransportException {}
    }
}
