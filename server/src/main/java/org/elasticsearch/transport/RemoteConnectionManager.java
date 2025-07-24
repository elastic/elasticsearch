/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.transport.RemoteClusterPortSettings.REMOTE_CLUSTER_PROFILE;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME;

public class RemoteConnectionManager implements ConnectionManager {

    private static final Logger logger = LogManager.getLogger(RemoteConnectionManager.class);

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RemoteConnectionManager.class);

    private final String clusterAlias;
    private final RemoteClusterCredentialsManager credentialsManager;
    private final ConnectionManager delegate;
    private final AtomicLong counter = new AtomicLong();
    private volatile List<DiscoveryNode> connectedNodes = Collections.emptyList();

    RemoteConnectionManager(String clusterAlias, RemoteClusterCredentialsManager credentialsManager, ConnectionManager delegate) {
        this.clusterAlias = clusterAlias;
        this.credentialsManager = credentialsManager;
        this.delegate = delegate;
        this.delegate.addListener(new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                addConnectedNode(node);
                try {
                    // called when a node is successfully connected through a proxy connection
                    maybeLogDeprecationWarning(wrapConnectionWithRemoteClusterInfo(connection, clusterAlias, credentialsManager));
                } catch (Exception e) {
                    logger.warn("Failed to log deprecation warning.", e);
                }
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, @Nullable Exception closeException) {
                removeConnectedNode(node);
            }
        });
    }

    public RemoteClusterCredentialsManager getCredentialsManager() {
        return credentialsManager;
    }

    /**
     * Remote cluster connections have a different lifecycle from intra-cluster connections. Use {@link #connectToRemoteClusterNode}
     * instead of this method.
     */
    @Override
    public final void connectToNode(
        DiscoveryNode node,
        ConnectionProfile connectionProfile,
        ConnectionValidator connectionValidator,
        ActionListener<Releasable> listener
    ) throws ConnectTransportException {
        // it's a mistake to call this expecting a useful Releasable back, we never release remote cluster connections today.
        assert false : "use connectToRemoteClusterNode instead";
        listener.onFailure(new UnsupportedOperationException("use connectToRemoteClusterNode instead"));
    }

    public void connectToRemoteClusterNode(DiscoveryNode node, ConnectionValidator connectionValidator, ActionListener<Void> listener)
        throws ConnectTransportException {
        delegate.connectToNode(node, null, connectionValidator, listener.map(connectionReleasable -> {
            // We drop the connectionReleasable here but it's not really a leak: we never close individual connections to a remote cluster
            // ourselves - instead we close the whole connection manager if the remote cluster is removed, which bypasses any refcounting
            // and just closes the underlying channels.
            return null;
        }));
    }

    @Override
    public void addListener(TransportConnectionListener listener) {
        delegate.addListener(listener);
    }

    @Override
    public void removeListener(TransportConnectionListener listener) {
        delegate.removeListener(listener);
    }

    @Override
    public void openConnection(DiscoveryNode node, @Nullable ConnectionProfile profile, ActionListener<Transport.Connection> listener) {
        assert profile == null || profile.getTransportProfile().equals(getConnectionProfile().getTransportProfile())
            : "A single remote connection manager can only ever handle a single transport profile";
        delegate.openConnection(
            node,
            profile,
            listener.delegateFailureAndWrap(
                (l, connection) -> l.onResponse(
                    maybeLogDeprecationWarning(wrapConnectionWithRemoteClusterInfo(connection, clusterAlias, credentialsManager))
                )
            )
        );
    }

    private InternalRemoteConnection maybeLogDeprecationWarning(InternalRemoteConnection connection) {
        if (connection.getClusterCredentials() == null
            && (false == REMOTE_CLUSTER_PROFILE.equals(this.getConnectionProfile().getTransportProfile()))) {
            deprecationLogger.warn(
                DeprecationCategory.SECURITY,
                "remote_cluster_certificate_access-" + connection.getClusterAlias(),
                "The remote cluster connection to [{}] is using the certificate-based security model. "
                    + "The certificate-based security model is deprecated and will be removed in a future major version. "
                    + "Migrate the remote cluster from the certificate-based to the API key-based security model.",
                connection.getClusterAlias()
            );
        }
        return connection;
    }

    @Override
    public Transport.Connection getConnection(DiscoveryNode node) {
        try {
            return getConnectionInternal(node);
        } catch (NodeNotConnectedException e) {
            return new ProxyConnection(getAnyRemoteConnection(), node);
        }
    }

    @Override
    public boolean nodeConnected(DiscoveryNode node) {
        return delegate.nodeConnected(node);
    }

    @Override
    public void disconnectFromNode(DiscoveryNode node) {
        delegate.disconnectFromNode(node);
    }

    @Override
    public ConnectionProfile getConnectionProfile() {
        return delegate.getConnectionProfile();
    }

    public Transport.Connection getAnyRemoteConnection() {
        List<DiscoveryNode> localConnectedNodes = this.connectedNodes;
        long curr;
        while ((curr = counter.incrementAndGet()) == Long.MIN_VALUE)
            ;
        if (localConnectedNodes.isEmpty() == false) {
            DiscoveryNode nextNode = localConnectedNodes.get(Math.floorMod(curr, localConnectedNodes.size()));
            try {
                return getConnectionInternal(nextNode);
            } catch (NodeNotConnectedException e) {
                // Ignore. We will manually create an iterator of open nodes
            }
        }
        Set<DiscoveryNode> allConnectionNodes = getAllConnectedNodes();
        for (DiscoveryNode connectedNode : allConnectionNodes) {
            try {
                return getConnectionInternal(connectedNode);
            } catch (NodeNotConnectedException e) {
                // Ignore. We will try the next one until all are exhausted.
            }
        }
        throw new ConnectTransportException(null, "Unable to connect to [" + clusterAlias + "]");
    }

    @Override
    public Set<DiscoveryNode> getAllConnectedNodes() {
        return delegate.getAllConnectedNodes();
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public void close() {
        delegate.closeNoBlock();
    }

    @Override
    public void closeNoBlock() {
        delegate.closeNoBlock();
    }

    /**
     * This method returns a remote cluster alias for the given transport connection if it targets a node in the remote cluster.
     * This method will return an optional empty in case the connection targets the local node or the node in the local cluster.
     *
     * @param connection the transport connection for which to resolve a remote cluster alias
     * @return a cluster alias if the connection target a node in the remote cluster, otherwise an empty result
     */
    public static Optional<String> resolveRemoteClusterAlias(Transport.Connection connection) {
        return resolveRemoteClusterAliasWithCredentials(connection).map(RemoteClusterAliasWithCredentials::clusterAlias);
    }

    public record RemoteClusterAliasWithCredentials(String clusterAlias, @Nullable SecureString credentials) {
        @Override
        public String toString() {
            return "RemoteClusterAliasWithCredentials{clusterAlias='" + clusterAlias + "', credentials='::es_redacted::'}";
        }
    }

    /**
     * This method returns information (alias and credentials) for remote cluster for the given transport connection.
     * Either or both of alias and credentials can be null depending on the connection.
     *
     * @param connection the transport connection for which to resolve a remote cluster alias
     */
    public static Optional<RemoteClusterAliasWithCredentials> resolveRemoteClusterAliasWithCredentials(Transport.Connection connection) {
        Transport.Connection unwrapped = TransportService.unwrapConnection(connection);
        if (unwrapped instanceof InternalRemoteConnection remoteConnection) {
            return Optional.of(
                new RemoteClusterAliasWithCredentials(remoteConnection.getClusterAlias(), remoteConnection.getClusterCredentials())
            );
        }
        return Optional.empty();
    }

    private Transport.Connection getConnectionInternal(DiscoveryNode node) throws NodeNotConnectedException {
        Transport.Connection connection = delegate.getConnection(node);
        return wrapConnectionWithRemoteClusterInfo(connection, clusterAlias, credentialsManager);
    }

    private synchronized void addConnectedNode(DiscoveryNode addedNode) {
        this.connectedNodes = CollectionUtils.appendToCopy(this.connectedNodes, addedNode);
    }

    private synchronized void removeConnectedNode(DiscoveryNode removedNode) {
        int newSize = this.connectedNodes.size() - 1;
        ArrayList<DiscoveryNode> newConnectedNodes = new ArrayList<>(newSize);
        for (DiscoveryNode connectedNode : this.connectedNodes) {
            if (connectedNode.equals(removedNode) == false) {
                newConnectedNodes.add(connectedNode);
            }
        }
        assert newConnectedNodes.size() == newSize : "Expected connection node count: " + newSize + ", Found: " + newConnectedNodes.size();
        this.connectedNodes = Collections.unmodifiableList(newConnectedNodes);
    }

    static final class ProxyConnection implements Transport.Connection {
        private final Transport.Connection connection;
        private final DiscoveryNode targetNode;

        private ProxyConnection(Transport.Connection connection, DiscoveryNode targetNode) {
            this.connection = connection;
            this.targetNode = targetNode;
        }

        @Override
        public DiscoveryNode getNode() {
            return targetNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            connection.sendRequest(
                requestId,
                TransportActionProxy.getProxyAction(action),
                TransportActionProxy.wrapRequest(targetNode, request),
                options
            );
        }

        @Override
        public void close() {
            assert false : "proxy connections must not be closed";
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            connection.addCloseListener(listener);
        }

        @Override
        public void addRemovedListener(ActionListener<Void> listener) {
            connection.addRemovedListener(listener);
        }

        @Override
        public boolean isClosed() {
            return connection.isClosed();
        }

        @Override
        public TransportVersion getTransportVersion() {
            return connection.getTransportVersion();
        }

        @Override
        public Object getCacheKey() {
            return connection.getCacheKey();
        }

        Transport.Connection getConnection() {
            return connection;
        }

        @Override
        public void incRef() {}

        @Override
        public boolean tryIncRef() {
            return true;
        }

        @Override
        public boolean decRef() {
            assert false : "proxy connections must not be released";
            return false;
        }

        @Override
        public boolean hasReferences() {
            return true;
        }

        @Override
        public void onRemoved() {}
    }

    private static final class InternalRemoteConnection implements Transport.Connection {

        private static final Logger logger = LogManager.getLogger(InternalRemoteConnection.class);
        private final Transport.Connection connection;
        private final String clusterAlias;
        @Nullable
        private final SecureString clusterCredentials;

        private InternalRemoteConnection(Transport.Connection connection, String clusterAlias, @Nullable SecureString clusterCredentials) {
            assert false == connection instanceof InternalRemoteConnection : "should not double wrap";
            assert false == connection instanceof ProxyConnection
                : "proxy connection should wrap internal remote connection, not the other way around";
            this.connection = Objects.requireNonNull(connection);
            this.clusterAlias = Objects.requireNonNull(clusterAlias);
            this.clusterCredentials = clusterCredentials;
        }

        public String getClusterAlias() {
            return clusterAlias;
        }

        @Nullable
        public SecureString getClusterCredentials() {
            return clusterCredentials;
        }

        @Override
        public DiscoveryNode getNode() {
            return connection.getNode();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            final String effectiveAction;
            if (clusterCredentials != null && TransportService.HANDSHAKE_ACTION_NAME.equals(action)) {
                logger.trace("sending remote cluster specific handshake to node [{}] of remote cluster [{}]", getNode(), clusterAlias);
                effectiveAction = REMOTE_CLUSTER_HANDSHAKE_ACTION_NAME;
            } else {
                effectiveAction = action;
            }
            connection.sendRequest(requestId, effectiveAction, request, options);
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            connection.addCloseListener(listener);
        }

        @Override
        public boolean isClosed() {
            return connection.isClosed();
        }

        @Override
        public TransportVersion getTransportVersion() {
            return connection.getTransportVersion();
        }

        @Override
        public Object getCacheKey() {
            return connection.getCacheKey();
        }

        @Override
        public void close() {
            connection.close();
        }

        @Override
        public void onRemoved() {
            connection.onRemoved();
        }

        @Override
        public void addRemovedListener(ActionListener<Void> listener) {
            connection.addRemovedListener(listener);
        }

        @Override
        public void incRef() {
            connection.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return connection.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return connection.decRef();
        }

        @Override
        public boolean hasReferences() {
            return connection.hasReferences();
        }
    }

    static InternalRemoteConnection wrapConnectionWithRemoteClusterInfo(
        Transport.Connection connection,
        String clusterAlias,
        RemoteClusterCredentialsManager credentialsManager
    ) {
        return new InternalRemoteConnection(connection, clusterAlias, credentialsManager.resolveCredentials(clusterAlias));
    }
}
