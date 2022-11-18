/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Releasable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteConnectionManager implements ConnectionManager {

    private final String clusterAlias;
    private final ConnectionManager delegate;
    private final AtomicLong counter = new AtomicLong();
    private volatile List<DiscoveryNode> connectedNodes = Collections.emptyList();

    RemoteConnectionManager(String clusterAlias, ConnectionManager delegate) {
        this.clusterAlias = clusterAlias;
        this.delegate = delegate;
        this.delegate.addListener(new TransportConnectionListener() {
            @Override
            public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                addConnectedNode(node);
            }

            @Override
            public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                removeConnectedNode(node);
            }
        });
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
    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener) {
        delegate.openConnection(
            node,
            profile,
            listener.delegateFailure((l, connection) -> l.onResponse(wrapRemoteConnectionWithClusterAlias(connection)))
        );
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
        throw new NoSuchRemoteClusterException(clusterAlias);
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
        public Version getVersion() {
            return connection.getVersion();
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

    private Transport.Connection getConnectionInternal(DiscoveryNode node) throws NodeNotConnectedException {
        Transport.Connection connection = delegate.getConnection(node);
        return wrapRemoteConnectionWithClusterAlias(connection);
    }

    private Transport.Connection wrapRemoteConnectionWithClusterAlias(Transport.Connection connection) {
        return new RemoteClusterAliasAwareConnection(clusterAlias, connection);
    }

    public static final class RemoteClusterAliasAwareConnection implements Transport.Connection {

        private final String clusterAlias;
        private final Transport.Connection wrapped;

        public RemoteClusterAliasAwareConnection(String clusterAlias, Transport.Connection wrapped) {
            this.clusterAlias = clusterAlias;
            this.wrapped = wrapped;
        }

        public Transport.Connection unwrap() {
            return wrapped;
        }

        public String getClusterAlias() {
            return clusterAlias;
        }

        @Override
        public DiscoveryNode getNode() {
            return wrapped.getNode();
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
            throws IOException, TransportException {
            wrapped.sendRequest(requestId, action, request, options);
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            wrapped.addCloseListener(listener);
        }

        @Override
        public boolean isClosed() {
            return wrapped.isClosed();
        }

        @Override
        public Version getVersion() {
            return wrapped.getVersion();
        }

        @Override
        public Object getCacheKey() {
            return wrapped.getCacheKey();
        }

        @Override
        public void close() {
            wrapped.close();
        }

        @Override
        public void onRemoved() {
            wrapped.onRemoved();
        }

        @Override
        public void addRemovedListener(ActionListener<Void> listener) {
            wrapped.addRemovedListener(listener);
        }

        @Override
        public void incRef() {
            wrapped.incRef();
        }

        @Override
        public boolean tryIncRef() {
            return wrapped.tryIncRef();
        }

        @Override
        public boolean decRef() {
            return wrapped.decRef();
        }

        @Override
        public boolean hasReferences() {
            return wrapped.hasReferences();
        }
    }
}
