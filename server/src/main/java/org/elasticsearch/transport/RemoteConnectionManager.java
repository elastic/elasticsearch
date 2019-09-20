package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class RemoteConnectionManager implements Closeable {

    private final String clusterAlias;
    private final ConnectionManager connectionManager;
    private final AtomicLong counter = new AtomicLong();
    private volatile List<Transport.Connection> connections = Collections.emptyList();

    RemoteConnectionManager(String clusterAlias, ConnectionManager connectionManager) {
        this.clusterAlias = clusterAlias;
        this.connectionManager = connectionManager;
    }

    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                              ConnectionManager.ConnectionValidator connectionValidator,
                              ActionListener<Void> listener) throws ConnectTransportException {
        connectionManager.connectToNode(node, connectionProfile, connectionValidator, new ActionListener<>() {
            @Override
            public void onResponse(Void v) {
                try {
                    // TODO: Propagate the connection to the listener
                    Transport.Connection newConnection = connectionManager.getConnection(node);
                    addConnection(newConnection);
                    newConnection.addCloseListener(ActionListener.wrap(() -> removeConnection(newConnection)));
                    listener.onResponse(v);
                } catch (NodeNotConnectedException e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public boolean nodeConnected(DiscoveryNode node) {
        return connectionManager.nodeConnected(node);
    }

    public void openConnection(DiscoveryNode node, ConnectionProfile profile, ActionListener<Transport.Connection> listener) {
        connectionManager.openConnection(node, profile, listener);
    }

    public Transport.Connection getRemoteConnection(DiscoveryNode node) {
        try {
            return connectionManager.getConnection(node);
        } catch (NodeNotConnectedException e) {
            return new ProxyConnection(getAnyRemoteConnection(), node);
        }
    }

    public Transport.Connection getAnyRemoteConnection() {
        List<Transport.Connection> localConnections = this.connections;
        if (localConnections.isEmpty()) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        } else {
            long curr;
            while ((curr = counter.incrementAndGet()) == Long.MIN_VALUE);
            return localConnections.get(Math.floorMod(curr, localConnections.size()));
        }
    }

    public ConnectionManager getConnectionManager() {
        return connectionManager;
    }

    public int size() {
        return connectionManager.size();
    }

    public void close() {
        connectionManager.closeNoBlock();
    }

    private synchronized void addConnection(Transport.Connection addedConnection) {
        Set<Transport.Connection> newConnections = new HashSet<>(this.connections);
        newConnections.add(addedConnection);
        this.connections = List.copyOf(newConnections);
    }

    private synchronized void removeConnection(Transport.Connection removedConnection) {
        ArrayList<Transport.Connection> newConnections = new ArrayList<>(this.connections.size());
        for (Transport.Connection connection : this.connections) {
            if (connection.equals(removedConnection) == false) {
                newConnections.add(connection);
            }
        }
        this.connections = Collections.unmodifiableList(newConnections);
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
            connection.sendRequest(requestId, TransportActionProxy.getProxyAction(action),
                TransportActionProxy.wrapRequest(targetNode, request), options);
        }

        @Override
        public void close() {
            assert false: "proxy connections must not be closed";
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
        public Version getVersion() {
            return connection.getVersion();
        }
    }
}
