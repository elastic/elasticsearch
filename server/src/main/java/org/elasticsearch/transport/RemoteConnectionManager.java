package org.elasticsearch.transport;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;

import java.io.IOException;
import java.util.concurrent.CopyOnWriteArrayList;

public class RemoteConnectionManager {

    private final ConnectionManager connectionManager;
    private final CopyOnWriteArrayList<Transport.Connection> connections = new CopyOnWriteArrayList<>();

    RemoteConnectionManager(ConnectionManager connectionManager) {
        this.connectionManager = connectionManager;
    }

    public void connectToNode(DiscoveryNode node, ConnectionProfile connectionProfile,
                              ConnectionManager.ConnectionValidator connectionValidator,
                              ActionListener<Void> listener) throws ConnectTransportException {
        connectionManager.connectToNode(node, connectionProfile, connectionValidator, new ActionListener<>() {
            @Override
            public void onResponse(Void aVoid) {
                Transport.Connection newConnection = new ProxyConnection2(null, node);
                connections.add(newConnection);
                newConnection.addCloseListener(ActionListener.wrap(() -> connections.remove(newConnection)));
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    public Transport.Connection getConnection(DiscoveryNode node) {
        try {
            return connectionManager.getConnection(node);
        } catch (NodeNotConnectedException e) {
            return new ProxyConnection2(null, node);
        }
    }

    static final class ProxyConnection2 implements Transport.Connection {
        private final Transport.Connection connection;
        private final DiscoveryNode targetNode;

        private ProxyConnection2(Transport.Connection connection, DiscoveryNode targetNode) {
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
