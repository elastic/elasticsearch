/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.transport;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Function;

/**
 * Represents a connection to a single remote cluster. In contrast to a local cluster a remote cluster is not joined such that the
 * current node is part of the cluster and it won't receive cluster state updates from the remote cluster. Remote clusters are also not
 * fully connected with the current node. From a connection perspective a local cluster forms a bi-directional star network while in the
 * remote case we only connect to a subset of the nodes in the cluster in an uni-directional fashion.
 *
 * This class also handles the discovery of nodes from the remote cluster. The initial list of seed nodes is only used to discover all nodes
 * in the remote cluster and connects to all eligible nodes, for details see {@link RemoteClusterService#REMOTE_NODE_ATTRIBUTE}.
 *
 * In the case of a disconnection, this class will issue a re-connect task to establish at most
 * {@link SniffConnectionStrategy#REMOTE_CONNECTIONS_PER_CLUSTER} until either all eligible nodes are exhausted or the maximum number of
 * connections per cluster has been reached.
 */
final class RemoteClusterConnection implements Closeable {

    private final TransportService transportService;
    private final RemoteConnectionManager remoteConnectionManager;
    private final RemoteConnectionStrategy connectionStrategy;
    private final String clusterAlias;
    private final ThreadPool threadPool;
    private volatile boolean skipUnavailable;
    private final TimeValue initialConnectionTimeout;

    /**
     * Creates a new {@link RemoteClusterConnection}
     * @param settings the nodes settings object
     * @param clusterAlias the configured alias of the cluster to connect to
     * @param transportService the local nodes transport service
     */
    RemoteClusterConnection(Settings settings, String clusterAlias, TransportService transportService) {
        this.transportService = transportService;
        this.clusterAlias = clusterAlias;
        ConnectionProfile profile = RemoteConnectionStrategy.buildConnectionProfile(clusterAlias, settings);
        this.remoteConnectionManager = new RemoteConnectionManager(clusterAlias, createConnectionManager(profile, transportService));
        this.connectionStrategy = RemoteConnectionStrategy.buildStrategy(clusterAlias, transportService, remoteConnectionManager, settings);
        // we register the transport service here as a listener to make sure we notify handlers on disconnect etc.
        this.remoteConnectionManager.addListener(transportService);
        this.skipUnavailable = RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE
            .getConcreteSettingForNamespace(clusterAlias).get(settings);
        this.threadPool = transportService.threadPool;
        initialConnectionTimeout = RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
    }

    /**
     * Updates the skipUnavailable flag that can be dynamically set for each remote cluster
     */
    void setSkipUnavailable(boolean skipUnavailable) {
        this.skipUnavailable = skipUnavailable;
    }

    /**
     * Returns whether this cluster is configured to be skipped when unavailable
     */
    boolean isSkipUnavailable() {
        return skipUnavailable;
    }

    /**
     * Ensures that this cluster is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    void ensureConnected(ActionListener<Void> listener) {
        if (remoteConnectionManager.size() == 0) {
            connectionStrategy.connect(listener);
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Collects all nodes on the connected cluster and returns / passes a nodeID to {@link DiscoveryNode} lookup function
     * that returns <code>null</code> if the node ID is not found.
     *
     * The requests to get cluster state on the connected cluster are made in the system context because logically
     * they are equivalent to checking a single detail in the local cluster state and should not require that the
     * user who made the request that is using this method in its implementation is authorized to view the entire
     * cluster state.
     */
    void collectNodes(ActionListener<Function<String, DiscoveryNode>> listener) {
        Runnable runnable = () -> {
            final ThreadContext threadContext = threadPool.getThreadContext();
            final ContextPreservingActionListener<Function<String, DiscoveryNode>> contextPreservingActionListener =
                new ContextPreservingActionListener<>(threadContext.newRestorableContext(false), listener);
            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                // we stash any context here since this is an internal execution and should not leak any existing context information
                threadContext.markAsSystemContext();

                final ClusterStateRequest request = new ClusterStateRequest();
                request.clear();
                request.nodes(true);
                request.local(true); // run this on the node that gets the request it's as good as any other
                Transport.Connection connection = remoteConnectionManager.getAnyRemoteConnection();
                transportService.sendRequest(connection, ClusterStateAction.NAME, request, TransportRequestOptions.EMPTY,
                    new TransportResponseHandler<ClusterStateResponse>() {

                        @Override
                        public ClusterStateResponse read(StreamInput in) throws IOException {
                            return new ClusterStateResponse(in);
                        }

                        @Override
                        public void handleResponse(ClusterStateResponse response) {
                            DiscoveryNodes nodes = response.getState().nodes();
                            contextPreservingActionListener.onResponse(nodes::get);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            contextPreservingActionListener.onFailure(exp);
                        }
                    });
            }
        };
        try {
            // just in case if we are not connected for some reason we try to connect and if we fail we have to notify the listener
            // this will cause some back pressure on the search end and eventually will cause rejections but that's fine
            // we can't proceed with a search on a cluster level.
            // in the future we might want to just skip the remote nodes in such a case but that can already be implemented on the
            // caller end since they provide the listener.
            ensureConnected(ActionListener.wrap((x) -> runnable.run(), listener::onFailure));
        } catch (Exception ex) {
            listener.onFailure(ex);
        }
    }

    /**
     * Returns a connection to the remote cluster, preferably a direct connection to the provided {@link DiscoveryNode}.
     * If such node is not connected, the returned connection will be a proxy connection that redirects to it.
     */
    Transport.Connection getConnection(DiscoveryNode remoteClusterNode) {
        return remoteConnectionManager.getConnection(remoteClusterNode);
    }

    Transport.Connection getConnection() {
        return remoteConnectionManager.getAnyRemoteConnection();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(connectionStrategy, remoteConnectionManager);
    }

    public boolean isClosed() {
        return connectionStrategy.isClosed();
    }

    // for testing only
    boolean assertNoRunningConnections() {
        return connectionStrategy.assertNoRunningConnections();
    }

    boolean isNodeConnected(final DiscoveryNode node) {
        return remoteConnectionManager.nodeConnected(node);
    }

    /**
     * Get the information about remote nodes to be rendered on {@code _remote/info} requests.
     */
    public RemoteConnectionInfo getConnectionInfo() {
        return new RemoteConnectionInfo(clusterAlias, connectionStrategy.getModeInfo(), initialConnectionTimeout, skipUnavailable);
    }

    int getNumNodesConnected() {
        return remoteConnectionManager.size();
    }

    private static ConnectionManager createConnectionManager(ConnectionProfile connectionProfile, TransportService transportService) {
        return new ClusterConnectionManager(connectionProfile, transportService.transport);
    }

    ConnectionManager getConnectionManager() {
        return remoteConnectionManager;
    }

    boolean shouldRebuildConnection(Settings newSettings) {
        return connectionStrategy.shouldRebuildConnection(newSettings);
    }
}
