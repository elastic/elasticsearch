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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
 * {@link RemoteClusterService#REMOTE_CONNECTIONS_PER_CLUSTER} until either all eligible nodes are exhausted or the maximum number of
 * connections per cluster has been reached.
 */
final class RemoteClusterConnection implements TransportConnectionListener, Closeable {

    private static final Logger logger = LogManager.getLogger(RemoteClusterConnection.class);

    private final TransportService transportService;
    private final ConnectionManager connectionManager;
    private final String clusterAlias;
    private final int maxNumRemoteConnections;
    private final Predicate<DiscoveryNode> nodePredicate;
    private final ThreadPool threadPool;
    private volatile String proxyAddress;
    private volatile List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes;
    private volatile boolean skipUnavailable;
    private final ConnectHandler connectHandler;
    private final TimeValue initialConnectionTimeout;
    private final SetOnce<ClusterName> remoteClusterName = new SetOnce<>();

    /**
     * Creates a new {@link RemoteClusterConnection}
     * @param settings the nodes settings object
     * @param clusterAlias the configured alias of the cluster to connect to
     * @param seedNodes a list of seed nodes to discover eligible nodes from
     * @param transportService the local nodes transport service
     * @param maxNumRemoteConnections the maximum number of connections to the remote cluster
     * @param nodePredicate a predicate to filter eligible remote nodes to connect to
     * @param proxyAddress the proxy address
     * @param connectionProfile the connection profile to use
     */
    RemoteClusterConnection(Settings settings, String clusterAlias, List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes,
                            TransportService transportService, int maxNumRemoteConnections, Predicate<DiscoveryNode> nodePredicate,
                            String proxyAddress, ConnectionProfile connectionProfile) {
        this(settings, clusterAlias, seedNodes, transportService, maxNumRemoteConnections, nodePredicate, proxyAddress,
            createConnectionManager(connectionProfile, transportService));
    }

    // Public for tests to pass a StubbableConnectionManager
    RemoteClusterConnection(Settings settings, String clusterAlias, List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes,
                            TransportService transportService, int maxNumRemoteConnections, Predicate<DiscoveryNode> nodePredicate,
                            String proxyAddress, ConnectionManager connectionManager) {
        this.transportService = transportService;
        this.maxNumRemoteConnections = maxNumRemoteConnections;
        this.nodePredicate = nodePredicate;
        this.clusterAlias = clusterAlias;
        this.connectionManager = connectionManager;
        this.seedNodes = Collections.unmodifiableList(seedNodes);
        this.skipUnavailable = RemoteClusterService.REMOTE_CLUSTER_SKIP_UNAVAILABLE
            .getConcreteSettingForNamespace(clusterAlias).get(settings);
        this.connectHandler = new ConnectHandler();
        this.threadPool = transportService.threadPool;
        connectionManager.addListener(this);
        // we register the transport service here as a listener to make sure we notify handlers on disconnect etc.
        connectionManager.addListener(transportService);
        this.proxyAddress = proxyAddress;
        initialConnectionTimeout = RemoteClusterService.REMOTE_INITIAL_CONNECTION_TIMEOUT_SETTING.get(settings);
    }


    private static DiscoveryNode maybeAddProxyAddress(String proxyAddress, DiscoveryNode node) {
        if (proxyAddress == null || proxyAddress.isEmpty()) {
            return node;
        } else {
            // resolve proxy address lazy here
            InetSocketAddress proxyInetAddress = RemoteClusterAware.parseSeedAddress(proxyAddress);
            return new DiscoveryNode(node.getName(), node.getId(), node.getEphemeralId(), node.getHostName(), node
                .getHostAddress(), new TransportAddress(proxyInetAddress), node.getAttributes(), node.getRoles(), node.getVersion());
        }
    }

    /**
     * Updates the list of seed nodes for this cluster connection
     */
    synchronized void updateSeedNodes(
            final String proxyAddress,
            final List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes,
            final ActionListener<Void> connectListener) {
        this.seedNodes = List.copyOf(seedNodes);
        this.proxyAddress = proxyAddress;
        connectHandler.connect(connectListener);
    }

    /**
     * Updates the skipUnavailable flag that can be dynamically set for each remote cluster
     */
    void updateSkipUnavailable(boolean skipUnavailable) {
        this.skipUnavailable = skipUnavailable;
    }

    /**
     * Returns whether this cluster is configured to be skipped when unavailable
     */
    boolean isSkipUnavailable() {
        return skipUnavailable;
    }

    @Override
    public void onNodeDisconnected(DiscoveryNode node) {
        if (connectionManager.size() < maxNumRemoteConnections) {
            // try to reconnect and fill up the slot of the disconnected node
            connectHandler.connect(ActionListener.wrap(
                ignore -> logger.trace("successfully connected after disconnect of {}", node),
                e -> logger.trace(() -> new ParameterizedMessage("failed to connect after disconnect of {}", node), e)));
        }
    }

    /**
     * Ensures that this cluster is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    void ensureConnected(ActionListener<Void> voidActionListener) {
        if (connectionManager.size() == 0) {
            connectHandler.connect(voidActionListener);
        } else {
            voidActionListener.onResponse(null);
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
                final DiscoveryNode node = getAnyConnectedNode();
                Transport.Connection connection = connectionManager.getConnection(node);
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

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
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
        if (connectionManager.nodeConnected(remoteClusterNode)) {
            return connectionManager.getConnection(remoteClusterNode);
        }
        DiscoveryNode discoveryNode = getAnyConnectedNode();
        Transport.Connection connection = connectionManager.getConnection(discoveryNode);
        return new ProxyConnection(connection, remoteClusterNode);
    }

    private Predicate<ClusterName> getRemoteClusterNamePredicate() {
        return
            new Predicate<ClusterName>() {
                @Override
                public boolean test(ClusterName c) {
                    return remoteClusterName.get() == null || c.equals(remoteClusterName.get());
                }

                @Override
                public String toString() {
                    return remoteClusterName.get() == null ? "any cluster name"
                        : "expected remote cluster name [" + remoteClusterName.get().value() + "]";
                }
            };
    }


    static final class ProxyConnection implements Transport.Connection {
        private final Transport.Connection proxyConnection;
        private final DiscoveryNode targetNode;

        private ProxyConnection(Transport.Connection proxyConnection, DiscoveryNode targetNode) {
            this.proxyConnection = proxyConnection;
            this.targetNode = targetNode;
        }

        @Override
        public DiscoveryNode getNode() {
            return targetNode;
        }

        @Override
        public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws IOException, TransportException {
            proxyConnection.sendRequest(requestId, TransportActionProxy.getProxyAction(action),
                    TransportActionProxy.wrapRequest(targetNode, request), options);
        }

        @Override
        public void close() {
            assert false: "proxy connections must not be closed";
        }

        @Override
        public void addCloseListener(ActionListener<Void> listener) {
            proxyConnection.addCloseListener(listener);
        }

        @Override
        public boolean isClosed() {
            return proxyConnection.isClosed();
        }

        @Override
        public Version getVersion() {
            return proxyConnection.getVersion();
        }
    }

    Transport.Connection getConnection() {
        return connectionManager.getConnection(getAnyConnectedNode());
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(connectHandler);
        connectionManager.closeNoBlock();
    }

    public boolean isClosed() {
        return connectHandler.isClosed();
    }

    public String getProxyAddress() {
        return proxyAddress;
    }

    public List<Tuple<String, Supplier<DiscoveryNode>>> getSeedNodes() {
        return seedNodes;
    }

    /**
     * The connect handler manages node discovery and the actual connect to the remote cluster.
     * There is at most one connect job running at any time. If such a connect job is triggered
     * while another job is running the provided listeners are queued and batched up until the current running job returns.
     *
     * The handler has a built-in queue that can hold up to 100 connect attempts and will reject requests once the queue is full.
     * In a scenario when a remote cluster becomes unavailable we will queue requests up but if we can't connect quick enough
     * we will just reject the connect trigger which will lead to failing searches.
     */
    private class ConnectHandler implements Closeable {
        private static final int MAX_LISTENERS = 100;
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final Object mutex = new Object();
        private List<ActionListener<Void>> listeners = new ArrayList<>();

        /**
         * Triggers a connect round unless there is one running already. If there is a connect round running, the listener will either
         * be queued or rejected and failed.
         */
        void connect(ActionListener<Void> connectListener) {
            boolean runConnect = false;
            final ActionListener<Void> listener =
                ContextPreservingActionListener.wrapPreservingContext(connectListener, threadPool.getThreadContext());
            boolean closed;
            synchronized (mutex) {
                closed = this.closed.get();
                if (closed) {
                    assert listeners.isEmpty();
                } else {
                    if (listeners.size() >= MAX_LISTENERS) {
                        assert listeners.size() == MAX_LISTENERS;
                        listener.onFailure(new RejectedExecutionException("connect queue is full"));
                        return;
                    } else {
                        listeners.add(listener);
                    }
                    runConnect = listeners.size() == 1;
                }
            }
            if (closed) {
                connectListener.onFailure(new AlreadyClosedException("connect handler is already closed"));
                return;
            }
            if (runConnect) {
                ExecutorService executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
                executor.submit(new AbstractRunnable() {
                    @Override
                    public void onFailure(Exception e) {
                        ActionListener.onFailure(getAndClearListeners(), e);
                    }

                    @Override
                    protected void doRun() {
                        collectRemoteNodes(seedNodes.stream().map(Tuple::v2).iterator(),
                            new ActionListener<>() {
                                @Override
                                public void onResponse(Void aVoid) {
                                    ActionListener.onResponse(getAndClearListeners(), aVoid);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    ActionListener.onFailure(getAndClearListeners(), e);
                                }
                            });
                    }
                });
            }
        }

        private List<ActionListener<Void>> getAndClearListeners() {
            final List<ActionListener<Void>> result;
            synchronized (mutex) {
                if (listeners.isEmpty()) {
                    result = Collections.emptyList();
                } else {
                    result = listeners;
                    listeners = new ArrayList<>();
                }
            }
            return result;
        }

        private void collectRemoteNodes(Iterator<Supplier<DiscoveryNode>> seedNodes, ActionListener<Void> listener) {
            if (Thread.currentThread().isInterrupted()) {
                listener.onFailure(new InterruptedException("remote connect thread got interrupted"));
            }

            if (seedNodes.hasNext()) {
                final Consumer<Exception> onFailure = e -> {
                    if (e instanceof ConnectTransportException ||
                        e instanceof IOException ||
                        e instanceof IllegalStateException) {
                        // ISE if we fail the handshake with an version incompatible node
                        if (seedNodes.hasNext()) {
                            logger.debug(() -> new ParameterizedMessage(
                                "fetching nodes from external cluster [{}] failed moving to next node", clusterAlias), e);
                            collectRemoteNodes(seedNodes, listener);
                            return;
                        }
                    }
                    logger.warn(() -> new ParameterizedMessage("fetching nodes from external cluster [{}] failed", clusterAlias), e);
                    listener.onFailure(e);
                };

                final DiscoveryNode seedNode = maybeAddProxyAddress(proxyAddress, seedNodes.next().get());
                logger.debug("[{}] opening connection to seed node: [{}] proxy address: [{}]", clusterAlias, seedNode,
                    proxyAddress);
                final ConnectionProfile profile = ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG);
                final StepListener<Transport.Connection> openConnectionStep = new StepListener<>();
                try {
                    connectionManager.openConnection(seedNode, profile, openConnectionStep);
                } catch (Exception e) {
                    onFailure.accept(e);
                }

                final StepListener<TransportService.HandshakeResponse> handShakeStep = new StepListener<>();
                openConnectionStep.whenComplete(connection -> {
                    ConnectionProfile connectionProfile = connectionManager.getConnectionProfile();
                    transportService.handshake(connection, connectionProfile.getHandshakeTimeout().millis(),
                        getRemoteClusterNamePredicate(), handShakeStep);
                }, onFailure);

                final StepListener<Void> fullConnectionStep = new StepListener<>();
                handShakeStep.whenComplete(handshakeResponse -> {
                    final DiscoveryNode handshakeNode = maybeAddProxyAddress(proxyAddress, handshakeResponse.getDiscoveryNode());

                    if (nodePredicate.test(handshakeNode) && connectionManager.size() < maxNumRemoteConnections) {
                        connectionManager.connectToNode(handshakeNode, null,
                            transportService.connectionValidator(handshakeNode), fullConnectionStep);
                    } else {
                        fullConnectionStep.onResponse(null);
                    }
                }, e -> {
                    final Transport.Connection connection = openConnectionStep.result();
                    logger.warn(new ParameterizedMessage("failed to connect to seed node [{}]", connection.getNode()), e);
                    IOUtils.closeWhileHandlingException(connection);
                    onFailure.accept(e);
                });

                fullConnectionStep.whenComplete(aVoid -> {
                    if (remoteClusterName.get() == null) {
                        TransportService.HandshakeResponse handshakeResponse = handShakeStep.result();
                        assert handshakeResponse.getClusterName().value() != null;
                        remoteClusterName.set(handshakeResponse.getClusterName());
                    }
                    final Transport.Connection connection = openConnectionStep.result();

                    ClusterStateRequest request = new ClusterStateRequest();
                    request.clear();
                    request.nodes(true);
                    // here we pass on the connection since we can only close it once the sendRequest returns otherwise
                    // due to the async nature (it will return before it's actually sent) this can cause the request to fail
                    // due to an already closed connection.
                    ThreadPool threadPool = transportService.getThreadPool();
                    ThreadContext threadContext = threadPool.getThreadContext();
                    TransportService.ContextRestoreResponseHandler<ClusterStateResponse> responseHandler = new TransportService
                        .ContextRestoreResponseHandler<>(threadContext.newRestorableContext(false),
                        new SniffClusterStateResponseHandler(connection, listener, seedNodes));
                    try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                        // we stash any context here since this is an internal execution and should not leak any
                        // existing context information.
                        threadContext.markAsSystemContext();
                        transportService.sendRequest(connection, ClusterStateAction.NAME, request, TransportRequestOptions.EMPTY,
                            responseHandler);
                    }
                }, e -> {
                    IOUtils.closeWhileHandlingException(openConnectionStep.result());
                    onFailure.accept(e);
                });
            } else {
                listener.onFailure(new IllegalStateException("no seed node left"));
            }
        }

        @Override
        public void close() throws IOException {
            final List<ActionListener<Void>> toNotify;
            synchronized (mutex) {
                if (closed.compareAndSet(false, true)) {
                    toNotify = listeners;
                    listeners = Collections.emptyList();
                } else {
                    toNotify = Collections.emptyList();
                }
            }
            ActionListener.onFailure(toNotify, new AlreadyClosedException("connect handler is already closed"));
        }

        final boolean isClosed() {
            return closed.get();
        }

        /* This class handles the _state response from the remote cluster when sniffing nodes to connect to */
        private class SniffClusterStateResponseHandler implements TransportResponseHandler<ClusterStateResponse> {

            private final Transport.Connection connection;
            private final ActionListener<Void> listener;
            private final Iterator<Supplier<DiscoveryNode>> seedNodes;

            SniffClusterStateResponseHandler(Transport.Connection connection, ActionListener<Void> listener,
                                             Iterator<Supplier<DiscoveryNode>> seedNodes) {
                this.connection = connection;
                this.listener = listener;
                this.seedNodes = seedNodes;
            }

            @Override
            public ClusterStateResponse read(StreamInput in) throws IOException {
                return new ClusterStateResponse(in);
            }

            @Override
            public void handleResponse(ClusterStateResponse response) {
                handleNodes(response.getState().nodes().getNodes().valuesIt());
            }

            private void handleNodes(Iterator<DiscoveryNode> nodesIter) {
                while (nodesIter.hasNext()) {
                    final DiscoveryNode node = maybeAddProxyAddress(proxyAddress, nodesIter.next());
                    if (nodePredicate.test(node) && connectionManager.size() < maxNumRemoteConnections) {
                        connectionManager.connectToNode(node, null,
                            transportService.connectionValidator(node), new ActionListener<>() {
                                @Override
                                public void onResponse(Void aVoid) {
                                    handleNodes(nodesIter);
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    if (e instanceof ConnectTransportException ||
                                        e instanceof IllegalStateException) {
                                        // ISE if we fail the handshake with an version incompatible node
                                        // fair enough we can't connect just move on
                                        logger.debug(() -> new ParameterizedMessage("failed to connect to node {}", node), e);
                                        handleNodes(nodesIter);
                                    } else {
                                        logger.warn(() ->
                                            new ParameterizedMessage("fetching nodes from external cluster {} failed", clusterAlias), e);
                                        IOUtils.closeWhileHandlingException(connection);
                                        collectRemoteNodes(seedNodes, listener);
                                    }
                                }
                            });
                        return;
                    }
                }
                // We have to close this connection before we notify listeners - this is mainly needed for test correctness
                // since if we do it afterwards we might fail assertions that check if all high level connections are closed.
                // from a code correctness perspective we could also close it afterwards.
                IOUtils.closeWhileHandlingException(connection);
                listener.onResponse(null);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.warn(() -> new ParameterizedMessage("fetching nodes from external cluster {} failed", clusterAlias), exp);
                try {
                    IOUtils.closeWhileHandlingException(connection);
                } finally {
                    // once the connection is closed lets try the next node
                    collectRemoteNodes(seedNodes, listener);
                }
            }

            @Override
            public String executor() {
                return ThreadPool.Names.MANAGEMENT;
            }
        }
    }

    boolean assertNoRunningConnections() { // for testing only
        synchronized (connectHandler.mutex) {
            assert connectHandler.listeners.isEmpty();
        }
        return true;
    }

    boolean isNodeConnected(final DiscoveryNode node) {
        return connectionManager.nodeConnected(node);
    }

    private final AtomicLong nextNodeId = new AtomicLong();

    DiscoveryNode getAnyConnectedNode() {
        List<DiscoveryNode> nodes = new ArrayList<>(connectionManager.connectedNodes());
        if (nodes.isEmpty()) {
            throw new NoSuchRemoteClusterException(clusterAlias);
        } else {
            long curr;
            while ((curr = nextNodeId.incrementAndGet()) == Long.MIN_VALUE);
            return nodes.get(Math.floorMod(curr, nodes.size()));
        }
    }

    /**
     * Get the information about remote nodes to be rendered on {@code _remote/info} requests.
     */
    public RemoteConnectionInfo getConnectionInfo() {
        return new RemoteConnectionInfo(
                clusterAlias,
                seedNodes.stream().map(Tuple::v1).collect(Collectors.toList()),
                maxNumRemoteConnections,
                getNumNodesConnected(),
                initialConnectionTimeout,
                skipUnavailable);
    }

    int getNumNodesConnected() {
        return connectionManager.size();
    }

    private static ConnectionManager createConnectionManager(ConnectionProfile connectionProfile, TransportService transportService) {
        return new ConnectionManager(connectionProfile, transportService.transport);
    }

    ConnectionManager getConnectionManager() {
        return connectionManager;
    }
}
