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
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
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
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_COMPRESS;
import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE;

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
    private final ConnectedNodes connectedNodes;
    private final String clusterAlias;
    private final int maxNumRemoteConnections;
    private final Predicate<DiscoveryNode> nodePredicate;
    private final ThreadPool threadPool;
    private volatile String proxyAddress;
    private volatile List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes;
    private volatile boolean skipUnavailable;
    private final ConnectHandler connectHandler;
    private final TimeValue initialConnectionTimeout;
    private SetOnce<ClusterName> remoteClusterName = new SetOnce<>();

    /**
     * Creates a new {@link RemoteClusterConnection}
     * @param settings the nodes settings object
     * @param clusterAlias the configured alias of the cluster to connect to
     * @param seedNodes a list of seed nodes to discover eligible nodes from
     * @param transportService the local nodes transport service
     * @param maxNumRemoteConnections the maximum number of connections to the remote cluster
     * @param nodePredicate a predicate to filter eligible remote nodes to connect to
     * @param proxyAddress the proxy address
     */
    RemoteClusterConnection(Settings settings, String clusterAlias, List<Tuple<String, Supplier<DiscoveryNode>>> seedNodes,
                            TransportService transportService, int maxNumRemoteConnections, Predicate<DiscoveryNode> nodePredicate,
                            String proxyAddress) {
        this(settings, clusterAlias, seedNodes, transportService, maxNumRemoteConnections, nodePredicate, proxyAddress,
            createConnectionManager(settings, clusterAlias, transportService));
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
        this.connectedNodes = new ConnectedNodes(clusterAlias);
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
            // resovle proxy address lazy here
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
        this.seedNodes = Collections.unmodifiableList(new ArrayList<>(seedNodes));
        this.proxyAddress = proxyAddress;
        connectHandler.connect(connectListener);
    }

    /**
     * Updates the skipUnavailable flag that can be dynamically set for each remote cluster
     */
    void updateSkipUnavailable(boolean skipUnavailable) {
        this.skipUnavailable = skipUnavailable;
    }

    @Override
    public void onNodeDisconnected(DiscoveryNode node) {
        boolean remove = connectedNodes.remove(node);
        if (remove && connectedNodes.size() < maxNumRemoteConnections) {
            // try to reconnect and fill up the slot of the disconnected node
            connectHandler.forceConnect();
        }
    }

    /**
     * Fetches all shards for the search request from this remote connection. This is used to later run the search on the remote end.
     */
    public void fetchSearchShards(ClusterSearchShardsRequest searchRequest,
                                  ActionListener<ClusterSearchShardsResponse> listener) {

        final ActionListener<ClusterSearchShardsResponse> searchShardsListener;
        final Consumer<Exception> onConnectFailure;
        if (skipUnavailable) {
            onConnectFailure = (exception) -> listener.onResponse(ClusterSearchShardsResponse.EMPTY);
            searchShardsListener = ActionListener.wrap(listener::onResponse, (e) -> listener.onResponse(ClusterSearchShardsResponse.EMPTY));
        } else {
            onConnectFailure = listener::onFailure;
            searchShardsListener = listener;
        }
        // in case we have no connected nodes we try to connect and if we fail we either notify the listener or not depending on
        // the skip_unavailable setting
        ensureConnected(ActionListener.wrap((x) -> fetchShardsInternal(searchRequest, searchShardsListener), onConnectFailure));
    }

    /**
     * Ensures that this cluster is connected. If the cluster is connected this operation
     * will invoke the listener immediately.
     */
    public void ensureConnected(ActionListener<Void> voidActionListener) {
        if (connectedNodes.size() == 0) {
            connectHandler.connect(voidActionListener);
        } else {
            voidActionListener.onResponse(null);
        }
    }

    private void fetchShardsInternal(ClusterSearchShardsRequest searchShardsRequest,
                                     final ActionListener<ClusterSearchShardsResponse> listener) {
        final DiscoveryNode node = getAnyConnectedNode();
        Transport.Connection connection = connectionManager.getConnection(node);
        transportService.sendRequest(connection, ClusterSearchShardsAction.NAME, searchShardsRequest, TransportRequestOptions.EMPTY,
            new TransportResponseHandler<ClusterSearchShardsResponse>() {

                @Override
                public ClusterSearchShardsResponse read(StreamInput in) throws IOException {
                    return new ClusterSearchShardsResponse(in);
                }

                @Override
                public void handleResponse(ClusterSearchShardsResponse clusterSearchShardsResponse) {
                    listener.onResponse(clusterSearchShardsResponse);
                }

                @Override
                public void handleException(TransportException e) {
                    listener.onFailure(e);
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SEARCH;
                }
            });
    }

    /**
     * Collects all nodes on the connected cluster and returns / passes a nodeID to {@link DiscoveryNode} lookup function
     * that returns <code>null</code> if the node ID is not found.
     */
    void collectNodes(ActionListener<Function<String, DiscoveryNode>> listener) {
        Runnable runnable = () -> {
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
                        ClusterStateResponse response = new ClusterStateResponse();
                        response.readFrom(in);
                        return response;
                    }

                    @Override
                    public void handleResponse(ClusterStateResponse response) {
                        DiscoveryNodes nodes = response.getState().nodes();
                        listener.onResponse(nodes::get);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
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
        IOUtils.close(connectHandler, connectionManager);
    }

    public boolean isClosed() {
        return connectHandler.isClosed();
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
        private final Semaphore running = new Semaphore(1);
        private final AtomicBoolean closed = new AtomicBoolean(false);
        private final BlockingQueue<ActionListener<Void>> queue = new ArrayBlockingQueue<>(100);
        private final CancellableThreads cancellableThreads = new CancellableThreads();

        /**
         * Triggers a connect round iff there are pending requests queued up and if there is no
         * connect round currently running.
         */
        void maybeConnect() {
            connect(null);
        }

        /**
         * Triggers a connect round unless there is one running already. If there is a connect round running, the listener will either
         * be queued or rejected and failed.
         */
        void connect(ActionListener<Void> connectListener) {
            connect(connectListener, false);
        }

        /**
         * Triggers a connect round unless there is one already running. In contrast to {@link #maybeConnect()} will this method also
         * trigger a connect round if there is no listener queued up.
         */
        void forceConnect() {
            connect(null, true);
        }

        private void connect(ActionListener<Void> connectListener, boolean forceRun) {
            final boolean runConnect;
            final Collection<ActionListener<Void>> toNotify;
            final ActionListener<Void> listener = connectListener == null ? null :
                ContextPreservingActionListener.wrapPreservingContext(connectListener, threadPool.getThreadContext());
            synchronized (queue) {
                if (listener != null && queue.offer(listener) == false) {
                    listener.onFailure(new RejectedExecutionException("connect queue is full"));
                    return;
                }
                if (forceRun == false && queue.isEmpty()) {
                    return;
                }
                runConnect = running.tryAcquire();
                if (runConnect) {
                    toNotify = new ArrayList<>();
                    queue.drainTo(toNotify);
                    if (closed.get()) {
                        running.release();
                        ActionListener.onFailure(toNotify, new AlreadyClosedException("connect handler is already closed"));
                        return;
                    }
                } else {
                    toNotify = Collections.emptyList();
                }
            }
            if (runConnect) {
                forkConnect(toNotify);
            }
        }

        private void forkConnect(final Collection<ActionListener<Void>> toNotify) {
            ExecutorService executor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
            executor.submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    synchronized (queue) {
                        running.release();
                    }
                    try {
                        ActionListener.onFailure(toNotify, e);
                    } finally {
                        maybeConnect();
                    }
                }

                @Override
                protected void doRun() {
                    ActionListener<Void> listener = ActionListener.wrap((x) -> {
                        synchronized (queue) {
                            running.release();
                        }
                        try {
                            ActionListener.onResponse(toNotify, x);
                        } finally {
                            maybeConnect();
                        }

                    }, (e) -> {
                        synchronized (queue) {
                            running.release();
                        }
                        try {
                            ActionListener.onFailure(toNotify, e);
                        } finally {
                            maybeConnect();
                        }
                    });
                    collectRemoteNodes(seedNodes.stream().map(Tuple::v2).iterator(), transportService, connectionManager, listener);
                }
            });
        }

        private void collectRemoteNodes(Iterator<Supplier<DiscoveryNode>> seedNodes, final TransportService transportService,
                                        final ConnectionManager manager, ActionListener<Void> listener) {
            if (Thread.currentThread().isInterrupted()) {
                listener.onFailure(new InterruptedException("remote connect thread got interrupted"));
            }
            try {
                if (seedNodes.hasNext()) {
                    cancellableThreads.executeIO(() -> {
                        final DiscoveryNode seedNode = maybeAddProxyAddress(proxyAddress, seedNodes.next().get());
                        logger.debug("[{}] opening connection to seed node: [{}] proxy address: [{}]", clusterAlias, seedNode,
                            proxyAddress);
                        final TransportService.HandshakeResponse handshakeResponse;
                        ConnectionProfile profile = ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG);
                        Transport.Connection connection = manager.openConnection(seedNode, profile);
                        boolean success = false;
                        try {
                            try {
                                ConnectionProfile connectionProfile = connectionManager.getConnectionProfile();
                                handshakeResponse = transportService.handshake(connection, connectionProfile.getHandshakeTimeout().millis(),
                                    (c) -> remoteClusterName.get() == null ? true : c.equals(remoteClusterName.get()));
                            } catch (IllegalStateException ex) {
                                logger.warn(() -> new ParameterizedMessage("seed node {} cluster name mismatch expected " +
                                    "cluster name {}", connection.getNode(), remoteClusterName.get()), ex);
                                throw ex;
                            }

                            final DiscoveryNode handshakeNode = maybeAddProxyAddress(proxyAddress, handshakeResponse.getDiscoveryNode());
                            if (nodePredicate.test(handshakeNode) && connectedNodes.size() < maxNumRemoteConnections) {
                                manager.connectToNode(handshakeNode, null, transportService.connectionValidator(handshakeNode));
                                if (remoteClusterName.get() == null) {
                                    assert handshakeResponse.getClusterName().value() != null;
                                    remoteClusterName.set(handshakeResponse.getClusterName());
                                }
                                connectedNodes.add(handshakeNode);
                            }
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
                                new SniffClusterStateResponseHandler(connection, listener, seedNodes,
                                    cancellableThreads));
                            try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
                                // we stash any context here since this is an internal execution and should not leak any
                                // existing context information.
                                threadContext.markAsSystemContext();
                                transportService.sendRequest(connection, ClusterStateAction.NAME, request, TransportRequestOptions.EMPTY,
                                    responseHandler);
                            }
                            success = true;
                        } finally {
                            if (success == false) {
                                connection.close();
                            }
                        }
                    });
                } else {
                    listener.onFailure(new IllegalStateException("no seed node left"));
                }
            } catch (CancellableThreads.ExecutionCancelledException ex) {
                logger.warn(() -> new ParameterizedMessage("fetching nodes from external cluster [{}] failed", clusterAlias), ex);
                listener.onFailure(ex); // we got canceled - fail the listener and step out
            } catch (ConnectTransportException | IOException | IllegalStateException ex) {
                // ISE if we fail the handshake with an version incompatible node
                if (seedNodes.hasNext()) {
                    logger.debug(() -> new ParameterizedMessage("fetching nodes from external cluster [{}] failed moving to next node",
                        clusterAlias), ex);
                    collectRemoteNodes(seedNodes, transportService, manager, listener);
                } else {
                    logger.warn(() -> new ParameterizedMessage("fetching nodes from external cluster [{}] failed", clusterAlias), ex);
                    listener.onFailure(ex);
                }
            }
        }

        @Override
        public void close() throws IOException {
            try {
                if (closed.compareAndSet(false, true)) {
                    cancellableThreads.cancel("connect handler is closed");
                    running.acquire(); // acquire the semaphore to ensure all connections are closed and all thread joined
                    running.release();
                    maybeConnect(); // now go and notify pending listeners
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        final boolean isClosed() {
            return closed.get();
        }

        /* This class handles the _state response from the remote cluster when sniffing nodes to connect to */
        private class SniffClusterStateResponseHandler implements TransportResponseHandler<ClusterStateResponse> {

            private final Transport.Connection connection;
            private final ActionListener<Void> listener;
            private final Iterator<Supplier<DiscoveryNode>> seedNodes;
            private final CancellableThreads cancellableThreads;

            SniffClusterStateResponseHandler(Transport.Connection connection, ActionListener<Void> listener,
                                             Iterator<Supplier<DiscoveryNode>> seedNodes,
                                             CancellableThreads cancellableThreads) {
                this.connection = connection;
                this.listener = listener;
                this.seedNodes = seedNodes;
                this.cancellableThreads = cancellableThreads;
            }

            @Override
            public ClusterStateResponse read(StreamInput in) throws IOException {
                ClusterStateResponse response = new ClusterStateResponse();
                response.readFrom(in);
                return response;
            }

            @Override
            public void handleResponse(ClusterStateResponse response) {
                try {
                    if (remoteClusterName.get() == null) {
                        assert response.getClusterName().value() != null;
                        remoteClusterName.set(response.getClusterName());
                    }
                    try (Closeable theConnection = connection) { // the connection is unused - see comment in #collectRemoteNodes
                        // we have to close this connection before we notify listeners - this is mainly needed for test correctness
                        // since if we do it afterwards we might fail assertions that check if all high level connections are closed.
                        // from a code correctness perspective we could also close it afterwards. This try/with block will
                        // maintain the possibly exceptions thrown from within the try block and suppress the ones that are possible thrown
                        // by closing the connection
                        cancellableThreads.executeIO(() -> {
                            DiscoveryNodes nodes = response.getState().nodes();
                            Iterable<DiscoveryNode> nodesIter = nodes.getNodes()::valuesIt;
                            for (DiscoveryNode n : nodesIter) {
                                DiscoveryNode node = maybeAddProxyAddress(proxyAddress, n);
                                if (nodePredicate.test(node) && connectedNodes.size() < maxNumRemoteConnections) {
                                    try {
                                        connectionManager.connectToNode(node, null,
                                            transportService.connectionValidator(node)); // noop if node is connected
                                        connectedNodes.add(node);
                                    } catch (ConnectTransportException | IllegalStateException ex) {
                                        // ISE if we fail the handshake with an version incompatible node
                                        // fair enough we can't connect just move on
                                        logger.debug(() -> new ParameterizedMessage("failed to connect to node {}", node), ex);
                                    }
                                }
                            }
                        });
                    }
                    listener.onResponse(null);
                } catch (CancellableThreads.ExecutionCancelledException ex) {
                    listener.onFailure(ex); // we got canceled - fail the listener and step out
                } catch (Exception ex) {
                    logger.warn(() -> new ParameterizedMessage("fetching nodes from external cluster {} failed", clusterAlias), ex);
                    collectRemoteNodes(seedNodes, transportService, connectionManager, listener);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                logger.warn(() -> new ParameterizedMessage("fetching nodes from external cluster {} failed", clusterAlias), exp);
                try {
                    IOUtils.closeWhileHandlingException(connection);
                } finally {
                    // once the connection is closed lets try the next node
                    collectRemoteNodes(seedNodes, transportService, connectionManager, listener);
                }
            }

            @Override
            public String executor() {
                return ThreadPool.Names.MANAGEMENT;
            }
        }
    }

    boolean assertNoRunningConnections() { // for testing only
        assert connectHandler.running.availablePermits() == 1;
        return true;
    }

    boolean isNodeConnected(final DiscoveryNode node) {
        return connectedNodes.contains(node);
    }

    DiscoveryNode getAnyConnectedNode() {
        return connectedNodes.getAny();
    }

    void addConnectedNode(DiscoveryNode node) {
        connectedNodes.add(node);
    }

    /**
     * Get the information about remote nodes to be rendered on {@code _remote/info} requests.
     */
    public RemoteConnectionInfo getConnectionInfo() {
        return new RemoteConnectionInfo(
                clusterAlias,
                seedNodes.stream().map(Tuple::v1).collect(Collectors.toList()),
                maxNumRemoteConnections,
                connectedNodes.size(),
                initialConnectionTimeout,
                skipUnavailable);
    }

    int getNumNodesConnected() {
        return connectedNodes.size();
    }

    private static final class ConnectedNodes {

        private final Set<DiscoveryNode> nodeSet = new HashSet<>();
        private final String clusterAlias;

        private Iterator<DiscoveryNode> currentIterator = null;

        private ConnectedNodes(String clusterAlias) {
            this.clusterAlias = clusterAlias;
        }

        public synchronized DiscoveryNode getAny() {
            ensureIteratorAvailable();
            if (currentIterator.hasNext()) {
                return currentIterator.next();
            } else {
                throw new IllegalStateException("No node available for cluster: " + clusterAlias);
            }
        }

        synchronized boolean remove(DiscoveryNode node) {
            final boolean setRemoval = nodeSet.remove(node);
            if (setRemoval) {
                currentIterator = null;
            }
            return setRemoval;
        }

        synchronized boolean add(DiscoveryNode node) {
            final boolean added = nodeSet.add(node);
            if (added) {
                currentIterator = null;
            }
            return added;
        }

        synchronized int size() {
            return nodeSet.size();
        }

        synchronized boolean contains(DiscoveryNode node) {
            return nodeSet.contains(node);
        }

        private synchronized void ensureIteratorAvailable() {
            if (currentIterator == null) {
                currentIterator = nodeSet.iterator();
            } else if (currentIterator.hasNext() == false && nodeSet.isEmpty() == false) {
                // iterator rollover
                currentIterator = nodeSet.iterator();
            }
        }
    }

    private static ConnectionManager createConnectionManager(Settings settings, String clusterAlias, TransportService transportService) {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder()
            .setConnectTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .setHandshakeTimeout(TransportSettings.CONNECT_TIMEOUT.get(settings))
            .addConnections(6, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING) // TODO make this configurable?
            // we don't want this to be used for anything else but search
            .addConnections(0, TransportRequestOptions.Type.BULK,
                TransportRequestOptions.Type.STATE,
                TransportRequestOptions.Type.RECOVERY)
            .setCompressionEnabled(REMOTE_CLUSTER_COMPRESS.getConcreteSettingForNamespace(clusterAlias).get(settings))
            .setPingInterval(REMOTE_CLUSTER_PING_SCHEDULE.getConcreteSettingForNamespace(clusterAlias).get(settings));
        return new ConnectionManager(builder.build(), transportService.transport, transportService.threadPool);
    }

    ConnectionManager getConnectionManager() {
        return connectionManager;
    }
}
