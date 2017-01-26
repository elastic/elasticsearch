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
package org.elasticsearch.action.search;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsAction;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsRequest;
import org.elasticsearch.action.admin.cluster.shards.ClusterSearchShardsResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

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
final class RemoteClusterConnection extends AbstractComponent implements TransportConnectionListener, Closeable {

    private final TransportService transportService;
    private final ConnectionProfile remoteProfile;
    private final Set<DiscoveryNode> connectedNodes = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Supplier<DiscoveryNode> nodeSupplier;
    private final String clusterAlias;
    private final int maxNumRemoteConnections;
    private final Predicate<DiscoveryNode> nodePredicate;
    private volatile List<DiscoveryNode> seedNodes;
    private final ConnectHandler connectHandler;

    /**
     * Creates a new {@link RemoteClusterConnection}
     * @param settings the nodes settings object
     * @param clusterAlias the configured alias of the cluster to connect to
     * @param seedNodes a list of seed nodes to discover eligible nodes from
     * @param transportService the local nodes transport service
     * @param maxNumRemoteConnections the maximum number of connections to the remote cluster
     * @param nodePredicate a predicate to filter eligible remote nodes to connect to
     */
    RemoteClusterConnection(Settings settings, String clusterAlias, List<DiscoveryNode> seedNodes,
                            TransportService transportService, int maxNumRemoteConnections, Predicate<DiscoveryNode> nodePredicate) {
        super(settings);
        this.transportService = transportService;
        this.maxNumRemoteConnections = maxNumRemoteConnections;
        this.nodePredicate = nodePredicate;
        this.clusterAlias = clusterAlias;
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.setConnectTimeout(TcpTransport.TCP_CONNECT_TIMEOUT.get(settings));
        builder.setHandshakeTimeout(TcpTransport.TCP_CONNECT_TIMEOUT.get(settings));
        builder.addConnections(6, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING); // TODO make this configurable?
        builder.addConnections(0, // we don't want this to be used for anything else but search
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.STATE,
            TransportRequestOptions.Type.RECOVERY);
        remoteProfile = builder.build();
        nodeSupplier = new Supplier<DiscoveryNode>() {
            private volatile Iterator<DiscoveryNode> current;
            @Override
            public DiscoveryNode get() {
                if (current == null || current.hasNext() == false) {
                    current = connectedNodes.iterator();
                    if (current.hasNext() == false) {
                        throw new IllegalStateException("No node available for cluster: " + clusterAlias + " nodes: " + connectedNodes);
                    }
                }
                return current.next();
            }
        };
        this.seedNodes = Collections.unmodifiableList(seedNodes);
        this.connectHandler = new ConnectHandler();
        transportService.addConnectionListener(this);
    }

    /**
     * Updates the list of seed nodes for this cluster connection
     */
    synchronized void updateSeedNodes(List<DiscoveryNode> seedNodes, ActionListener<Void> connectListener) {
        this.seedNodes = Collections.unmodifiableList(new ArrayList<>(seedNodes));
        connectHandler.connect(connectListener);
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
    public void fetchSearchShards(SearchRequest searchRequest, final List<String> indices,
                                  ActionListener<ClusterSearchShardsResponse> listener) {
        if (connectedNodes.isEmpty()) {
            // just in case if we are not connected for some reason we try to connect and if we fail we have to notify the listener
            // this will cause some back pressure on the search end and eventually will cause rejections but that's fine
            // we can't proceed with a search on a cluster level.
            // in the future we might want to just skip the remote nodes in such a case but that can already be implemented on the caller
            // end since they provide the listener.
            connectHandler.connect(ActionListener.wrap((x) -> fetchShardsInternal(searchRequest, indices, listener), listener::onFailure));
        } else {
            fetchShardsInternal(searchRequest, indices, listener);
        }
    }

    private void fetchShardsInternal(SearchRequest searchRequest, List<String> indices,
                                     final ActionListener<ClusterSearchShardsResponse> listener) {
        final DiscoveryNode node = nodeSupplier.get();
        ClusterSearchShardsRequest searchShardsRequest = new ClusterSearchShardsRequest(indices.toArray(new String[indices.size()]))
            .indicesOptions(searchRequest.indicesOptions()).local(true).preference(searchRequest.preference())
            .routing(searchRequest.routing());
        transportService.sendRequest(node, ClusterSearchShardsAction.NAME, searchShardsRequest,
            new TransportResponseHandler<ClusterSearchShardsResponse>() {

                @Override
                public ClusterSearchShardsResponse newInstance() {
                    return new ClusterSearchShardsResponse();
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
     * Returns a connection to the remote cluster. This connection might be a proxy connection that redirects internally to the
     * given node.
     */
    Transport.Connection getConnection(DiscoveryNode remoteClusterNode) {
        DiscoveryNode discoveryNode = nodeSupplier.get();
        Transport.Connection connection = transportService.getConnection(discoveryNode);
        return new Transport.Connection() {
            @Override
            public DiscoveryNode getNode() {
                return remoteClusterNode;
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options)
                throws IOException, TransportException {
                connection.sendRequest(requestId, TransportActionProxy.getProxyAction(action),
                    TransportActionProxy.wrapRequest(remoteClusterNode, request), options);
            }

            @Override
            public void close() throws IOException {
                assert false: "proxy connections must not be closed";
            }
        };
    }

    @Override
    public void close() throws IOException {
        connectHandler.close();
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
            synchronized (queue) {
                if (connectListener != null && queue.offer(connectListener) == false) {
                    connectListener.onFailure(new RejectedExecutionException("connect queue is full"));
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
            ThreadPool threadPool = transportService.getThreadPool();
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
                protected void doRun() throws Exception {
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
                    collectRemoteNodes(seedNodes.iterator(), transportService, listener);
                }
            });

        }

        void collectRemoteNodes(Iterator<DiscoveryNode> seedNodes,
                                final TransportService transportService, ActionListener<Void> listener) {
            if (Thread.currentThread().isInterrupted()) {
                listener.onFailure(new InterruptedException("remote connect thread got interrupted"));
            }
            try {
                if (seedNodes.hasNext()) {
                    cancellableThreads.executeIO(() -> {
                        final DiscoveryNode seedNode = seedNodes.next();
                        final DiscoveryNode handshakeNode;
                        Transport.Connection connection = transportService.openConnection(seedNode,
                            ConnectionProfile.buildSingleChannelProfile(TransportRequestOptions.Type.REG, null, null));
                        boolean success = false;
                        try {
                            handshakeNode = transportService.handshake(connection, remoteProfile.getHandshakeTimeout().millis(),
                                (c) -> true);
                            if (nodePredicate.test(handshakeNode) && connectedNodes.size() < maxNumRemoteConnections) {
                                transportService.connectToNode(handshakeNode, remoteProfile);
                                connectedNodes.add(handshakeNode);
                            }
                            ClusterStateRequest request = new ClusterStateRequest();
                            request.clear();
                            request.nodes(true);
                            // here we pass on the connection since we can only close it once the sendRequest returns otherwise
                            // due to the async nature (it will return before it's actually sent) this can cause the request to fail
                            // due to an already closed connection.
                            transportService.sendRequest(connection,
                                ClusterStateAction.NAME, request, TransportRequestOptions.EMPTY,
                                new SniffClusterStateResponseHandler(transportService, connection, listener, seedNodes,
                                    cancellableThreads));
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
                listener.onFailure(ex); // we got canceled - fail the listener and step out
            } catch (ConnectTransportException | IOException | IllegalStateException ex) {
                // ISE if we fail the handshake with an version incompatible node
                if (seedNodes.hasNext()) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("fetching nodes from external cluster {} failed",
                        clusterAlias), ex);
                    collectRemoteNodes(seedNodes, transportService, listener);
                } else {
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

            private final TransportService transportService;
            private final Transport.Connection connection;
            private final ActionListener<Void> listener;
            private final Iterator<DiscoveryNode> seedNodes;
            private final CancellableThreads cancellableThreads;

            SniffClusterStateResponseHandler(TransportService transportService, Transport.Connection connection,
                                             ActionListener<Void> listener, Iterator<DiscoveryNode> seedNodes,
                                             CancellableThreads cancellableThreads) {
                this.transportService = transportService;
                this.connection = connection;
                this.listener = listener;
                this.seedNodes = seedNodes;
                this.cancellableThreads = cancellableThreads;
            }

            @Override
            public ClusterStateResponse newInstance() {
                return new ClusterStateResponse();
            }

            @Override
            public void handleResponse(ClusterStateResponse response) {
                try {
                    try (Closeable theConnection = connection) { // the connection is unused - see comment in #collectRemoteNodes
                        // we have to close this connection before we notify listeners - this is mainly needed for test correctness
                        // since if we do it afterwards we might fail assertions that check if all high level connections are closed.
                        // from a code correctness perspective we could also close it afterwards. This try/with block will
                        // maintain the possibly exceptions thrown from within the try block and suppress the ones that are possible thrown
                        // by closing the connection
                        cancellableThreads.executeIO(() -> {
                            DiscoveryNodes nodes = response.getState().nodes();
                            Iterable<DiscoveryNode> nodesIter = nodes.getNodes()::valuesIt;
                            for (DiscoveryNode node : nodesIter) {
                                if (nodePredicate.test(node) && connectedNodes.size() < maxNumRemoteConnections) {
                                    try {
                                        transportService.connectToNode(node, remoteProfile); // noop if node is connected
                                        connectedNodes.add(node);
                                    } catch (ConnectTransportException | IllegalStateException ex) {
                                        // ISE if we fail the handshake with an version incompatible node
                                        // fair enough we can't connect just move on
                                        logger.debug((Supplier<?>)
                                            () -> new ParameterizedMessage("failed to connect to node {}", node), ex);
                                    }
                                }
                            }
                        });
                    }
                    listener.onResponse(null);
                } catch (CancellableThreads.ExecutionCancelledException ex) {
                    listener.onFailure(ex); // we got canceled - fail the listener and step out
                } catch (Exception ex) {
                    logger.warn((Supplier<?>)
                        () -> new ParameterizedMessage("fetching nodes from external cluster {} failed",
                            clusterAlias), ex);
                    collectRemoteNodes(seedNodes, transportService, listener);
                }
            }

            @Override
            public void handleException(TransportException exp) {
                logger.warn((Supplier<?>)
                    () -> new ParameterizedMessage("fetching nodes from external cluster {} failed", clusterAlias),
                    exp);
                try {
                    IOUtils.closeWhileHandlingException(connection);
                } finally {
                    // once the connection is closed lets try the next node
                    collectRemoteNodes(seedNodes, transportService, listener);
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
}
