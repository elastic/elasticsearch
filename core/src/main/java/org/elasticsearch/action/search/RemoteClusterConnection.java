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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;

final class RemoteClusterConnection extends AbstractComponent implements TransportConnectionListener {

    private final TransportService transportService;
    private final ConnectionProfile remoteProfile;
    private final CopyOnWriteArrayList<DiscoveryNode> clusterNodes = new CopyOnWriteArrayList();
    private final Supplier<DiscoveryNode> nodeSupplier;
    private final String clusterName;
    private volatile List<DiscoveryNode> seedNodes;
    private final ConnectHandler connectHandler;

    RemoteClusterConnection(Settings settings, String clusterName, List<DiscoveryNode> seedNodes,
                                   TransportService transportService) {
        super(settings);
        this.transportService = transportService;
        this.clusterName = clusterName;
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(6, TransportRequestOptions.Type.REG, TransportRequestOptions.Type.PING); // TODO make this configurable?
        builder.addConnections(0,  // we don't want this to be used for anything else but search
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.STATE,
            TransportRequestOptions.Type.RECOVERY);
        remoteProfile = builder.build();
        nodeSupplier = new Supplier<DiscoveryNode>() {
            private volatile Iterator<DiscoveryNode> current;
            @Override
            public DiscoveryNode get() {
                if (current == null || current.hasNext() == false) {
                    current = clusterNodes.iterator();
                    if (current.hasNext() == false) {
                        throw new IllegalStateException("No node available for cluster: " + clusterName + " nodes: " + clusterNodes );
                    }
                }
                return current.next();
            }
        };
        this.seedNodes = seedNodes;
        this.connectHandler = new ConnectHandler();
    }

    synchronized void updateSeedNodes(List<DiscoveryNode> seedNodes, ActionListener<Void> connectListener) {
        this.seedNodes = new ArrayList<>(seedNodes);
        connectHandler.handshake(connectListener);
    }

    @Override
    public void onNodeDisconnected(DiscoveryNode node) {
        boolean remove = clusterNodes.remove(node);
        if (remove == true && clusterNodes.isEmpty()) {
            // try to reconnect
            ActionListener<Void> listener = ActionListener.wrap(x -> {},
                e -> logger.error("failed to establish connection to remote cluster", e));
            connectHandler.handshake(listener);
        }
    }

    private void ensureConnected(DiscoveryNode[] nodes) {
        boolean seenNotConnectedNode = false;
        for (DiscoveryNode node : nodes) {
            if (transportService.nodeConnected(node) == false) {
                seenNotConnectedNode = true;
                transportService.connectToNode(node, remoteProfile);
            }
        }
        if (seenNotConnectedNode) {
            TimeValue connectTimeout = TimeValue.timeValueSeconds(10); // TODO make configurable
            connectHandler.handshakeAndConnect(clusterNodes.iterator(), transportService, connectTimeout,
                ActionListener.wrap((x) -> {
                }, x -> {
                }), false); // nocommit handle exceptions here what should we do
        }
    }

    public void fetchSearchShards(SearchRequest searchRequest, final List<String> indices,
                                  ActionListener<ClusterSearchShardsResponse> listener) {
        if (clusterNodes.isEmpty()) {
            connectHandler.handshake(ActionListener.wrap((x) -> fetchSearchShards(searchRequest, indices, listener), listener::onFailure));
        } else {
            fetchShardsInternal(searchRequest, indices, listener);
        }
    }

    private void fetchShardsInternal(SearchRequest searchRequest, List<String> indices, final ActionListener<ClusterSearchShardsResponse> listener) {
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
                    ensureConnected(clusterSearchShardsResponse.getNodes());
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

    public String getClusterName() {
        return clusterName;
    }


    private class ConnectHandler {
        private Semaphore running = new Semaphore(1);
        private BlockingQueue<ActionListener<Void>> queue = new ArrayBlockingQueue<>(100);

        public void handshake(ActionListener<Void> connectListener) {
            final boolean runConnect;
            final Collection<ActionListener<Void>> toNotify;
            synchronized (queue) {
                if (connectListener != null && queue.offer(connectListener) == false) {
                    throw new IllegalStateException("connect queue is full");
                }
                if (queue.isEmpty()) {
                    return;
                }
                runConnect = running.tryAcquire();
                if (runConnect) {
                    toNotify = new ArrayList<>();
                    queue.drainTo(toNotify);
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
                    for (ActionListener<Void> queuedListener : toNotify) {
                        queuedListener.onFailure(e);
                    }
                }

                @Override
                protected void doRun() throws Exception {
                    ActionListener<Void> listener = ActionListener.wrap((x) -> {

                            synchronized (queue) {
                                running.release();
                            }
                            for (ActionListener<Void> queuedListener : toNotify) {
                                queuedListener.onResponse(x);
                            }
                            handshake(null);
                        },
                        (e) -> {
                            synchronized (queue) {
                                running.release();
                            }
                            for (ActionListener<Void> queuedListener : toNotify) {
                                queuedListener.onFailure(e);
                            }
                            handshake(null);
                        });
                    TimeValue connectTimeout = TimeValue.timeValueSeconds(10); // TODO make configurable
                    Iterator<DiscoveryNode> iterator = Collections.synchronizedList(seedNodes).iterator();
                    handshakeAndConnect(iterator, transportService, connectTimeout, listener, true);
                }
            });

        }



        void handshakeAndConnect(Iterator<DiscoveryNode> seedNodes,
                                 final TransportService transportService, TimeValue connectTimeout, ActionListener<Void> listener,
                                 boolean connect) {
                try {
                    if (seedNodes.hasNext()) {
                        final DiscoveryNode seedNode = seedNodes.next();
                        final DiscoveryNode handshakeNode;
                        if (connect) {
                            try (Transport.Connection connection = transportService.openConnection(seedNode, ConnectionProfile.LIGHT_PROFILE)) {
                                handshakeNode = transportService.handshake(connection, connectTimeout.millis(), (c) -> true);
                                transportService.connectToNode(handshakeNode, remoteProfile);
                                clusterNodes.add(handshakeNode);
                            }
                        } else {
                            handshakeNode = seedNode;
                        }
                        ClusterStateRequest request = new ClusterStateRequest();
                        request.clear();
                        request.nodes(true);
                        transportService.sendRequest(transportService.getConnection(handshakeNode),
                            ClusterStateAction.NAME, request, TransportRequestOptions.EMPTY,
                            new TransportResponseHandler<ClusterStateResponse>() {

                                @Override
                                public ClusterStateResponse newInstance() {
                                    return new ClusterStateResponse();
                                }

                                @Override
                                public void handleResponse(ClusterStateResponse response) {
                                    DiscoveryNodes nodes = response.getState().nodes();
                                    Iterable<DiscoveryNode> nodesIter = nodes.getDataNodes()::valuesIt;
                                    for (DiscoveryNode node : nodesIter) {
                                        transportService.connectToNode(node); // noop if node is connected
                                        clusterNodes.add(node);
                                    }
                                    listener.onResponse(null);
                                }

                                @Override
                                public void handleException(TransportException exp) {
                                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("fetching nodes from external cluster {} failed",
                                        clusterName), exp);
                                    handshakeAndConnect(seedNodes, transportService, connectTimeout, listener, connect);
                                }

                                @Override
                                public String executor() {
                                    return ThreadPool.Names.MANAGEMENT;
                                }
                            });
                    } else {
                        listener.onFailure(new IllegalStateException("no seed node left"));
                    }
                } catch (IOException ex) {
                    if (seedNodes.hasNext()) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("fetching nodes from external cluster {} failed",
                            clusterName), ex);
                        handshakeAndConnect(seedNodes, transportService, connectTimeout, listener, connect);
                    } else {
                        listener.onFailure(ex);
                    }
                }
        }
    }

}
