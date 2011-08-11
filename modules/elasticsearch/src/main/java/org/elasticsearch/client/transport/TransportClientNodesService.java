/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.client.transport;

import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TransportActions;
import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.unit.TimeValue.*;

/**
 * @author kimchy (shay.banon)
 */
public class TransportClientNodesService extends AbstractComponent {

    private final TimeValue nodesSamplerInterval;

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    // nodes that are added to be discovered
    private volatile ImmutableList<DiscoveryNode> listedNodes = ImmutableList.of();

    private final Object transportMutex = new Object();

    private volatile ImmutableList<DiscoveryNode> nodes = ImmutableList.of();

    private final AtomicInteger tempNodeIdGenerator = new AtomicInteger();

    private final NodeSampler nodesSampler;

    private volatile ScheduledFuture nodesSamplerFuture;

    private final AtomicInteger randomNodeGenerator = new AtomicInteger();

    private volatile boolean closed;

    @Inject public TransportClientNodesService(Settings settings, ClusterName clusterName,
                                               TransportService transportService, ThreadPool threadPool) {
        super(settings);
        this.clusterName = clusterName;
        this.transportService = transportService;
        this.threadPool = threadPool;

        this.nodesSamplerInterval = componentSettings.getAsTime("nodes_sampler_interval", timeValueSeconds(5));

        if (logger.isDebugEnabled()) {
            logger.debug("node_sampler_interval[" + nodesSamplerInterval + "]");
        }

        if (componentSettings.getAsBoolean("sniff", false)) {
            this.nodesSampler = new SniffNodesSampler();
        } else {
            this.nodesSampler = new SimpleNodeSampler();
        }
        this.nodesSamplerFuture = threadPool.schedule(nodesSamplerInterval, ThreadPool.Names.CACHED, new ScheduledNodeSampler());

        // we want the transport service to throw connect exceptions, so we can retry
        transportService.throwConnectException(true);
    }

    public ImmutableList<TransportAddress> transportAddresses() {
        ImmutableList.Builder<TransportAddress> lstBuilder = ImmutableList.builder();
        for (DiscoveryNode listedNode : listedNodes) {
            lstBuilder.add(listedNode.address());
        }
        return lstBuilder.build();
    }

    public ImmutableList<DiscoveryNode> connectedNodes() {
        return this.nodes;
    }

    public TransportClientNodesService addTransportAddress(TransportAddress transportAddress) {
        synchronized (transportMutex) {
            ImmutableList.Builder<DiscoveryNode> builder = ImmutableList.builder();
            listedNodes = builder.addAll(listedNodes).add(new DiscoveryNode("#transport#-" + tempNodeIdGenerator.incrementAndGet(), transportAddress)).build();
        }
        nodesSampler.sample();
        return this;
    }

    public TransportClientNodesService removeTransportAddress(TransportAddress transportAddress) {
        synchronized (transportMutex) {
            ImmutableList.Builder<DiscoveryNode> builder = ImmutableList.builder();
            for (DiscoveryNode otherNode : listedNodes) {
                if (!otherNode.address().equals(transportAddress)) {
                    builder.add(otherNode);
                }
            }
            listedNodes = builder.build();
        }
        nodesSampler.sample();
        return this;
    }

    public <T> T execute(NodeCallback<T> callback) throws ElasticSearchException {
        ImmutableList<DiscoveryNode> nodes = this.nodes;
        if (nodes.isEmpty()) {
            throw new NoNodeAvailableException();
        }
        int index = randomNodeGenerator.incrementAndGet();
        if (index < 0) {
            index = 0;
            randomNodeGenerator.set(0);
        }
        for (int i = 0; i < nodes.size(); i++) {
            DiscoveryNode node = nodes.get((index + i) % nodes.size());
            try {
                return callback.doWithNode(node);
            } catch (ElasticSearchException e) {
                if (!(e.unwrapCause() instanceof ConnectTransportException)) {
                    throw e;
                }
            }
        }
        throw new NoNodeAvailableException();
    }

    public <Response> void execute(NodeListenerCallback<Response> callback, ActionListener<Response> listener) throws ElasticSearchException {
        ImmutableList<DiscoveryNode> nodes = this.nodes;
        if (nodes.isEmpty()) {
            throw new NoNodeAvailableException();
        }
        int index = randomNodeGenerator.incrementAndGet();
        if (index < 0) {
            index = 0;
            randomNodeGenerator.set(0);
        }
        RetryListener<Response> retryListener = new RetryListener<Response>(callback, listener, nodes, index);
        try {
            callback.doWithNode(nodes.get((index) % nodes.size()), retryListener);
        } catch (ElasticSearchException e) {
            if (e.unwrapCause() instanceof ConnectTransportException) {
                retryListener.onFailure(e);
            } else {
                throw e;
            }
        }
    }

    public static class RetryListener<Response> implements ActionListener<Response> {
        private final NodeListenerCallback<Response> callback;
        private final ActionListener<Response> listener;
        private final ImmutableList<DiscoveryNode> nodes;
        private final int index;

        private volatile int i;

        public RetryListener(NodeListenerCallback<Response> callback, ActionListener<Response> listener, ImmutableList<DiscoveryNode> nodes, int index) {
            this.callback = callback;
            this.listener = listener;
            this.nodes = nodes;
            this.index = index;
        }

        @Override public void onResponse(Response response) {
            listener.onResponse(response);
        }

        @Override public void onFailure(Throwable e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof ConnectTransportException) {
                int i = ++this.i;
                if (i == nodes.size()) {
                    listener.onFailure(new NoNodeAvailableException());
                } else {
                    try {
                        callback.doWithNode(nodes.get((index + i) % nodes.size()), this);
                    } catch (Exception e1) {
                        // retry the next one...
                        onFailure(e);
                    }
                }
            } else {
                listener.onFailure(e);
            }
        }
    }

    public void close() {
        closed = true;
        nodesSamplerFuture.cancel(true);
        for (DiscoveryNode node : nodes) {
            transportService.disconnectFromNode(node);
        }
        for (DiscoveryNode listedNode : listedNodes) {
            transportService.disconnectFromNode(listedNode);
        }
        nodes = ImmutableList.of();
    }

    interface NodeSampler {
        void sample();
    }

    class ScheduledNodeSampler implements Runnable {
        @Override public void run() {
            nodesSampler.sample();
            if (!closed) {
                nodesSamplerFuture = threadPool.schedule(nodesSamplerInterval, ThreadPool.Names.CACHED, this);
            }
        }
    }

    class SimpleNodeSampler implements NodeSampler {

        @Override public synchronized void sample() {
            if (closed) {
                return;
            }
            HashSet<DiscoveryNode> newNodes = new HashSet<DiscoveryNode>();
            for (DiscoveryNode node : listedNodes) {
                if (!transportService.nodeConnected(node)) {
                    try {
                        transportService.connectToNode(node);
                    } catch (Exception e) {
                        logger.debug("Failed to connect to node " + node + ", removed from nodes list", e);
                        continue;
                    }
                }
                try {
                    NodesInfoResponse nodeInfo = transportService.submitRequest(node, TransportActions.Admin.Cluster.Node.INFO, Requests.nodesInfoRequest("_local"), new FutureTransportResponseHandler<NodesInfoResponse>() {
                        @Override public NodesInfoResponse newInstance() {
                            return new NodesInfoResponse();
                        }
                    }).txGet();
                    if (!clusterName.equals(nodeInfo.clusterName())) {
                        logger.warn("Node {} not part of the cluster {}, ignoring...", node, clusterName);
                    } else {
                        newNodes.add(node);
                    }
                } catch (Exception e) {
                    logger.warn("failed to get node info for {}", e, node);
                }
            }
            nodes = new ImmutableList.Builder<DiscoveryNode>().addAll(newNodes).build();
        }
    }

    class SniffNodesSampler implements NodeSampler {

        @Override public synchronized void sample() {
            if (closed) {
                return;
            }
            ImmutableList<DiscoveryNode> listedNodes = TransportClientNodesService.this.listedNodes;
            final CountDownLatch latch = new CountDownLatch(listedNodes.size());
            final CopyOnWriteArrayList<NodesInfoResponse> nodesInfoResponses = new CopyOnWriteArrayList<NodesInfoResponse>();
            for (final DiscoveryNode listedNode : listedNodes) {
                threadPool.cached().execute(new Runnable() {
                    @Override public void run() {
                        try {
                            transportService.connectToNode(listedNode); // make sure we are connected to it
                            transportService.sendRequest(listedNode, TransportActions.Admin.Cluster.Node.INFO, Requests.nodesInfoRequest("_all"), new BaseTransportResponseHandler<NodesInfoResponse>() {

                                @Override public NodesInfoResponse newInstance() {
                                    return new NodesInfoResponse();
                                }

                                @Override public String executor() {
                                    return ThreadPool.Names.SAME;
                                }

                                @Override public void handleResponse(NodesInfoResponse response) {
                                    nodesInfoResponses.add(response);
                                    latch.countDown();
                                }

                                @Override public void handleException(TransportException exp) {
                                    logger.debug("Failed to get node info from " + listedNode + ", removed from nodes list", exp);
                                    latch.countDown();
                                }
                            });
                        } catch (Exception e) {
                            logger.debug("Failed to get node info from " + listedNode + ", removed from nodes list", e);
                            latch.countDown();
                        }
                    }
                });
            }

            try {
                latch.await();
            } catch (InterruptedException e) {
                return;
            }

            HashSet<DiscoveryNode> newNodes = new HashSet<DiscoveryNode>();
            for (NodesInfoResponse nodesInfoResponse : nodesInfoResponses) {
                for (NodeInfo nodeInfo : nodesInfoResponse) {
                    if (!clusterName.equals(nodesInfoResponse.clusterName())) {
                        logger.warn("Node {} not part of the cluster {}, ignoring...", nodeInfo.node(), clusterName);
                    } else {
                        if (nodeInfo.node().dataNode()) { // only add data nodes to connect to
                            newNodes.add(nodeInfo.node());
                        }
                    }
                }
            }
            // now, make sure we are connected to all the updated nodes
            for (Iterator<DiscoveryNode> it = newNodes.iterator(); it.hasNext(); ) {
                DiscoveryNode node = it.next();
                try {
                    transportService.connectToNode(node);
                } catch (Exception e) {
                    it.remove();
                    logger.debug("Failed to connect to discovered node [" + node + "]", e);
                }
            }
            nodes = new ImmutableList.Builder<DiscoveryNode>().addAll(newNodes).build();
        }
    }

    public static interface NodeCallback<T> {

        T doWithNode(DiscoveryNode node) throws ElasticSearchException;
    }

    public static interface NodeListenerCallback<Response> {

        void doWithNode(DiscoveryNode node, ActionListener<Response> listener) throws ElasticSearchException;
    }
}
