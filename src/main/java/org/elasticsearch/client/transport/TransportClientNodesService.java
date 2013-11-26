/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoAction;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 *
 */
public class TransportClientNodesService extends AbstractComponent {

    private final TimeValue nodesSamplerInterval;

    private final long pingTimeout;

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Version version;

    // nodes that are added to be discovered
    private volatile ImmutableList<DiscoveryNode> listedNodes = ImmutableList.of();

    private final Object mutex = new Object();

    private volatile ImmutableList<DiscoveryNode> nodes = ImmutableList.of();

    private final AtomicInteger tempNodeIdGenerator = new AtomicInteger();

    private final NodeSampler nodesSampler;

    private volatile ScheduledFuture nodesSamplerFuture;

    private final AtomicInteger randomNodeGenerator = new AtomicInteger();

    private final boolean ignoreClusterName;

    private volatile boolean closed;

    @Inject
    public TransportClientNodesService(Settings settings, ClusterName clusterName, TransportService transportService, ThreadPool threadPool, Version version) {
        super(settings);
        this.clusterName = clusterName;
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.version = version;

        this.nodesSamplerInterval = componentSettings.getAsTime("nodes_sampler_interval", timeValueSeconds(5));
        this.pingTimeout = componentSettings.getAsTime("ping_timeout", timeValueSeconds(5)).millis();
        this.ignoreClusterName = componentSettings.getAsBoolean("ignore_cluster_name", false);

        if (logger.isDebugEnabled()) {
            logger.debug("node_sampler_interval[" + nodesSamplerInterval + "]");
        }

        if (componentSettings.getAsBoolean("sniff", false)) {
            this.nodesSampler = new SniffNodesSampler();
        } else {
            this.nodesSampler = new SimpleNodeSampler();
        }
        this.nodesSamplerFuture = threadPool.schedule(nodesSamplerInterval, ThreadPool.Names.GENERIC, new ScheduledNodeSampler());

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

    public ImmutableList<DiscoveryNode> listedNodes() {
        return this.listedNodes;
    }

    public TransportClientNodesService addTransportAddresses(TransportAddress... transportAddresses) {
        synchronized (mutex) {
            if (closed) {
                throw new ElasticSearchIllegalStateException("transport client is closed, can't add an address");
            }
            List<TransportAddress> filtered = Lists.newArrayListWithExpectedSize(transportAddresses.length);
            for (TransportAddress transportAddress : transportAddresses) {
                boolean found = false;
                for (DiscoveryNode otherNode : listedNodes) {
                    if (otherNode.address().equals(transportAddress)) {
                        found = true;
                        logger.debug("address [{}] already exists with [{}], ignoring...", transportAddress, otherNode);
                        break;
                    }
                }
                if (!found) {
                    filtered.add(transportAddress);
                }
            }
            if (filtered.isEmpty()) {
                return this;
            }
            ImmutableList.Builder<DiscoveryNode> builder = ImmutableList.builder();
            builder.addAll(listedNodes());
            for (TransportAddress transportAddress : filtered) {
                DiscoveryNode node = new DiscoveryNode("#transport#-" + tempNodeIdGenerator.incrementAndGet(), transportAddress, version);
                logger.debug("adding address [{}]", node);
                builder.add(node);
            }
            listedNodes = builder.build();
            nodesSampler.sample();
        }
        return this;
    }

    public TransportClientNodesService removeTransportAddress(TransportAddress transportAddress) {
        synchronized (mutex) {
            if (closed) {
                throw new ElasticSearchIllegalStateException("transport client is closed, can't remove an address");
            }
            ImmutableList.Builder<DiscoveryNode> builder = ImmutableList.builder();
            for (DiscoveryNode otherNode : listedNodes) {
                if (!otherNode.address().equals(transportAddress)) {
                    builder.add(otherNode);
                } else {
                    logger.debug("removing address [{}]", otherNode);
                }
            }
            listedNodes = builder.build();
            nodesSampler.sample();
        }
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

        @Override
        public void onResponse(Response response) {
            listener.onResponse(response);
        }

        @Override
        public void onFailure(Throwable e) {
            if (ExceptionsHelper.unwrapCause(e) instanceof ConnectTransportException) {
                int i = ++this.i;
                if (i >= nodes.size()) {
                    listener.onFailure(new NoNodeAvailableException());
                } else {
                    try {
                        callback.doWithNode(nodes.get((index + i) % nodes.size()), this);
                    } catch (Throwable e1) {
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
        synchronized (mutex) {
            if (closed) {
                return;
            }
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
    }

    abstract class NodeSampler {
        public void sample() {
            synchronized (mutex) {
                if (closed) {
                    return;
                }
                doSample();
            }
        }

        protected abstract void doSample();

        /**
         * validates a set of potentially newly discovered nodes and returns an immutable
         * list of the nodes that has passed.
         */
        protected ImmutableList<DiscoveryNode> validateNewNodes(Set<DiscoveryNode> nodes) {
            for (Iterator<DiscoveryNode> it = nodes.iterator(); it.hasNext(); ) {
                DiscoveryNode node = it.next();
                if (!transportService.nodeConnected(node)) {
                    try {
                        logger.trace("connecting to node [{}]", node);
                        transportService.connectToNode(node);
                    } catch (Throwable e) {
                        it.remove();
                        logger.debug("failed to connect to discovered node [" + node + "]", e);
                    }
                }
            }

            return new ImmutableList.Builder<DiscoveryNode>().addAll(nodes).build();
        }

    }

    class ScheduledNodeSampler implements Runnable {
        @Override
        public void run() {
            try {
                nodesSampler.sample();
                if (!closed) {
                    nodesSamplerFuture = threadPool.schedule(nodesSamplerInterval, ThreadPool.Names.GENERIC, this);
                }
            } catch (Exception e) {
                logger.warn("failed to sample", e);
            }
        }
    }

    class SimpleNodeSampler extends NodeSampler {

        @Override
        protected void doSample() {
            HashSet<DiscoveryNode> newNodes = new HashSet<DiscoveryNode>();
            for (DiscoveryNode listedNode : listedNodes) {
                if (!transportService.nodeConnected(listedNode)) {
                    try {
                        // its a listed node, light connect to it...
                        logger.trace("connecting to listed node (light) [{}]", listedNode);
                        transportService.connectToNodeLight(listedNode);
                    } catch (Throwable e) {
                        logger.debug("failed to connect to node [{}], removed from nodes list", e, listedNode);
                        continue;
                    }
                }
                try {
                    NodesInfoResponse nodeInfo = transportService.submitRequest(listedNode, NodesInfoAction.NAME,
                            Requests.nodesInfoRequest("_local"),
                            TransportRequestOptions.options().withType(TransportRequestOptions.Type.STATE).withTimeout(pingTimeout),
                            new FutureTransportResponseHandler<NodesInfoResponse>() {
                                @Override
                                public NodesInfoResponse newInstance() {
                                    return new NodesInfoResponse();
                                }
                            }).txGet();
                    if (!ignoreClusterName && !clusterName.equals(nodeInfo.getClusterName())) {
                        logger.warn("node {} not part of the cluster {}, ignoring...", listedNode, clusterName);
                    } else if (nodeInfo.getNodes().length != 0) {
                        // use discovered information but do keep the original transport address, so people can control which address
                        // is exactly used.

                        DiscoveryNode nodeWithInfo = nodeInfo.getNodes()[0].getNode();
                        newNodes.add(new DiscoveryNode(nodeWithInfo.name(), nodeWithInfo.id(), listedNode.address(), nodeWithInfo.attributes(), nodeWithInfo.version()));
                    } else {
                        // although we asked for one node, our target may not have completed initialization yet and doesn't have
                        // cluster nodes
                        logger.debug("node {} didn't return any discovery info, temporarily using transport discovery node", listedNode);
                        newNodes.add(listedNode);
                    }
                } catch (Exception e) {
                    logger.info("failed to get node info for {}, disconnecting...", e, listedNode);
                    transportService.disconnectFromNode(listedNode);
                }
            }

            nodes = validateNewNodes(newNodes);
        }
    }

    class SniffNodesSampler extends NodeSampler {

        @Override
        protected void doSample() {
            // the nodes we are going to ping include the core listed nodes that were added
            // and the last round of discovered nodes
            Set<DiscoveryNode> nodesToPing = Sets.newHashSet();
            for (DiscoveryNode node : listedNodes) {
                nodesToPing.add(node);
            }
            for (DiscoveryNode node : nodes) {
                nodesToPing.add(node);
            }

            final CountDownLatch latch = new CountDownLatch(nodesToPing.size());
            final Queue<ClusterStateResponse> clusterStateResponses = ConcurrentCollections.newQueue();
            for (final DiscoveryNode listedNode : nodesToPing) {
                threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (!transportService.nodeConnected(listedNode)) {
                                try {

                                    // if its one of hte actual nodes we will talk to, not to listed nodes, fully connect
                                    if (nodes.contains(listedNode)) {
                                        logger.trace("connecting to cluster node [{}]", listedNode);
                                        transportService.connectToNode(listedNode);
                                    } else {
                                        // its a listed node, light connect to it...
                                        logger.trace("connecting to listed node (light) [{}]", listedNode);
                                        transportService.connectToNodeLight(listedNode);
                                    }
                                } catch (Exception e) {
                                    logger.debug("failed to connect to node [{}], ignoring...", e, listedNode);
                                    latch.countDown();
                                    return;
                                }
                            }
                            transportService.sendRequest(listedNode, ClusterStateAction.NAME,
                                    Requests.clusterStateRequest()
                                            .filterAll().filterNodes(false).local(true),
                                    TransportRequestOptions.options().withType(TransportRequestOptions.Type.STATE).withTimeout(pingTimeout),
                                    new BaseTransportResponseHandler<ClusterStateResponse>() {

                                        @Override
                                        public ClusterStateResponse newInstance() {
                                            return new ClusterStateResponse();
                                        }

                                        @Override
                                        public String executor() {
                                            return ThreadPool.Names.SAME;
                                        }

                                        @Override
                                        public void handleResponse(ClusterStateResponse response) {
                                            clusterStateResponses.add(response);
                                            latch.countDown();
                                        }

                                        @Override
                                        public void handleException(TransportException e) {
                                            logger.info("failed to get local cluster state for {}, disconnecting...", e, listedNode);
                                            transportService.disconnectFromNode(listedNode);
                                            latch.countDown();
                                        }
                                    });
                        } catch (Exception e) {
                            logger.info("failed to get local cluster state info for {}, disconnecting...", e, listedNode);
                            transportService.disconnectFromNode(listedNode);
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

            HashSet<DiscoveryNode> newNodes = new HashSet<DiscoveryNode>(listedNodes);
            for (ClusterStateResponse clusterStateResponse : clusterStateResponses) {
                if (!ignoreClusterName && !clusterName.equals(clusterStateResponse.getClusterName())) {
                    logger.warn("node {} not part of the cluster {}, ignoring...", clusterStateResponse.getState().nodes().localNode(), clusterName);
                }
                for (ObjectCursor<DiscoveryNode> cursor : clusterStateResponse.getState().nodes().dataNodes().values()) {
                    newNodes.add(cursor.value);
                }
            }

            nodes = validateNewNodes(newNodes);
        }
    }

    public static interface NodeCallback<T> {

        T doWithNode(DiscoveryNode node) throws ElasticSearchException;
    }

    public static interface NodeListenerCallback<Response> {

        void doWithNode(DiscoveryNode node, ActionListener<Response> listener) throws ElasticSearchException;
    }
}
