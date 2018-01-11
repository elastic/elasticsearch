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

package org.elasticsearch.client.transport;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessRequest;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessResponse;
import org.elasticsearch.action.admin.cluster.node.liveness.TransportLivenessAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ConnectionProfile;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.PlainTransportFuture;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;

final class TransportClientNodesService extends AbstractComponent implements Closeable {

    private final TimeValue nodesSamplerInterval;

    private final long pingTimeout;

    private final ClusterName clusterName;

    private final TransportService transportService;

    private final ThreadPool threadPool;

    private final Version minCompatibilityVersion;

    // nodes that are added to be discovered
    private volatile List<DiscoveryNode> listedNodes = Collections.emptyList();

    private final Object mutex = new Object();

    private volatile List<DiscoveryNode> nodes = Collections.emptyList();
    private volatile List<DiscoveryNode> filteredNodes = Collections.emptyList();

    private final AtomicInteger tempNodeIdGenerator = new AtomicInteger();

    private final NodeSampler nodesSampler;

    private volatile ScheduledFuture nodesSamplerFuture;

    private final AtomicInteger randomNodeGenerator = new AtomicInteger(Randomness.get().nextInt());

    private final boolean ignoreClusterName;

    private volatile boolean closed;

    private final TransportClient.HostFailureListener hostFailureListener;

    // TODO: migrate this to use low level connections and single type channels
    /** {@link ConnectionProfile} to use when to connecting to the listed nodes and doing a liveness check */
    private static final ConnectionProfile LISTED_NODES_PROFILE;

    static {
        ConnectionProfile.Builder builder = new ConnectionProfile.Builder();
        builder.addConnections(1,
            TransportRequestOptions.Type.BULK,
            TransportRequestOptions.Type.PING,
            TransportRequestOptions.Type.RECOVERY,
            TransportRequestOptions.Type.REG,
            TransportRequestOptions.Type.STATE);
        LISTED_NODES_PROFILE = builder.build();
    }

    TransportClientNodesService(Settings settings, TransportService transportService,
                                       ThreadPool threadPool, TransportClient.HostFailureListener hostFailureListener) {
        super(settings);
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.minCompatibilityVersion = Version.CURRENT.minimumCompatibilityVersion();

        this.nodesSamplerInterval = TransportClient.CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL.get(this.settings);
        this.pingTimeout = TransportClient.CLIENT_TRANSPORT_PING_TIMEOUT.get(this.settings).millis();
        this.ignoreClusterName = TransportClient.CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME.get(this.settings);

        if (logger.isDebugEnabled()) {
            logger.debug("node_sampler_interval[{}]", nodesSamplerInterval);
        }

        if (TransportClient.CLIENT_TRANSPORT_SNIFF.get(this.settings)) {
            this.nodesSampler = new SniffNodesSampler();
        } else {
            this.nodesSampler = new SimpleNodeSampler();
        }
        this.hostFailureListener = hostFailureListener;
        this.nodesSamplerFuture = threadPool.schedule(nodesSamplerInterval, ThreadPool.Names.GENERIC, new ScheduledNodeSampler());
    }

    public List<TransportAddress> transportAddresses() {
        List<TransportAddress> lstBuilder = new ArrayList<>();
        for (DiscoveryNode listedNode : listedNodes) {
            lstBuilder.add(listedNode.getAddress());
        }
        return Collections.unmodifiableList(lstBuilder);
    }

    public List<DiscoveryNode> connectedNodes() {
        return this.nodes;
    }

    public List<DiscoveryNode> filteredNodes() {
        return this.filteredNodes;
    }

    public List<DiscoveryNode> listedNodes() {
        return this.listedNodes;
    }

    public TransportClientNodesService addTransportAddresses(TransportAddress... transportAddresses) {
        synchronized (mutex) {
            if (closed) {
                throw new IllegalStateException("transport client is closed, can't add an address");
            }
            List<TransportAddress> filtered = new ArrayList<>(transportAddresses.length);
            for (TransportAddress transportAddress : transportAddresses) {
                boolean found = false;
                for (DiscoveryNode otherNode : listedNodes) {
                    if (otherNode.getAddress().equals(transportAddress)) {
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
            List<DiscoveryNode> builder = new ArrayList<>(listedNodes);
            for (TransportAddress transportAddress : filtered) {
                DiscoveryNode node = new DiscoveryNode("#transport#-" + tempNodeIdGenerator.incrementAndGet(),
                        transportAddress, Collections.emptyMap(), Collections.emptySet(), minCompatibilityVersion);
                logger.debug("adding address [{}]", node);
                builder.add(node);
            }
            listedNodes = Collections.unmodifiableList(builder);
            nodesSampler.sample();
        }
        return this;
    }

    public TransportClientNodesService removeTransportAddress(TransportAddress transportAddress) {
        synchronized (mutex) {
            if (closed) {
                throw new IllegalStateException("transport client is closed, can't remove an address");
            }
            List<DiscoveryNode> listNodesBuilder = new ArrayList<>();
            for (DiscoveryNode otherNode : listedNodes) {
                if (!otherNode.getAddress().equals(transportAddress)) {
                    listNodesBuilder.add(otherNode);
                } else {
                    logger.debug("removing address [{}] from listed nodes", otherNode);
                }
            }
            listedNodes = Collections.unmodifiableList(listNodesBuilder);
            List<DiscoveryNode> nodesBuilder = new ArrayList<>();
            for (DiscoveryNode otherNode : nodes) {
                if (!otherNode.getAddress().equals(transportAddress)) {
                    nodesBuilder.add(otherNode);
                } else {
                    logger.debug("disconnecting from node with address [{}]", otherNode);
                    transportService.disconnectFromNode(otherNode);
                }
            }
            nodes = Collections.unmodifiableList(nodesBuilder);
            nodesSampler.sample();
        }
        return this;
    }

    public <Response> void execute(NodeListenerCallback<Response> callback, ActionListener<Response> listener) {
        // we first read nodes before checking the closed state; this
        // is because otherwise we could be subject to a race where we
        // read the state as not being closed, and then the client is
        // closed and the nodes list is cleared, and then a
        // NoNodeAvailableException is thrown
        // it is important that the order of first setting the state of
        // closed and then clearing the list of nodes is maintained in
        // the close method
        final List<DiscoveryNode> nodes = this.nodes;
        if (closed) {
            throw new IllegalStateException("transport client is closed");
        }
        ensureNodesAreAvailable(nodes);
        int index = getNodeNumber();
        RetryListener<Response> retryListener = new RetryListener<>(callback, listener, nodes, index, hostFailureListener);
        DiscoveryNode node = retryListener.getNode(0);
        try {
            callback.doWithNode(node, retryListener);
        } catch (Exception e) {
            try {
                //this exception can't come from the TransportService as it doesn't throw exception at all
                listener.onFailure(e);
            } finally {
                retryListener.maybeNodeFailed(node, e);
            }
        }
    }

    public static class RetryListener<Response> implements ActionListener<Response> {
        private final NodeListenerCallback<Response> callback;
        private final ActionListener<Response> listener;
        private final List<DiscoveryNode> nodes;
        private final int index;
        private final TransportClient.HostFailureListener hostFailureListener;

        private volatile int i;

        RetryListener(NodeListenerCallback<Response> callback, ActionListener<Response> listener,
                             List<DiscoveryNode> nodes, int index, TransportClient.HostFailureListener hostFailureListener) {
            this.callback = callback;
            this.listener = listener;
            this.nodes = nodes;
            this.index = index;
            this.hostFailureListener = hostFailureListener;
        }

        @Override
        public void onResponse(Response response) {
            listener.onResponse(response);
        }

        @Override
        public void onFailure(Exception e) {
            Throwable throwable = ExceptionsHelper.unwrapCause(e);
            if (throwable instanceof ConnectTransportException) {
                maybeNodeFailed(getNode(this.i), (ConnectTransportException) throwable);
                int i = ++this.i;
                if (i >= nodes.size()) {
                    listener.onFailure(new NoNodeAvailableException("None of the configured nodes were available: " + nodes, e));
                } else {
                    try {
                        callback.doWithNode(getNode(i), this);
                    } catch(final Exception inner) {
                        inner.addSuppressed(e);
                        // this exception can't come from the TransportService as it doesn't throw exceptions at all
                        listener.onFailure(inner);
                    }
                }
            } else {
                listener.onFailure(e);
            }
        }

        final DiscoveryNode getNode(int i) {
            return nodes.get((index + i) % nodes.size());
        }

        final void maybeNodeFailed(DiscoveryNode node, Exception ex) {
            if (ex instanceof NodeDisconnectedException || ex instanceof NodeNotConnectedException) {
                hostFailureListener.onNodeDisconnected(node, ex);
            }
        }
    }

    @Override
    public void close() {
        synchronized (mutex) {
            if (closed) {
                return;
            }
            closed = true;
            FutureUtils.cancel(nodesSamplerFuture);
            for (DiscoveryNode node : nodes) {
                transportService.disconnectFromNode(node);
            }
            for (DiscoveryNode listedNode : listedNodes) {
                transportService.disconnectFromNode(listedNode);
            }
            nodes = Collections.emptyList();
        }
    }

    private int getNodeNumber() {
        int index = randomNodeGenerator.incrementAndGet();
        if (index < 0) {
            index = 0;
            randomNodeGenerator.set(0);
        }
        return index;
    }

    private void ensureNodesAreAvailable(List<DiscoveryNode> nodes) {
        if (nodes.isEmpty()) {
            String message = String.format(Locale.ROOT, "None of the configured nodes are available: %s", this.listedNodes);
            throw new NoNodeAvailableException(message);
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
        protected List<DiscoveryNode> validateNewNodes(Set<DiscoveryNode> nodes) {
            for (Iterator<DiscoveryNode> it = nodes.iterator(); it.hasNext(); ) {
                DiscoveryNode node = it.next();
                if (!transportService.nodeConnected(node)) {
                    try {
                        logger.trace("connecting to node [{}]", node);
                        transportService.connectToNode(node);
                    } catch (Exception e) {
                        it.remove();
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("failed to connect to discovered node [{}]", node), e);
                    }
                }
            }

            return Collections.unmodifiableList(new ArrayList<>(nodes));
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
            HashSet<DiscoveryNode> newNodes = new HashSet<>();
            HashSet<DiscoveryNode> newFilteredNodes = new HashSet<>();
            for (DiscoveryNode listedNode : listedNodes) {
                try (Transport.Connection connection = transportService.openConnection(listedNode, LISTED_NODES_PROFILE)){
                    final PlainTransportFuture<LivenessResponse> handler = new PlainTransportFuture<>(
                        new FutureTransportResponseHandler<LivenessResponse>() {
                            @Override
                            public LivenessResponse newInstance() {
                                return new LivenessResponse();
                            }
                        });
                    transportService.sendRequest(connection, TransportLivenessAction.NAME, new LivenessRequest(),
                        TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).withTimeout(pingTimeout).build(),
                        handler);
                    final LivenessResponse livenessResponse = handler.txGet();
                    if (!ignoreClusterName && !clusterName.equals(livenessResponse.getClusterName())) {
                        logger.warn("node {} not part of the cluster {}, ignoring...", listedNode, clusterName);
                        newFilteredNodes.add(listedNode);
                    } else {
                        // use discovered information but do keep the original transport address,
                        // so people can control which address is exactly used.
                        DiscoveryNode nodeWithInfo = livenessResponse.getDiscoveryNode();
                        newNodes.add(new DiscoveryNode(nodeWithInfo.getName(), nodeWithInfo.getId(), nodeWithInfo.getEphemeralId(),
                            nodeWithInfo.getHostName(), nodeWithInfo.getHostAddress(), listedNode.getAddress(),
                            nodeWithInfo.getAttributes(), nodeWithInfo.getRoles(), nodeWithInfo.getVersion()));
                    }
                } catch (ConnectTransportException e) {
                    logger.debug(
                        (Supplier<?>)
                            () -> new ParameterizedMessage("failed to connect to node [{}], ignoring...", listedNode), e);
                    hostFailureListener.onNodeDisconnected(listedNode, e);
                } catch (Exception e) {
                    logger.info(
                        (Supplier<?>) () -> new ParameterizedMessage("failed to get node info for {}, disconnecting...", listedNode), e);
                }
            }

            nodes = validateNewNodes(newNodes);
            filteredNodes = Collections.unmodifiableList(new ArrayList<>(newFilteredNodes));
        }
    }

    class SniffNodesSampler extends NodeSampler {

        @Override
        protected void doSample() {
            // the nodes we are going to ping include the core listed nodes that were added
            // and the last round of discovered nodes
            Set<DiscoveryNode> nodesToPing = new HashSet<>();
            for (DiscoveryNode node : listedNodes) {
                nodesToPing.add(node);
            }
            for (DiscoveryNode node : nodes) {
                nodesToPing.add(node);
            }

            final CountDownLatch latch = new CountDownLatch(nodesToPing.size());
            final ConcurrentMap<DiscoveryNode, ClusterStateResponse> clusterStateResponses = ConcurrentCollections.newConcurrentMap();
            try {
                for (final DiscoveryNode nodeToPing : nodesToPing) {
                    threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new AbstractRunnable() {

                        /**
                         * we try to reuse existing connections but if needed we will open a temporary connection
                         * that will be closed at the end of the execution.
                         */
                        Transport.Connection connectionToClose = null;

                        void onDone() {
                            try {
                                IOUtils.closeWhileHandlingException(connectionToClose);
                            } finally {
                                latch.countDown();
                            }
                        }

                        @Override
                        public void onFailure(Exception e) {
                            onDone();
                            if (e instanceof ConnectTransportException) {
                                logger.debug((Supplier<?>)
                                    () -> new ParameterizedMessage("failed to connect to node [{}], ignoring...", nodeToPing), e);
                                hostFailureListener.onNodeDisconnected(nodeToPing, e);
                            } else {
                                logger.info(
                                    (Supplier<?>) () -> new ParameterizedMessage(
                                        "failed to get local cluster state info for {}, disconnecting...", nodeToPing), e);
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            Transport.Connection pingConnection = null;
                            if (nodes.contains(nodeToPing)) {
                                try {
                                    pingConnection = transportService.getConnection(nodeToPing);
                                } catch (NodeNotConnectedException e) {
                                    // will use a temp connection
                                }
                            }
                            if (pingConnection == null) {
                                logger.trace("connecting to cluster node [{}]", nodeToPing);
                                connectionToClose = transportService.openConnection(nodeToPing, LISTED_NODES_PROFILE);
                                pingConnection = connectionToClose;
                            }
                            transportService.sendRequest(pingConnection, ClusterStateAction.NAME,
                                Requests.clusterStateRequest().clear().nodes(true).local(true),
                                TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE)
                                    .withTimeout(pingTimeout).build(),
                                new TransportResponseHandler<ClusterStateResponse>() {

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
                                        clusterStateResponses.put(nodeToPing, response);
                                        onDone();
                                    }

                                    @Override
                                    public void handleException(TransportException e) {
                                        logger.info(
                                            (Supplier<?>) () -> new ParameterizedMessage(
                                                "failed to get local cluster state for {}, disconnecting...", nodeToPing), e);
                                        try {
                                            hostFailureListener.onNodeDisconnected(nodeToPing, e);
                                        } finally {
                                            onDone();
                                        }
                                    }
                                });
                        }
                    });
                }
                latch.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }

            HashSet<DiscoveryNode> newNodes = new HashSet<>();
            HashSet<DiscoveryNode> newFilteredNodes = new HashSet<>();
            for (Map.Entry<DiscoveryNode, ClusterStateResponse> entry : clusterStateResponses.entrySet()) {
                if (!ignoreClusterName && !clusterName.equals(entry.getValue().getClusterName())) {
                    logger.warn("node {} not part of the cluster {}, ignoring...",
                            entry.getValue().getState().nodes().getLocalNode(), clusterName);
                    newFilteredNodes.add(entry.getKey());
                    continue;
                }
                for (ObjectCursor<DiscoveryNode> cursor : entry.getValue().getState().nodes().getDataNodes().values()) {
                    newNodes.add(cursor.value);
                }
            }

            nodes = validateNewNodes(newNodes);
            filteredNodes = Collections.unmodifiableList(new ArrayList<>(newFilteredNodes));
        }
    }

    public interface NodeListenerCallback<Response> {

        void doWithNode(DiscoveryNode node, ActionListener<Response> listener);
    }

    // pkg private for testing
    void doSample() {
        nodesSampler.doSample();
    }
}
