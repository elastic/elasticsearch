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
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

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

    private final Version minCompatibilityVersion;

    // nodes that are added to be discovered
    private volatile List<DiscoveryNode> listedNodes = Collections.emptyList();

    private final Object mutex = new Object();

    private volatile List<DiscoveryNode> nodes = Collections.emptyList();
    private volatile List<DiscoveryNode> filteredNodes = Collections.emptyList();

    private final AtomicInteger tempNodeIdGenerator = new AtomicInteger();

    private final NodeSampler nodesSampler;

    private volatile ScheduledFuture nodesSamplerFuture;

    private final AtomicInteger randomNodeGenerator = new AtomicInteger();

    private final boolean ignoreClusterName;

    private volatile boolean closed;


    public static final Setting<TimeValue> CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL =
        Setting.positiveTimeSetting("client.transport.nodes_sampler_interval", timeValueSeconds(5), Property.NodeScope);
    public static final Setting<TimeValue> CLIENT_TRANSPORT_PING_TIMEOUT =
        Setting.positiveTimeSetting("client.transport.ping_timeout", timeValueSeconds(5), Property.NodeScope);
    public static final Setting<Boolean> CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME =
        Setting.boolSetting("client.transport.ignore_cluster_name", false, Property.NodeScope);
    public static final Setting<Boolean> CLIENT_TRANSPORT_SNIFF =
        Setting.boolSetting("client.transport.sniff", false, Property.NodeScope);

    @Inject
    public TransportClientNodesService(Settings settings,TransportService transportService,
                                       ThreadPool threadPool, Version version) {
        super(settings);
        this.clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
        this.transportService = transportService;
        this.threadPool = threadPool;
        this.minCompatibilityVersion = version.minimumCompatibilityVersion();

        this.nodesSamplerInterval = CLIENT_TRANSPORT_NODES_SAMPLER_INTERVAL.get(this.settings);
        this.pingTimeout = CLIENT_TRANSPORT_PING_TIMEOUT.get(this.settings).millis();
        this.ignoreClusterName = CLIENT_TRANSPORT_IGNORE_CLUSTER_NAME.get(this.settings);

        if (logger.isDebugEnabled()) {
            logger.debug("node_sampler_interval[{}]", nodesSamplerInterval);
        }

        if (CLIENT_TRANSPORT_SNIFF.get(this.settings)) {
            this.nodesSampler = new SniffNodesSampler();
        } else {
            this.nodesSampler = new SimpleNodeSampler();
        }
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
            List<DiscoveryNode> builder = new ArrayList<>();
            builder.addAll(listedNodes());
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
            List<DiscoveryNode> builder = new ArrayList<>();
            for (DiscoveryNode otherNode : listedNodes) {
                if (!otherNode.getAddress().equals(transportAddress)) {
                    builder.add(otherNode);
                } else {
                    logger.debug("removing address [{}]", otherNode);
                }
            }
            listedNodes = Collections.unmodifiableList(builder);
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
        RetryListener<Response> retryListener = new RetryListener<>(callback, listener, nodes, index);
        DiscoveryNode node = nodes.get((index) % nodes.size());
        try {
            callback.doWithNode(node, retryListener);
        } catch (Throwable t) {
            //this exception can't come from the TransportService as it doesn't throw exception at all
            listener.onFailure(t);
        }
    }

    public static class RetryListener<Response> implements ActionListener<Response> {
        private final NodeListenerCallback<Response> callback;
        private final ActionListener<Response> listener;
        private final List<DiscoveryNode> nodes;
        private final int index;

        private volatile int i;

        public RetryListener(NodeListenerCallback<Response> callback, ActionListener<Response> listener,
                             List<DiscoveryNode> nodes, int index) {
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
                    listener.onFailure(new NoNodeAvailableException("None of the configured nodes were available: " + nodes, e));
                } else {
                    try {
                        callback.doWithNode(nodes.get((index + i) % nodes.size()), this);
                    } catch(final Throwable t) {
                        // this exception can't come from the TransportService as it doesn't throw exceptions at all
                        listener.onFailure(t);
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
                    } catch (Throwable e) {
                        it.remove();
                        logger.debug("failed to connect to discovered node [{}]", e, node);
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
                if (!transportService.nodeConnected(listedNode)) {
                    try {
                        // its a listed node, light connect to it...
                        logger.trace("connecting to listed node (light) [{}]", listedNode);
                        transportService.connectToNodeLight(listedNode);
                    } catch (Throwable e) {
                        logger.debug("failed to connect to node [{}], removed from nodes list", e, listedNode);
                        newFilteredNodes.add(listedNode);
                        continue;
                    }
                }
                try {
                    LivenessResponse livenessResponse = transportService.submitRequest(listedNode, TransportLivenessAction.NAME,
                            new LivenessRequest(),
                            TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE).withTimeout(pingTimeout).build(),
                            new FutureTransportResponseHandler<LivenessResponse>() {
                                @Override
                                public LivenessResponse newInstance() {
                                    return new LivenessResponse();
                                }
                            }).txGet();
                    if (!ignoreClusterName && !clusterName.equals(livenessResponse.getClusterName())) {
                        logger.warn("node {} not part of the cluster {}, ignoring...", listedNode, clusterName);
                        newFilteredNodes.add(listedNode);
                    } else if (livenessResponse.getDiscoveryNode() != null) {
                        // use discovered information but do keep the original transport address,
                        // so people can control which address is exactly used.
                        DiscoveryNode nodeWithInfo = livenessResponse.getDiscoveryNode();
                        newNodes.add(new DiscoveryNode(nodeWithInfo.getName(), nodeWithInfo.getId(), nodeWithInfo.getHostName(),
                                nodeWithInfo.getHostAddress(), listedNode.getAddress(), nodeWithInfo.getAttributes(),
                                nodeWithInfo.getRoles(), nodeWithInfo.getVersion()));
                    } else {
                        // although we asked for one node, our target may not have completed
                        // initialization yet and doesn't have cluster nodes
                        logger.debug("node {} didn't return any discovery info, temporarily using transport discovery node", listedNode);
                        newNodes.add(listedNode);
                    }
                } catch (Throwable e) {
                    logger.info("failed to get node info for {}, disconnecting...", e, listedNode);
                    transportService.disconnectFromNode(listedNode);
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
            for (final DiscoveryNode listedNode : nodesToPing) {
                threadPool.executor(ThreadPool.Names.MANAGEMENT).execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (!transportService.nodeConnected(listedNode)) {
                                try {

                                    // if its one of the actual nodes we will talk to, not to listed nodes, fully connect
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
                                    Requests.clusterStateRequest().clear().nodes(true).local(true),
                                    TransportRequestOptions.builder().withType(TransportRequestOptions.Type.STATE)
                                            .withTimeout(pingTimeout).build(),
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
                                            clusterStateResponses.put(listedNode, response);
                                            latch.countDown();
                                        }

                                        @Override
                                        public void handleException(TransportException e) {
                                            logger.info("failed to get local cluster state for {}, disconnecting...", e, listedNode);
                                            transportService.disconnectFromNode(listedNode);
                                            latch.countDown();
                                        }
                                    });
                        } catch (Throwable e) {
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
}
