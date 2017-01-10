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
package org.elasticsearch.cluster;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.KeyedLock;
import org.elasticsearch.discovery.zen.MasterFaultDetection;
import org.elasticsearch.discovery.zen.NodesFaultDetection;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledFuture;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.positiveTimeSetting;


/**
 * This component is responsible for connecting to nodes once they are added to the cluster state, and disconnect when they are
 * removed. Also, it periodically checks that all connections are still open and if needed restores them.
 * Note that this component is *not* responsible for removing nodes from the cluster if they disconnect / do not respond
 * to pings. This is done by {@link NodesFaultDetection}. Master fault detection
 * is done by {@link MasterFaultDetection}.
 */
public class NodeConnectionsService extends AbstractLifecycleComponent {

    public static final Setting<TimeValue> CLUSTER_NODE_RECONNECT_INTERVAL_SETTING =
            positiveTimeSetting("cluster.nodes.reconnect_interval", TimeValue.timeValueSeconds(10), Property.NodeScope);
    private final ThreadPool threadPool;
    private final TransportService transportService;

    // map between current node and the number of failed connection attempts. 0 means successfully connected.
    // if a node doesn't appear in this list it shouldn't be monitored
    private ConcurrentMap<DiscoveryNode, Integer> nodes = ConcurrentCollections.newConcurrentMap();

    private final KeyedLock<DiscoveryNode> nodeLocks = new KeyedLock<>();

    private final TimeValue reconnectInterval;

    private volatile ScheduledFuture<?> backgroundFuture = null;

    @Inject
    public NodeConnectionsService(Settings settings, ThreadPool threadPool, TransportService transportService) {
        super(settings);
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.reconnectInterval = NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.get(settings);
    }

    public void connectToNodes(Iterable<DiscoveryNode> discoveryNodes) {

        // TODO: do this in parallel (and wait)
        for (final DiscoveryNode node : discoveryNodes) {
            try (Releasable ignored = nodeLocks.acquire(node)) {
                nodes.putIfAbsent(node, 0);
                validateNodeConnected(node);
            }
        }
    }

    /**
     * Disconnects from all nodes except the ones provided as parameter
     */
    public void disconnectFromNodesExcept(Iterable<DiscoveryNode> nodesToKeep) {
        Set<DiscoveryNode> currentNodes = new HashSet<>(nodes.keySet());
        for (DiscoveryNode node : nodesToKeep) {
            currentNodes.remove(node);
        }
        for (final DiscoveryNode node : currentNodes) {
            try (Releasable ignored = nodeLocks.acquire(node)) {
                Integer current = nodes.remove(node);
                assert current != null : "node " + node + " was removed in event but not in internal nodes";
                try {
                    transportService.disconnectFromNode(node);
                } catch (Exception e) {
                    logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to disconnect to node [{}]", node), e);
                }
            }
        }
    }

    void validateNodeConnected(DiscoveryNode node) {
        assert nodeLocks.isHeldByCurrentThread(node) : "validateNodeConnected must be called under lock";
        if (lifecycle.stoppedOrClosed() ||
                nodes.containsKey(node) == false) { // we double check existence of node since connectToNode might take time...
            // nothing to do
        } else {
            try {
                // connecting to an already connected node is a noop
                transportService.connectToNode(node);
                nodes.put(node, 0);
            } catch (Exception e) {
                Integer nodeFailureCount = nodes.get(node);
                assert nodeFailureCount != null : node + " didn't have a counter in nodes map";
                nodeFailureCount = nodeFailureCount + 1;
                // log every 6th failure
                if ((nodeFailureCount % 6) == 1) {
                    final int finalNodeFailureCount = nodeFailureCount;
                    logger.warn(
                        (Supplier<?>)
                            () -> new ParameterizedMessage(
                                "failed to connect to node {} (tried [{}] times)", node, finalNodeFailureCount), e);
                }
                nodes.put(node, nodeFailureCount);
            }
        }
    }

    class ConnectionChecker extends AbstractRunnable {

        @Override
        public void onFailure(Exception e) {
            logger.warn("unexpected error while checking for node reconnects", e);
        }

        protected void doRun() {
            for (DiscoveryNode node : nodes.keySet()) {
                try (Releasable ignored = nodeLocks.acquire(node)) {
                    validateNodeConnected(node);
                }
            }
        }

        @Override
        public void onAfter() {
            if (lifecycle.started()) {
                backgroundFuture = threadPool.schedule(reconnectInterval, ThreadPool.Names.GENERIC, this);
            }
        }
    }

    @Override
    protected void doStart() {
        backgroundFuture = threadPool.schedule(reconnectInterval, ThreadPool.Names.GENERIC, new ConnectionChecker());
    }

    @Override
    protected void doStop() {
        FutureUtils.cancel(backgroundFuture);
    }

    @Override
    protected void doClose() {

    }
}
