/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsRequest;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;

/**
 * A publication can succeed and complete before all nodes have applied the published state and acknowledged it; however we need every node
 * eventually either to apply the published state (or a later state) or be removed from the cluster. This component achieves this by
 * removing any lagging nodes from the cluster after a timeout.
 */
public class LagDetector {

    private static final Logger logger = LogManager.getLogger(LagDetector.class);

    // the timeout for each node to apply a cluster state update after the leader has applied it, before being removed from the cluster
    public static final Setting<TimeValue> CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.follower_lag.timeout",
        TimeValue.timeValueMillis(90000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    private final TimeValue clusterStateApplicationTimeout;
    private final LagListener lagListener;
    private final Supplier<DiscoveryNode> localNodeSupplier;
    private final ThreadPool threadPool;
    private final Map<DiscoveryNode, NodeAppliedStateTracker> appliedStateTrackersByNode = newConcurrentMap();

    public LagDetector(
        final Settings settings,
        final ThreadPool threadPool,
        final LagListener lagListener,
        final Supplier<DiscoveryNode> localNodeSupplier
    ) {
        this.threadPool = threadPool;
        this.clusterStateApplicationTimeout = CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.get(settings);
        this.lagListener = lagListener;
        this.localNodeSupplier = localNodeSupplier;
    }

    public void setTrackedNodes(final Iterable<DiscoveryNode> discoveryNodes) {
        final Set<DiscoveryNode> discoveryNodeSet = new HashSet<>();
        discoveryNodes.forEach(discoveryNodeSet::add);
        discoveryNodeSet.remove(localNodeSupplier.get());
        appliedStateTrackersByNode.keySet().retainAll(discoveryNodeSet);
        discoveryNodeSet.forEach(node -> appliedStateTrackersByNode.putIfAbsent(node, new NodeAppliedStateTracker(node)));
    }

    public void clearTrackedNodes() {
        appliedStateTrackersByNode.clear();
    }

    public void setAppliedVersion(final DiscoveryNode discoveryNode, final long appliedVersion) {
        final NodeAppliedStateTracker nodeAppliedStateTracker = appliedStateTrackersByNode.get(discoveryNode);
        if (nodeAppliedStateTracker == null) {
            // Received an ack from a node that a later publication has removed (or we are no longer master). No big deal.
            logger.trace("node {} applied version {} but this node's version is not being tracked", discoveryNode, appliedVersion);
        } else {
            nodeAppliedStateTracker.increaseAppliedVersion(appliedVersion);
        }
    }

    public void startLagDetector(final long version) {
        final List<NodeAppliedStateTracker> laggingTrackers = appliedStateTrackersByNode.values()
            .stream()
            .filter(t -> t.appliedVersionLessThan(version))
            .toList();

        if (laggingTrackers.isEmpty()) {
            logger.trace("lag detection for version {} is unnecessary: {}", version, appliedStateTrackersByNode.values());
        } else {
            logger.debug("starting lag detector for version {}: {}", version, laggingTrackers);

            threadPool.scheduleUnlessShuttingDown(clusterStateApplicationTimeout, Names.CLUSTER_COORDINATION, new Runnable() {
                @Override
                public void run() {
                    laggingTrackers.forEach(t -> t.checkForLag(version));
                }

                @Override
                public String toString() {
                    return "lag detector for version " + version + " on " + laggingTrackers;
                }
            });
        }
    }

    @Override
    public String toString() {
        return "LagDetector{"
            + "clusterStateApplicationTimeout="
            + clusterStateApplicationTimeout
            + ", appliedStateTrackersByNode="
            + appliedStateTrackersByNode.values()
            + '}';
    }

    // for assertions
    Set<DiscoveryNode> getTrackedNodes() {
        return Collections.unmodifiableSet(appliedStateTrackersByNode.keySet());
    }

    private class NodeAppliedStateTracker {
        private final DiscoveryNode discoveryNode;
        private final AtomicLong appliedVersion = new AtomicLong();

        NodeAppliedStateTracker(final DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        void increaseAppliedVersion(long appliedVersion) {
            long maxAppliedVersion = this.appliedVersion.updateAndGet(v -> Math.max(v, appliedVersion));
            logger.trace("{} applied version {}, max now {}", this, appliedVersion, maxAppliedVersion);
        }

        boolean appliedVersionLessThan(final long version) {
            return appliedVersion.get() < version;
        }

        @Override
        public String toString() {
            return "NodeAppliedStateTracker{" + "discoveryNode=" + discoveryNode + ", appliedVersion=" + appliedVersion + '}';
        }

        void checkForLag(final long version) {
            if (appliedStateTrackersByNode.get(discoveryNode) != this) {
                logger.trace("{} no longer active when checking version {}", this, version);
                return;
            }

            long appliedVersion = this.appliedVersion.get();
            if (version <= appliedVersion) {
                logger.trace("{} satisfied when checking version {}, node applied version {}", this, version, appliedVersion);
                return;
            }

            logger.warn(
                "node [{}] is lagging at cluster state version [{}], although publication of cluster state version [{}] completed [{}] ago",
                discoveryNode,
                appliedVersion,
                version,
                clusterStateApplicationTimeout
            );
            lagListener.onLagDetected(discoveryNode, appliedVersion, version);
        }
    }

    public interface LagListener {
        /**
         * Called when a node is detected as lagging and should be removed from the cluster.
         * @param discoveryNode the node that is lagging.
         * @param appliedVersion the cluster state version that the node has definitely applied
         * @param expectedVersion the cluster state version that we were waiting for the node to apply
         */
        void onLagDetected(DiscoveryNode discoveryNode, long appliedVersion, long expectedVersion);
    }

    /**
     * Wraps around another {@link LagListener} and logs the hot threads on the lagging node at debug level.
     */
    static class HotThreadsLoggingLagListener implements LagListener {

        private final TransportService transportService;
        private final Client client;
        private final LagListener delegate;

        HotThreadsLoggingLagListener(TransportService transportService, Client client, LagListener delegate) {
            this.transportService = transportService;
            this.client = client;
            this.delegate = delegate;
        }

        @Override
        public void onLagDetected(DiscoveryNode discoveryNode, long appliedVersion, long expectedVersion) {
            try {
                if (logger.isDebugEnabled() == false) {
                    return;
                }

                if (client == null) {
                    // only happens in tests
                    return;
                }

                final ActionListener<NodesHotThreadsResponse> debugListener = new ActionListener<>() {
                    @Override
                    public void onResponse(NodesHotThreadsResponse nodesHotThreadsResponse) {
                        if (nodesHotThreadsResponse.getNodes().size() == 0) {
                            assert nodesHotThreadsResponse.failures().size() == 1;
                            onFailure(nodesHotThreadsResponse.failures().get(0));
                            return;
                        }

                        logger.debug(
                            "hot threads from node [{}] lagging at version [{}] despite commit of cluster state version [{}]:\n{}",
                            discoveryNode.descriptionWithoutAttributes(),
                            appliedVersion,
                            expectedVersion,
                            nodesHotThreadsResponse.getNodes().get(0).getHotThreads()
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.debug(
                            new ParameterizedMessage(
                                "failed to get hot threads from node [{}] lagging at version {} "
                                    + "despite commit of cluster state version [{}]",
                                discoveryNode.descriptionWithoutAttributes(),
                                appliedVersion,
                                expectedVersion
                            ),
                            e
                        );
                    }
                };

                // we're removing the node from the cluster so we need to keep the connection open for the hot threads request
                transportService.connectToNode(discoveryNode, new ActionListener<>() {
                    @Override
                    public void onResponse(Releasable releasable) {
                        boolean success = false;
                        final ThreadContext threadContext = transportService.getThreadPool().getThreadContext();
                        try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                            threadContext.markAsSystemContext();
                            client.execute(
                                NodesHotThreadsAction.INSTANCE,
                                new NodesHotThreadsRequest(discoveryNode).threads(500),
                                ActionListener.runBefore(debugListener, () -> Releasables.close(releasable))
                            );
                            success = true;
                        } finally {
                            if (success == false) {
                                Releasables.close(releasable);
                            }
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        debugListener.onFailure(e);
                    }
                });
            } finally {
                // call delegate after transportService#connectToNode to keep existing connection open
                delegate.onLagDetected(discoveryNode, appliedVersion, expectedVersion);
            }
        }
    }

}
