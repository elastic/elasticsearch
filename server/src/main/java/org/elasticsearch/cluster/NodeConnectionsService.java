/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.positiveTimeSetting;
import static org.elasticsearch.core.Strings.format;

/**
 * This component is responsible for maintaining connections from this node to all the nodes listed in the cluster state, and for
 * disconnecting from nodes once they are removed from the cluster state. It periodically checks that all connections are still open and
 * restores them if needed. Note that this component is *not* responsible for removing nodes from the cluster state if they disconnect or
 * are unresponsive: this is the job of the master's fault detection components, particularly {@link FollowersChecker}.
 * <p>
 * The {@link NodeConnectionsService#connectToNodes(DiscoveryNodes, Runnable)} and {@link
 * NodeConnectionsService#disconnectFromNodesExcept(DiscoveryNodes)} methods are called on the {@link ClusterApplier} thread. This component
 * allows the {@code ClusterApplier} to block on forming connections to _new_ nodes, because the rest of the system treats a missing
 * connection with another node in the cluster state as an exceptional condition and we don't want this to happen to new nodes. However we
 * need not block on re-establishing existing connections because if a connection is down then we are already in an exceptional situation
 * and it doesn't matter much if we stay in this situation a little longer.
 * <p>
 * This component does not block on disconnections at all, because a disconnection might need to wait for an ongoing (background) connection
 * attempt to complete first.
 */
public class NodeConnectionsService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(NodeConnectionsService.class);

    public static final Setting<TimeValue> CLUSTER_NODE_RECONNECT_INTERVAL_SETTING = positiveTimeSetting(
        "cluster.nodes.reconnect_interval",
        TimeValue.timeValueSeconds(10),
        Property.NodeScope
    );

    private final ThreadPool threadPool;
    private final TransportService transportService;

    // Protects changes to targetsByNode and its values (i.e. ConnectionTarget#activityType and ConnectionTarget#listener).
    // Crucially there are no blocking calls under this mutex: it is not held while connecting or disconnecting.
    private final Object mutex = new Object();

    // contains an entry for every node in the latest cluster state
    private final Map<DiscoveryNode, ConnectionTarget> targetsByNode = new HashMap<>();

    private final TimeValue reconnectInterval;
    private volatile ConnectionChecker connectionChecker;
    private final ConnectionHistory connectionHistory;

    @Inject
    public NodeConnectionsService(Settings settings, ThreadPool threadPool, TransportService transportService) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.reconnectInterval = NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.get(settings);
        this.connectionHistory = new ConnectionHistory();
    }

    /**
     * Connect to all the given nodes, but do not disconnect from any extra nodes. Calls the completion handler on completion of all
     * connection attempts to _new_ nodes, without waiting for any attempts to re-establish connections to nodes that were already known.
     */
    public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {

        if (discoveryNodes.getSize() == 0) {
            onCompletion.run();
            return;
        }

        final List<Runnable> runnables = new ArrayList<>(discoveryNodes.getSize());
        try (var refs = new RefCountingRunnable(onCompletion)) {
            synchronized (mutex) {
                connectionHistory.reserveConnectionHistoryForNodes(DiscoveryNodes);
                // Ugly hack: when https://github.com/elastic/elasticsearch/issues/94946 is fixed, just iterate over discoveryNodes here
                for (final Iterator<DiscoveryNode> iterator = discoveryNodes.mastersFirstStream().iterator(); iterator.hasNext();) {
                    final DiscoveryNode discoveryNode = iterator.next();
                    ConnectionTarget connectionTarget = targetsByNode.get(discoveryNode);
                    final boolean isNewNode = connectionTarget == null;
                    if (isNewNode) {
                        connectionTarget = new ConnectionTarget(discoveryNode);
                        targetsByNode.put(discoveryNode, connectionTarget);
                    }

                    if (isNewNode) {
                        logger.debug("connecting to {}", discoveryNode);
                        runnables.add(connectionTarget.connect(refs.acquire()));
                    } else {
                        // known node, try and ensure it's connected but do not wait
                        logger.trace("checking connection to existing node [{}]", discoveryNode);
                        runnables.add(connectionTarget.connect(null));
                    }
                }
            }
        }
        runnables.forEach(Runnable::run);
    }

    /**
     * Disconnect from any nodes to which we are currently connected which do not appear in the given nodes. Does not wait for the
     * disconnections to complete, because they might have to wait for ongoing connection attempts first.
     */
    public void disconnectFromNodesExcept(DiscoveryNodes discoveryNodes) {
        final List<Runnable> runnables = new ArrayList<>();
        synchronized (mutex) {
            final Set<DiscoveryNode> nodesToDisconnect = new HashSet<>(targetsByNode.keySet());
            for (final DiscoveryNode discoveryNode : discoveryNodes) {
                nodesToDisconnect.remove(discoveryNode);
            }

            connectionHistory.removeConnectionHistoryForNodes(nodesToDisconnect);
            for (final DiscoveryNode discoveryNode : nodesToDisconnect) {
                runnables.add(targetsByNode.remove(discoveryNode)::disconnect);
            }
        }
        runnables.forEach(Runnable::run);
    }

    /**
     * Makes a single attempt to reconnect to any nodes which are disconnected but should be connected. Does not attempt to reconnect any
     * nodes which are in the process of disconnecting. The onCompletion handler is called after all connection attempts have completed.
     */
    void ensureConnections(Runnable onCompletion) {
        final List<Runnable> runnables = new ArrayList<>();
        try (var refs = new RefCountingRunnable(onCompletion)) {
            synchronized (mutex) {
                logger.trace("ensureConnections: {}", targetsByNode);
                for (ConnectionTarget connectionTarget : targetsByNode.values()) {
                    runnables.add(connectionTarget.connect(refs.acquire()));
                }
            }
        }
        runnables.forEach(Runnable::run);
    }

    class ConnectionChecker extends AbstractRunnable {
        protected void doRun() {
            if (connectionChecker == this) {
                ensureConnections(this::scheduleNextCheck);
            }
        }

        void scheduleNextCheck() {
            if (connectionChecker == this) {
                threadPool.scheduleUnlessShuttingDown(reconnectInterval, threadPool.generic(), this);
            }
        }

        @Override
        public void onFailure(Exception e) {
            logger.warn("unexpected error while checking for node reconnects", e);
            scheduleNextCheck();
        }

        @Override
        public String toString() {
            return "periodic reconnection checker";
        }
    }

    @Override
    protected void doStart() {
        final ConnectionChecker connectionChecker = new ConnectionChecker();
        this.connectionChecker = connectionChecker;
        connectionChecker.scheduleNextCheck();
    }

    @Override
    protected void doStop() {
        connectionChecker = null;
    }

    @Override
    protected void doClose() {}

    // for disruption tests, re-establish any disrupted connections
    public void reconnectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
        connectToNodes(discoveryNodes, () -> {
            disconnectFromNodesExcept(discoveryNodes);
            ensureConnections(onCompletion);
        });
    }

    private class ConnectionTarget {
        private final DiscoveryNode discoveryNode;

        private final AtomicInteger consecutiveFailureCount = new AtomicInteger();
        private final AtomicReference<Releasable> connectionRef = new AtomicReference<>();

        // all access to these fields is synchronized
        private List<Releasable> pendingRefs;
        private boolean connectionInProgress;

        // placeholder listener for a fire-and-forget connection attempt
        private static final List<Releasable> NOOP = List.of();

        ConnectionTarget(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        private void setConnectionRef(Releasable connectionReleasable) {
            Releasables.close(connectionRef.getAndSet(connectionReleasable));
        }

        Runnable connect(Releasable onCompletion) {
            return () -> {
                registerRef(onCompletion);
                doConnect();
            };
        }

        private synchronized void registerRef(Releasable ref) {
            if (ref == null) {
                pendingRefs = pendingRefs == null ? NOOP : pendingRefs;
                return;
            }

            if (pendingRefs == null || pendingRefs == NOOP) {
                pendingRefs = new ArrayList<>();
            }
            pendingRefs.add(ref);
        }

        private synchronized Releasable acquireRefs() {
            // Avoid concurrent connection attempts because they don't necessarily complete in order otherwise, and out-of-order completion
            // might mean we end up disconnected from a node even though we triggered a call to connect() after all close() calls had
            // finished.
            if (connectionInProgress == false) {
                var refs = pendingRefs;
                if (refs != null) {
                    pendingRefs = null;
                    connectionInProgress = true;
                    return Releasables.wrap(refs);
                }
            }
            return null;
        }

        private synchronized void releaseListener() {
            assert connectionInProgress;
            connectionInProgress = false;
        }

        private void doConnect() {
            // noinspection resource
            var refs = acquireRefs();
            if (refs == null) {
                return;
            }

            final boolean alreadyConnected = transportService.nodeConnected(discoveryNode);

            if (alreadyConnected) {
                logger.trace("refreshing connection to {}", discoveryNode);
            } else {
                logger.debug("connecting to {}", discoveryNode);
            }

            // It's possible that connectionRef is a reference to an older connection that closed out from under us, but that something else
            // has opened a fresh connection to the node. Therefore we always call connectToNode() and update connectionRef.
            transportService.connectToNode(discoveryNode, ActionListener.runAfter(new ActionListener<>() {
                @Override
                public void onResponse(Releasable connectionReleasable) {
                    if (alreadyConnected) {
                        logger.trace("refreshed connection to {}", discoveryNode);
                    } else {
                        logger.debug("connected to {}", discoveryNode);
                    }
                    consecutiveFailureCount.set(0);
                    setConnectionRef(connectionReleasable);

                    final boolean isActive;
                    synchronized (mutex) {
                        isActive = targetsByNode.get(discoveryNode) == ConnectionTarget.this;
                    }
                    if (isActive == false) {
                        logger.debug("connected to stale {} - releasing stale connection", discoveryNode);
                        setConnectionRef(null);
                    }
                    Releasables.closeExpectNoException(refs);
                }

                @Override
                public void onFailure(Exception e) {
                    final int currentFailureCount = consecutiveFailureCount.incrementAndGet();
                    // Only warn every 6th failure. We work around this log while stopping integ test clusters in InternalTestCluster#close
                    // by temporarily raising the log level to ERROR. If the nature of this log changes in the future, that workaround might
                    // need to be adjusted.
                    final Level level = currentFailureCount % 6 == 1 ? Level.WARN : Level.DEBUG;
                    logger.log(level, () -> format("failed to connect to %s (tried [%s] times)", discoveryNode, currentFailureCount), e);
                    setConnectionRef(null);
                    Releasables.closeExpectNoException(refs);
                }
            }, () -> {
                releaseListener();
                threadPool.generic().execute(new Runnable() {
                    @Override
                    public void run() {
                        ConnectionTarget.this.doConnect();
                    }

                    @Override
                    public String toString() {
                        return "ensure connection to " + discoveryNode;
                    }
                });
            }));
        }

        void disconnect() {
            setConnectionRef(null);
            logger.debug("disconnected from {}", discoveryNode);
        }

        @Override
        public String toString() {
            synchronized (mutex) {
                return "ConnectionTarget{" + "discoveryNode=" + discoveryNode + '}';
            }
        }
    }

    private class ConnectionHistory {
        record NodeConnectionHistory(String ephemeralId, long disconnectTime, Exception disconnectCause) {}

        /**
         * Holds the DiscoveryNode nodeId to connection history record.
         *
         * Entries for each node are reserved during NodeConnectionsService.connectToNodes, by placing a (nodeId, dummy) entry
         * for each node in the cluster. On node disconnect, this entry is updated with its NodeConnectionHistory. On node
         * connect, this entry is reset to the dummy value. On NodeConnectionsService.disconnectFromNodesExcept, node entries
         * are removed.
         *
         * Each node in the cluster always has a nodeHistory entry that is either the dummy value or a connection history record. This
         * allows node disconnect callbacks to discard their entry if the disconnect occurred because of a change in cluster state.
         */
        private final NodeConnectionHistory dummy = new NodeConnectionHistory("", 0, null);
        private final ConcurrentMap<String, NodeConnectionHistory> nodeHistory = ConcurrentCollections.newConcurrentMap();

        ConnectionHistory() {
            NodeConnectionsService.this.transportService.addConnectionListener(new TransportConnectionListener() {
                @Override
                public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
                    // log case where the remote node has same ephemeralId as its previous connection
                    // (the network was disrupted, but not the remote process)
                    NodeConnectionHistory nodeConnectionHistory = nodeHistory.get(node.getId());
                    if (nodeConnectionHistory != null) {
                        nodeHistory.replace(node.getId(), nodeConnectionHistory, dummy);
                    }

                    if (nodeConnectionHistory != null
                        && nodeConnectionHistory != dummy
                        && nodeConnectionHistory.ephemeralId.equals(node.getEphemeralId())) {
                        if (nodeConnectionHistory.disconnectCause != null) {
                            logger.warn(
                                () -> format(
                                    "reopened transport connection to node [%s] "
                                        + "which disconnected exceptionally [%dms] ago but did not "
                                        + "restart, so the disconnection is unexpected; "
                                        + "if unexpected, see [{}] for troubleshooting guidance",
                                    node.descriptionWithoutAttributes(),
                                    nodeConnectionHistory.disconnectTime,
                                    ReferenceDocs.NETWORK_DISCONNECT_TROUBLESHOOTING
                                ),
                                nodeConnectionHistory.disconnectCause
                            );
                        } else {
                            logger.warn(
                                """
                                    reopened transport connection to node [{}] \
                                    which disconnected gracefully [{}ms] ago but did not \
                                    restart, so the disconnection is unexpected; \
                                    if unexpected, see [{}] for troubleshooting guidance""",
                                node.descriptionWithoutAttributes(),
                                nodeConnectionHistory.disconnectTime,
                                ReferenceDocs.NETWORK_DISCONNECT_TROUBLESHOOTING
                            );
                        }
                    }
                }

                @Override
                public void onNodeDisconnected(DiscoveryNode node, Transport.Connection connection) {
                    connection.addCloseListener(new ActionListener<Void>() {
                        @Override
                        public void onResponse(Void ignored) {
                            insertNodeConnectionHistory(null);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            insertNodeConnectionHistory(e);
                        }

                        private void insertNodeConnectionHistory(@Nullable Exception e) {
                            final long disconnectTime = threadPool.absoluteTimeInMillis();
                            final NodeConnectionHistory nodeConnectionHistory = new NodeConnectionHistory(
                                node.getEphemeralId(),
                                disconnectTime,
                                e
                            );
                            final String nodeId = node.getId();
                            NodeConnectionHistory previousConnectionHistory = nodeHistory.get(nodeId);
                            if (previousConnectionHistory != null) {
                                nodeHistory.replace(nodeId, previousConnectionHistory, nodeConnectionHistory);
                            }
                        }
                    });
                }
            });
        }

        void reserveConnectionHistoryForNodes(DiscoveryNodes nodes) {
            for (DiscoveryNode node : nodes) {
                nodeHistory.put(node.getId(), dummy);
            }
        }

        void removeConnectionHistoryForNodes(Set<DiscoveryNode> nodes) {
            final int startSize = nodeHistory.size();
            for (DiscoveryNode node : nodes) {
                nodeHistory.remove(node.getId());
            }
            logger.trace("Connection history garbage-collected from {} to {} entries", startSize, nodeHistory.size());
        }

        int connectionHistorySize() {
            return nodeHistory.size();
        }
    }
}
