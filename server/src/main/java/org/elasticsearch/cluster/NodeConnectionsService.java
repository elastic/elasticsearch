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

    @Inject
    public NodeConnectionsService(Settings settings, ThreadPool threadPool, TransportService transportService) {
        this.threadPool = threadPool;
        this.transportService = transportService;
        this.reconnectInterval = NodeConnectionsService.CLUSTER_NODE_RECONNECT_INTERVAL_SETTING.get(settings);
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
        transportService.addConnectionListener(new ConnectionChangeListener());
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

    // exposed for testing
    protected DisconnectionHistory disconnectionHistoryForNode(DiscoveryNode node) {
        synchronized (mutex) {
            ConnectionTarget connectionTarget = targetsByNode.get(node);
            if (connectionTarget != null) {
                return connectionTarget.disconnectionHistory;
            }
        }
        return null;
    }

    /**
     * Time of disconnect in absolute time ({@link ThreadPool#absoluteTimeInMillis()}),
     * and disconnect-causing exception, if any
     */
    record DisconnectionHistory(long disconnectTimeMillis, @Nullable Exception disconnectCause) {
        public long getDisconnectTimeMillis() {
            return disconnectTimeMillis;
        }

        public Exception getDisconnectCause() {
            return disconnectCause;
        }
    }

    private class ConnectionTarget {
        private final DiscoveryNode discoveryNode;

        private final AtomicInteger consecutiveFailureCount = new AtomicInteger();
        private final AtomicReference<Releasable> connectionRef = new AtomicReference<>();

        // access is synchronized by the service mutex
        @Nullable // null when node is connected or initialized; non-null in between disconnects and connects
        private DisconnectionHistory disconnectionHistory = null;

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
            return "ConnectionTarget{discoveryNode=" + discoveryNode + '}';
        }
    }

    /**
     * Receives connection/disconnection events from the transport, and records them in per-node DisconnectionHistory
     * structures for logging network issues. DisconnectionHistory records are stored their node's ConnectionTarget.
     *
     * Network issues (that this listener monitors for) occur whenever a reconnection to a node succeeds,
     * and it has the same ephemeral ID as it did during the last connection; this happens when a connection event
     * occurs, and its ConnectionTarget entry has a previous DisconnectionHistory stored.
     */
    private class ConnectionChangeListener implements TransportConnectionListener {
        @Override
        public void onNodeConnected(DiscoveryNode node, Transport.Connection connection) {
            DisconnectionHistory disconnectionHistory = null;
            synchronized (mutex) {
                ConnectionTarget connectionTarget = targetsByNode.get(node);
                if (connectionTarget != null) {
                    disconnectionHistory = connectionTarget.disconnectionHistory;
                    connectionTarget.disconnectionHistory = null;
                }
            }

            if (disconnectionHistory != null) {
                long millisSinceDisconnect = threadPool.absoluteTimeInMillis() - disconnectionHistory.disconnectTimeMillis;
                TimeValue timeValueSinceDisconnect = TimeValue.timeValueMillis(millisSinceDisconnect);
                if (disconnectionHistory.disconnectCause != null) {
                    logger.warn(
                        () -> format(
                            """
                                reopened transport connection to node [%s] \
                                which disconnected exceptionally [%s/%dms] ago but did not \
                                restart, so the disconnection is unexpected; \
                                see [%s] for troubleshooting guidance""",
                            node.descriptionWithoutAttributes(),
                            timeValueSinceDisconnect,
                            millisSinceDisconnect,
                            ReferenceDocs.NETWORK_DISCONNECT_TROUBLESHOOTING
                        ),
                        disconnectionHistory.disconnectCause
                    );
                } else {
                    logger.warn(
                        """
                            reopened transport connection to node [{}] \
                            which disconnected gracefully [{}/{}ms] ago but did not \
                            restart, so the disconnection is unexpected; \
                            see [{}] for troubleshooting guidance""",
                        node.descriptionWithoutAttributes(),
                        timeValueSinceDisconnect,
                        millisSinceDisconnect,
                        ReferenceDocs.NETWORK_DISCONNECT_TROUBLESHOOTING
                    );
                }
            }
        }

        @Override
        public void onNodeDisconnected(DiscoveryNode node, @Nullable Exception closeException) {
            DisconnectionHistory disconnectionHistory = new DisconnectionHistory(threadPool.absoluteTimeInMillis(), closeException);
            synchronized (mutex) {
                ConnectionTarget connectionTarget = targetsByNode.get(node);
                if (connectionTarget != null) {
                    connectionTarget.disconnectionHistory = disconnectionHistory;
                }
            }
        }
    }
}
