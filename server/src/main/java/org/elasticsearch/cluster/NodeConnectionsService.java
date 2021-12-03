/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.positiveTimeSetting;

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

        final GroupedActionListener<Void> listener = new GroupedActionListener<>(
            ActionListener.wrap(onCompletion),
            discoveryNodes.getSize()
        );

        final List<Runnable> runnables = new ArrayList<>(discoveryNodes.getSize());
        synchronized (mutex) {
            for (final DiscoveryNode discoveryNode : discoveryNodes) {
                ConnectionTarget connectionTarget = targetsByNode.get(discoveryNode);
                final boolean isNewNode = connectionTarget == null;
                if (isNewNode) {
                    connectionTarget = new ConnectionTarget(discoveryNode);
                    targetsByNode.put(discoveryNode, connectionTarget);
                }

                if (isNewNode) {
                    logger.debug("connecting to {}", discoveryNode);
                    runnables.add(
                        connectionTarget.connect(ActionListener.runAfter(listener, () -> logger.debug("connected to {}", discoveryNode)))
                    );
                } else {
                    // known node, try and ensure it's connected but do not wait
                    logger.trace("checking connection to existing node [{}]", discoveryNode);
                    runnables.add(connectionTarget.connect(null));
                    runnables.add(() -> listener.onResponse(null));
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
        synchronized (mutex) {
            final Collection<ConnectionTarget> connectionTargets = targetsByNode.values();
            if (connectionTargets.isEmpty()) {
                runnables.add(onCompletion);
            } else {
                logger.trace("ensureConnections: {}", targetsByNode);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                    ActionListener.wrap(onCompletion),
                    connectionTargets.size()
                );
                for (final ConnectionTarget connectionTarget : connectionTargets) {
                    runnables.add(connectionTarget.connect(listener));
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
                threadPool.scheduleUnlessShuttingDown(reconnectInterval, ThreadPool.Names.GENERIC, this);
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

        ConnectionTarget(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        private void setConnectionRef(Releasable connectionReleasable) {
            Releasables.close(connectionRef.getAndSet(connectionReleasable));
        }

        Runnable connect(ActionListener<Void> listener) {
            return () -> {
                final boolean alreadyConnected = transportService.nodeConnected(discoveryNode);

                if (alreadyConnected) {
                    logger.trace("refreshing connection to {}", discoveryNode);
                } else {
                    logger.debug("connecting to {}", discoveryNode);
                }

                // It's possible that connectionRef is a reference to an older connection that closed out from under us, but that something
                // else has opened a fresh connection to the node. Therefore we always call connectToNode() and update connectionRef.
                transportService.connectToNode(discoveryNode, new ActionListener<>() {
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
                        if (listener != null) {
                            listener.onResponse(null);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        final int currentFailureCount = consecutiveFailureCount.incrementAndGet();
                        // only warn every 6th failure
                        final Level level = currentFailureCount % 6 == 1 ? Level.WARN : Level.DEBUG;
                        logger.log(
                            level,
                            new ParameterizedMessage("failed to connect to {} (tried [{}] times)", discoveryNode, currentFailureCount),
                            e
                        );
                        setConnectionRef(null);
                        if (listener != null) {
                            listener.onFailure(e);
                        }
                    }
                });
            };
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
}
