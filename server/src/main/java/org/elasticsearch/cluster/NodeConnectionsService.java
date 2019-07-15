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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.cluster.coordination.FollowersChecker;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
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

    public static final Setting<TimeValue> CLUSTER_NODE_RECONNECT_INTERVAL_SETTING =
        positiveTimeSetting("cluster.nodes.reconnect_interval", TimeValue.timeValueSeconds(10), Property.NodeScope);

    private final ThreadPool threadPool;
    private final TransportService transportService;

    // Protects changes to targetsByNode and its values (i.e. ConnectionTarget#activityType and ConnectionTarget#listener).
    // Crucially there are no blocking calls under this mutex: it is not held while connecting or disconnecting.
    private final Object mutex = new Object();

    // contains an entry for every node in the latest cluster state, as well as for nodes from which we are in the process of
    // disconnecting
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
     * connection attempts to _new_ nodes, but not on attempts to re-establish connections to nodes that are already known.
     */
    public void connectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {

        if (discoveryNodes.getSize() == 0) {
            onCompletion.run();
            return;
        }

        final GroupedActionListener<Void> listener
            = new GroupedActionListener<>(ActionListener.wrap(onCompletion), discoveryNodes.getSize());

        final List<Runnable> runnables = new ArrayList<>(discoveryNodes.getSize());
        synchronized (mutex) {
            for (final DiscoveryNode discoveryNode : discoveryNodes) {
                ConnectionTarget connectionTarget = targetsByNode.get(discoveryNode);
                final boolean isNewNode;
                if (connectionTarget == null) {
                    // new node, set up target and listener
                    connectionTarget = new ConnectionTarget(discoveryNode);
                    targetsByNode.put(discoveryNode, connectionTarget);
                    isNewNode = true;
                } else {
                    // existing node, but maybe we're disconnecting from it, in which case it was recently removed from the cluster
                    // state and has now been re-added so we should wait for the re-connection
                    isNewNode = connectionTarget.isPendingDisconnection();
                }

                if (isNewNode) {
                    runnables.add(connectionTarget.connect(listener));
                } else {
                    // known node, try and ensure it's connected but do not wait
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
                runnables.add(targetsByNode.get(discoveryNode).disconnect());
            }
        }
        runnables.forEach(Runnable::run);
    }

    void ensureConnections(Runnable onCompletion) {
        // Called by tests after some disruption has concluded. It is possible that one or more targets are currently CONNECTING and have
        // been since the disruption was active, and that the connection attempt was thwarted by a concurrent disruption to the connection.
        // If so, we cannot simply add our listener to the queue because it will be notified when this CONNECTING activity completes even
        // though it was disrupted. We must therefore wait for all the current activity to finish and then go through and reconnect to
        // any missing nodes.
        awaitPendingActivity(() -> connectDisconnectedTargets(onCompletion));
    }

    private void awaitPendingActivity(Runnable onCompletion) {
        final List<Runnable> runnables = new ArrayList<>();
        synchronized (mutex) {
            final Collection<ConnectionTarget> connectionTargets = targetsByNode.values();
            if (connectionTargets.isEmpty()) {
                runnables.add(onCompletion);
            } else {
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                    ActionListener.wrap(onCompletion), connectionTargets.size());
                for (final ConnectionTarget connectionTarget : connectionTargets) {
                    runnables.add(connectionTarget.awaitCurrentActivity(listener));
                }
            }
        }
        runnables.forEach(Runnable::run);
    }

    /**
     * Makes a single attempt to reconnect to any nodes which are disconnected but should be connected. Does not attempt to reconnect any
     * nodes which are in the process of disconnecting. The onCompletion handler is called after all ongoing connection/disconnection
     * attempts have completed.
     */
    private void connectDisconnectedTargets(Runnable onCompletion) {
        final List<Runnable> runnables = new ArrayList<>();
        synchronized (mutex) {
            final Collection<ConnectionTarget> connectionTargets = targetsByNode.values();
            if (connectionTargets.isEmpty()) {
                runnables.add(onCompletion);
            } else {
                logger.trace("connectDisconnectedTargets: {}", targetsByNode);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                    ActionListener.wrap(onCompletion), connectionTargets.size());
                for (final ConnectionTarget connectionTarget : connectionTargets) {
                    runnables.add(connectionTarget.ensureConnected(listener));
                }
            }
        }
        runnables.forEach(Runnable::run);
    }

    class ConnectionChecker extends AbstractRunnable {
        protected void doRun() {
            if (connectionChecker == this) {
                connectDisconnectedTargets(this::scheduleNextCheck);
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
    protected void doClose() {
    }

    // for disruption tests, re-establish any disrupted connections
    public void reconnectToNodes(DiscoveryNodes discoveryNodes, Runnable onCompletion) {
        connectToNodes(discoveryNodes, () -> {
            disconnectFromNodesExcept(discoveryNodes);
            ensureConnections(onCompletion);
        });
    }

    private enum ActivityType {
        IDLE,
        CONNECTING,
        DISCONNECTING
    }

    /**
     * {@link ConnectionTarget} ensures that we are never concurrently connecting to and disconnecting from a node, and that we eventually
     * either connect to or disconnect from it according to whether {@link ConnectionTarget#connect(ActionListener)} or
     * {@link ConnectionTarget#disconnect()} was called last.
     * <p>
     * Each {@link ConnectionTarget} is in one of these states:
     * <p>
     * - idle                       ({@link ConnectionTarget#future} has no listeners)
     * - awaiting connection        ({@link ConnectionTarget#future} may contain listeners awaiting a connection)
     * - awaiting disconnection     ({@link ConnectionTarget#future} may contain listeners awaiting a disconnection)
     * <p>
     * It will be awaiting connection (respectively disconnection) after calling {@code connect()} (respectively {@code disconnect()}). It
     * will eventually become idle if these methods are not called infinitely often.
     * <p>
     * These methods return a {@link Runnable} which starts the connection/disconnection process iff it was idle before the method was
     * called, and which notifies any failed listeners if the {@code ConnectionTarget} went from {@code CONNECTING} to {@code DISCONNECTING}
     * or vice versa. The connection/disconnection process continues until all listeners have been removed, at which point it becomes idle
     * again.
     * <p>
     * Additionally if the last step of the process was a disconnection then this target is removed from the current set of targets. Thus
     * if this {@link ConnectionTarget} is idle and in the current set of targets then it should be connected.
     * <p>
     * All of the {@code listeners} are awaiting the completion of the same activity, which is either a connection or a disconnection.  If
     * we are currently connecting and then {@link ConnectionTarget#disconnect()} is called then all connection listeners are
     * removed from the list so they can be notified of failure; once the connecting process has finished a disconnection will be started.
     * Similarly if we are currently disconnecting and then {@link ConnectionTarget#connect(ActionListener)} is called then all
     * disconnection listeners are immediately removed for failure notification and a connection is started once the disconnection is
     * complete.
     */
    private class ConnectionTarget {
        private final DiscoveryNode discoveryNode;

        private PlainListenableActionFuture<Void> future = PlainListenableActionFuture.newListenableFuture();
        private ActivityType activityType = ActivityType.IDLE; // indicates what any listeners are awaiting

        private final AtomicInteger consecutiveFailureCount = new AtomicInteger();

        private final Runnable connectActivity = new AbstractRunnable() {

            final AbstractRunnable abstractRunnable = this;

            @Override
            protected void doRun() {
                assert Thread.holdsLock(mutex) == false : "mutex unexpectedly held";
                transportService.connectToNode(discoveryNode, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void aVoid) {
                        assert Thread.holdsLock(mutex) == false : "mutex unexpectedly held";
                        consecutiveFailureCount.set(0);
                        logger.debug("connected to {}", discoveryNode);
                        onCompletion(ActivityType.CONNECTING, null, disconnectActivity);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        abstractRunnable.onFailure(e);
                    }
                });
            }

            @Override
            public void onFailure(Exception e) {
                assert Thread.holdsLock(mutex) == false : "mutex unexpectedly held";
                final int currentFailureCount = consecutiveFailureCount.incrementAndGet();
                // only warn every 6th failure
                final Level level = currentFailureCount % 6 == 1 ? Level.WARN : Level.DEBUG;
                logger.log(level, new ParameterizedMessage("failed to connect to {} (tried [{}] times)",
                    discoveryNode, currentFailureCount), e);
                onCompletion(ActivityType.CONNECTING, e, disconnectActivity);
            }

            @Override
            public String toString() {
                return "connect to " + discoveryNode;
            }
        };

        private final Runnable disconnectActivity = new AbstractRunnable() {
            @Override
            protected void doRun() {
                assert Thread.holdsLock(mutex) == false : "mutex unexpectedly held";
                transportService.disconnectFromNode(discoveryNode);
                consecutiveFailureCount.set(0);
                logger.debug("disconnected from {}", discoveryNode);
                onCompletion(ActivityType.DISCONNECTING, null, connectActivity);
            }

            @Override
            public void onFailure(Exception e) {
                assert Thread.holdsLock(mutex) == false : "mutex unexpectedly held";
                consecutiveFailureCount.incrementAndGet();
                // we may not have disconnected, but will not retry, so this connection might have leaked
                logger.warn(new ParameterizedMessage("failed to disconnect from {}, possible connection leak", discoveryNode), e);
                assert false : "failed to disconnect from " + discoveryNode + ", possible connection leak\n" + e;
                onCompletion(ActivityType.DISCONNECTING, e, connectActivity);
            }
        };

        ConnectionTarget(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        Runnable connect(@Nullable ActionListener<Void> listener) {
            return addListenerAndStartActivity(listener, ActivityType.CONNECTING, connectActivity,
                "disconnection cancelled by reconnection");
        }

        Runnable disconnect() {
            return addListenerAndStartActivity(null, ActivityType.DISCONNECTING, disconnectActivity,
                "connection cancelled by disconnection");
        }

        Runnable ensureConnected(@Nullable ActionListener<Void> listener) {
            assert Thread.holdsLock(mutex) : "mutex not held";

            if (activityType == ActivityType.IDLE) {
                if (transportService.nodeConnected(discoveryNode)) {
                    return () -> listener.onResponse(null);
                } else {
                    // target is disconnected, and we are currently idle, so start a connection process.
                    activityType = ActivityType.CONNECTING;
                    addListener(listener);
                    return connectActivity;
                }
            } else {
                addListener(listener);
                return () -> {
                };
            }
        }

        Runnable awaitCurrentActivity(ActionListener<Void> listener) {
            assert Thread.holdsLock(mutex) : "mutex not held";

            if (activityType == ActivityType.IDLE) {
                return () -> listener.onResponse(null);
            } else {
                addListener(listener);
                return () -> {
                };
            }
        }

        private void addListener(@Nullable ActionListener<Void> listener) {
            assert Thread.holdsLock(mutex) : "mutex not held";
            assert activityType != ActivityType.IDLE;
            if (listener != null) {
                future.addListener(listener);
            }
        }

        private PlainListenableActionFuture<Void> getAndClearFuture() {
            assert Thread.holdsLock(mutex) : "mutex not held";
            final PlainListenableActionFuture<Void> drainedFuture = future;
            future = PlainListenableActionFuture.newListenableFuture();
            return drainedFuture;
        }

        private Runnable addListenerAndStartActivity(@Nullable ActionListener<Void> listener, ActivityType newActivityType,
                                                     Runnable activity, String cancellationMessage) {
            assert Thread.holdsLock(mutex) : "mutex not held";
            assert newActivityType.equals(ActivityType.IDLE) == false;

            if (activityType == ActivityType.IDLE) {
                activityType = newActivityType;
                addListener(listener);
                return activity;
            }

            if (activityType == newActivityType) {
                addListener(listener);
                return () -> {
                };
            }

            activityType = newActivityType;
            final PlainListenableActionFuture<Void> oldFuture = getAndClearFuture();
            addListener(listener);
            return () -> oldFuture.onFailure(new ElasticsearchException(cancellationMessage));
        }

        private void onCompletion(ActivityType completedActivityType, @Nullable Exception e, Runnable oppositeActivity) {
            assert Thread.holdsLock(mutex) == false : "mutex unexpectedly held";

            final Runnable cleanup;
            synchronized (mutex) {
                assert activityType != ActivityType.IDLE;
                if (activityType == completedActivityType) {
                    final PlainListenableActionFuture<Void> oldFuture = getAndClearFuture();
                    activityType = ActivityType.IDLE;

                    cleanup = e == null ? () -> oldFuture.onResponse(null) : () -> oldFuture.onFailure(e);

                    if (completedActivityType.equals(ActivityType.DISCONNECTING)) {
                        final ConnectionTarget removedTarget = targetsByNode.remove(discoveryNode);
                        assert removedTarget == this : removedTarget + " vs " + this;
                    }
                } else {
                    cleanup = oppositeActivity;
                }
            }
            cleanup.run();
        }

        boolean isPendingDisconnection() {
            assert Thread.holdsLock(mutex) : "mutex not held";
            return activityType == ActivityType.DISCONNECTING;
        }

        @Override
        public String toString() {
            synchronized (mutex) {
                return "ConnectionTarget{" +
                    "discoveryNode=" + discoveryNode +
                    ", activityType=" + activityType +
                    '}';
            }
        }
    }
}
