/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse.Empty;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.AbstractTransportRequest;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.ReceiveTimeoutTransportException;
import org.elasticsearch.transport.TransportConnectionListener;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportRequestOptions.Type;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.common.util.concurrent.ConcurrentCollections.newConcurrentMap;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

/**
 * The FollowersChecker is responsible for allowing a leader to check that its followers are still connected and healthy. On deciding that a
 * follower has failed the leader will remove it from the cluster. We are fairly lenient, possibly allowing multiple checks to fail before
 * considering a follower to be faulty, to allow for a brief network partition or a long GC cycle to occur without triggering the removal of
 * a node and the consequent shard reallocation.
 */
public final class FollowersChecker {

    private static final Logger logger = LogManager.getLogger(FollowersChecker.class);

    public static final String FOLLOWER_CHECK_ACTION_NAME = "internal:coordination/fault_detection/follower_check";

    // the time between checks sent to each node
    public static final Setting<TimeValue> FOLLOWER_CHECK_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.fault_detection.follower_check.interval",
        TimeValue.timeValueMillis(1000),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    // the timeout for each check sent to each node
    public static final Setting<TimeValue> FOLLOWER_CHECK_TIMEOUT_SETTING = Setting.timeSetting(
        "cluster.fault_detection.follower_check.timeout",
        TimeValue.timeValueMillis(10000),
        TimeValue.timeValueMillis(1),
        Setting.Property.NodeScope
    );

    // the number of failed checks that must happen before the follower is considered to have failed.
    public static final Setting<Integer> FOLLOWER_CHECK_RETRY_COUNT_SETTING = Setting.intSetting(
        "cluster.fault_detection.follower_check.retry_count",
        3,
        1,
        Setting.Property.NodeScope
    );

    private final TimeValue followerCheckInterval;
    private final TimeValue followerCheckTimeout;
    private final int followerCheckRetryCount;
    private final BiConsumer<DiscoveryNode, String> onNodeFailure;
    private final Consumer<FollowerCheckRequest> handleRequestAndUpdateState;

    private final Object mutex = new Object(); // protects writes to this state; read access does not need sync
    private final Map<DiscoveryNode, FollowerChecker> followerCheckers = newConcurrentMap();
    private final Set<DiscoveryNode> faultyNodes = new HashSet<>();

    private final TransportService transportService;
    private final NodeHealthService nodeHealthService;
    private final Executor clusterCoordinationExecutor;
    private volatile FastResponseState fastResponseState;

    public FollowersChecker(
        Settings settings,
        TransportService transportService,
        Consumer<FollowerCheckRequest> handleRequestAndUpdateState,
        BiConsumer<DiscoveryNode, String> onNodeFailure,
        NodeHealthService nodeHealthService
    ) {
        this.transportService = transportService;
        this.handleRequestAndUpdateState = handleRequestAndUpdateState;
        this.onNodeFailure = onNodeFailure;
        this.nodeHealthService = nodeHealthService;
        this.clusterCoordinationExecutor = transportService.getThreadPool().executor(Names.CLUSTER_COORDINATION);

        followerCheckInterval = FOLLOWER_CHECK_INTERVAL_SETTING.get(settings);
        followerCheckTimeout = FOLLOWER_CHECK_TIMEOUT_SETTING.get(settings);
        followerCheckRetryCount = FOLLOWER_CHECK_RETRY_COUNT_SETTING.get(settings);

        updateFastResponseState(0, Mode.CANDIDATE);
        transportService.registerRequestHandler(
            FOLLOWER_CHECK_ACTION_NAME,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            false,
            false,
            FollowerCheckRequest::new,
            (request, transportChannel, task) -> handleFollowerCheck(
                request,
                new ChannelActionListener<>(transportChannel).map(ignored -> Empty.INSTANCE)
            )
        );
        transportService.addConnectionListener(new TransportConnectionListener() {
            @Override
            public void onNodeDisconnected(DiscoveryNode node, @Nullable Exception closeException) {
                handleDisconnectedNode(node);
            }
        });
    }

    /**
     * Update the set of known nodes, starting to check any new ones and stopping checking any previously-known-but-now-unknown ones.
     */
    public void setCurrentNodes(DiscoveryNodes discoveryNodes) {
        synchronized (mutex) {
            final Predicate<DiscoveryNode> isUnknownNode = n -> discoveryNodes.nodeExists(n) == false;
            followerCheckers.keySet().removeIf(isUnknownNode);
            faultyNodes.removeIf(isUnknownNode);

            discoveryNodes.mastersFirstStream().forEach(discoveryNode -> {
                if (discoveryNode.equals(discoveryNodes.getLocalNode()) == false
                    && followerCheckers.containsKey(discoveryNode) == false
                    && faultyNodes.contains(discoveryNode) == false) {

                    final FollowerChecker followerChecker = new FollowerChecker(discoveryNode);
                    followerCheckers.put(discoveryNode, followerChecker);
                    followerChecker.start();
                }
            });
        }
    }

    /**
     * Clear the set of known nodes, stopping all checks.
     */
    public void clearCurrentNodes() {
        setCurrentNodes(DiscoveryNodes.EMPTY_NODES);
    }

    /**
     * The system is normally in a state in which every follower remains a follower of a stable leader in a single term for an extended
     * period of time, and therefore our response to every follower check is the same. We handle this case with a single volatile read
     * entirely on the network thread, and only if the fast path fails do we perform some work in the background, by notifying the
     * FollowersChecker whenever our term or mode changes here.
     */
    public void updateFastResponseState(final long term, final Mode mode) {
        fastResponseState = new FastResponseState(term, mode);
    }

    private void handleFollowerCheck(FollowerCheckRequest request, ActionListener<Void> listener) {
        final StatusInfo statusInfo = nodeHealthService.getHealth();
        if (statusInfo.getStatus() == UNHEALTHY) {
            final String message = "handleFollowerCheck: node is unhealthy ["
                + statusInfo.getInfo()
                + "], rejecting "
                + statusInfo.getInfo();
            logger.debug(message);
            throw new NodeHealthCheckFailureException(message);
        }

        final FastResponseState responder = this.fastResponseState;
        if (responder.mode == Mode.FOLLOWER && responder.term == request.term) {
            logger.trace("responding to {} on fast path", request);
            listener.onResponse(null);
            return;
        }

        if (request.term < responder.term) {
            throw new CoordinationStateRejectedException("rejecting " + request + " since local state is " + this);
        }

        clusterCoordinationExecutor.execute(ActionRunnable.run(listener, new CheckedRunnable<>() {
            @Override
            public void run() {
                logger.trace("responding to {} on slow path", request);
                handleRequestAndUpdateState.accept(request);
            }

            @Override
            public String toString() {
                return "responding to [" + request + "] on slow path";
            }
        }));
    }

    /**
     * @return nodes in the current cluster state which have failed their follower checks.
     */
    public Set<DiscoveryNode> getFaultyNodes() {
        synchronized (mutex) {
            return new HashSet<>(this.faultyNodes);
        }
    }

    @Override
    public String toString() {
        return "FollowersChecker{"
            + "followerCheckInterval="
            + followerCheckInterval
            + ", followerCheckTimeout="
            + followerCheckTimeout
            + ", followerCheckRetryCount="
            + followerCheckRetryCount
            + ", followerCheckers="
            + followerCheckers
            + ", faultyNodes="
            + faultyNodes
            + ", fastResponseState="
            + fastResponseState
            + '}';
    }

    // For assertions
    FastResponseState getFastResponseState() {
        return fastResponseState;
    }

    // For assertions
    Set<DiscoveryNode> getKnownFollowers() {
        synchronized (mutex) {
            final Set<DiscoveryNode> knownFollowers = new HashSet<>(faultyNodes);
            knownFollowers.addAll(followerCheckers.keySet());
            return knownFollowers;
        }
    }

    private void handleDisconnectedNode(DiscoveryNode discoveryNode) {
        FollowerChecker followerChecker = followerCheckers.get(discoveryNode);
        if (followerChecker != null) {
            followerChecker.failNode("disconnected");
        }
    }

    record FastResponseState(long term, Mode mode) {}

    /**
     * A checker for an individual follower.
     */
    private class FollowerChecker {
        private final DiscoveryNode discoveryNode;
        private int failureCountSinceLastSuccess;
        private int timeoutCountSinceLastSuccess;

        FollowerChecker(DiscoveryNode discoveryNode) {
            this.discoveryNode = discoveryNode;
        }

        private boolean running() {
            return this == followerCheckers.get(discoveryNode);
        }

        void start() {
            assert running();
            handleWakeUp();
        }

        private void handleWakeUp() {
            if (running() == false) {
                logger.trace("handleWakeUp: not running");
                return;
            }

            final FollowerCheckRequest request = new FollowerCheckRequest(fastResponseState.term, transportService.getLocalNode());
            logger.trace("handleWakeUp: checking {} with {}", discoveryNode, request);

            transportService.sendRequest(
                discoveryNode,
                FOLLOWER_CHECK_ACTION_NAME,
                request,
                TransportRequestOptions.of(followerCheckTimeout, Type.PING),
                new TransportResponseHandler.Empty() {

                    @Override
                    public Executor executor() {
                        return TransportResponseHandler.TRANSPORT_WORKER;
                    }

                    @Override
                    public void handleResponse() {
                        if (running() == false) {
                            logger.trace("{} no longer running", FollowerChecker.this);
                            return;
                        }

                        failureCountSinceLastSuccess = 0;
                        timeoutCountSinceLastSuccess = 0;
                        logger.trace("{} check successful", FollowerChecker.this);
                        scheduleNextWakeUp();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        if (running() == false) {
                            logger.debug(() -> format("%s no longer running", FollowerChecker.this), exp);
                            return;
                        }

                        if (exp instanceof ReceiveTimeoutTransportException) {
                            timeoutCountSinceLastSuccess++;
                        } else {
                            failureCountSinceLastSuccess++;
                        }

                        final String reason;
                        if (exp instanceof ConnectTransportException || exp.getCause() instanceof ConnectTransportException) {
                            logger.debug(() -> format("%s disconnected", FollowerChecker.this), exp);
                            reason = "disconnected";
                        } else if (exp.getCause() instanceof NodeHealthCheckFailureException) {
                            logger.debug(() -> format("%s health check failed", FollowerChecker.this), exp);
                            reason = "health check failed";
                        } else if (failureCountSinceLastSuccess + timeoutCountSinceLastSuccess >= followerCheckRetryCount) {
                            logger.debug(() -> format("%s failed too many times", FollowerChecker.this), exp);
                            reason = "followers check retry count exceeded [timeouts="
                                + timeoutCountSinceLastSuccess
                                + ", failures="
                                + failureCountSinceLastSuccess
                                + "]";
                        } else {
                            logger.debug(() -> format("%s failed, retrying", FollowerChecker.this), exp);
                            scheduleNextWakeUp();
                            return;
                        }

                        failNode(reason);
                    }
                }
            );
        }

        void failNode(String reason) {
            clusterCoordinationExecutor.execute(new AbstractRunnable() {

                @Override
                public void onRejection(Exception e) {
                    logger.debug(() -> format("rejected task to fail node [%s] with reason [%s]", discoveryNode, reason), e);
                    if (e instanceof EsRejectedExecutionException esRejectedExecutionException) {
                        assert esRejectedExecutionException.isExecutorShutdown();
                    } else {
                        assert false : e;
                    }
                }

                @Override
                protected void doRun() {
                    synchronized (mutex) {
                        if (running() == false) {
                            logger.trace("{} no longer running, not marking faulty", FollowerChecker.this);
                            return;
                        }
                        logger.debug("{} marking node as faulty", FollowerChecker.this);
                        faultyNodes.add(discoveryNode);
                        followerCheckers.remove(discoveryNode);
                    }
                    onNodeFailure.accept(discoveryNode, reason);
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : e;
                    logger.error(() -> format("unexpected failure when failing node [%s] with reason [%s]", discoveryNode, reason), e);
                }

                @Override
                public String toString() {
                    return "detected failure of " + discoveryNode;
                }
            });
        }

        private void scheduleNextWakeUp() {
            transportService.getThreadPool()
                .scheduleUnlessShuttingDown(followerCheckInterval, EsExecutors.DIRECT_EXECUTOR_SERVICE, new Runnable() {
                    @Override
                    public void run() {
                        handleWakeUp();
                    }

                    @Override
                    public String toString() {
                        return FollowerChecker.this + "::handleWakeUp";
                    }
                });
        }

        @Override
        public String toString() {
            return "FollowerChecker{"
                + "discoveryNode="
                + discoveryNode
                + ", failureCountSinceLastSuccess="
                + failureCountSinceLastSuccess
                + ", timeoutCountSinceLastSuccess="
                + timeoutCountSinceLastSuccess
                + ", ["
                + FOLLOWER_CHECK_RETRY_COUNT_SETTING.getKey()
                + "]="
                + followerCheckRetryCount
                + '}';
        }
    }

    public static class FollowerCheckRequest extends AbstractTransportRequest {

        private final long term;

        private final DiscoveryNode sender;

        public long getTerm() {
            return term;
        }

        public DiscoveryNode getSender() {
            return sender;
        }

        public FollowerCheckRequest(final long term, final DiscoveryNode sender) {
            this.term = term;
            this.sender = sender;
        }

        public FollowerCheckRequest(final StreamInput in) throws IOException {
            super(in);
            term = in.readLong();
            sender = new DiscoveryNode(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeLong(term);
            sender.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FollowerCheckRequest that = (FollowerCheckRequest) o;
            return term == that.term && Objects.equals(sender, that.sender);
        }

        @Override
        public String toString() {
            return "FollowerCheckRequest{" + "term=" + term + ", sender=" + sender + '}';
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, sender);
        }
    }
}
