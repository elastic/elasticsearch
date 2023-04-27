/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.ObjLongConsumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class JoinHelper {

    private static final Logger logger = LogManager.getLogger(JoinHelper.class);

    public static final String START_JOIN_ACTION_NAME = "internal:cluster/coordination/start_join";
    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";
    public static final String JOIN_PING_ACTION_NAME = "internal:cluster/coordination/join/ping";

    private final ClusterApplier clusterApplier;
    private final TransportService transportService;
    private final MasterServiceTaskQueue<JoinTask> joinTaskQueue;
    private final LongSupplier currentTermSupplier;
    private final NodeHealthService nodeHealthService;
    private final JoinReasonService joinReasonService;
    private final CircuitBreakerService circuitBreakerService;
    private final ObjLongConsumer<ActionListener<ClusterState>> latestStoredStateSupplier;

    private final Map<Tuple<DiscoveryNode, JoinRequest>, PendingJoinInfo> pendingOutgoingJoins = ConcurrentCollections.newConcurrentMap();
    private final AtomicReference<FailedJoinAttempt> lastFailedJoinAttempt = new AtomicReference<>();
    private final Map<DiscoveryNode, Releasable> joinConnections = new HashMap<>(); // synchronized on itself

    JoinHelper(
        AllocationService allocationService,
        MasterService masterService,
        ClusterApplier clusterApplier,
        TransportService transportService,
        LongSupplier currentTermSupplier,
        BiConsumer<JoinRequest, ActionListener<Void>> joinHandler,
        Function<StartJoinRequest, Join> joinLeaderInTerm,
        RerouteService rerouteService,
        NodeHealthService nodeHealthService,
        JoinReasonService joinReasonService,
        CircuitBreakerService circuitBreakerService,
        Function<ClusterState, ClusterState> maybeReconfigureAfterMasterElection,
        ObjLongConsumer<ActionListener<ClusterState>> latestStoredStateSupplier
    ) {
        this.joinTaskQueue = masterService.createTaskQueue(
            "node-join",
            Priority.URGENT,
            new NodeJoinExecutor(allocationService, rerouteService, maybeReconfigureAfterMasterElection)
        );
        this.clusterApplier = clusterApplier;
        this.transportService = transportService;
        this.circuitBreakerService = circuitBreakerService;
        this.currentTermSupplier = currentTermSupplier;
        this.nodeHealthService = nodeHealthService;
        this.joinReasonService = joinReasonService;
        this.latestStoredStateSupplier = latestStoredStateSupplier;

        transportService.registerRequestHandler(
            JOIN_ACTION_NAME,
            Names.CLUSTER_COORDINATION,
            false,
            false,
            JoinRequest::new,
            (request, channel, task) -> joinHandler.accept(
                request,
                new ChannelActionListener<Empty>(channel).map(ignored -> Empty.INSTANCE)
            )
        );

        transportService.registerRequestHandler(
            START_JOIN_ACTION_NAME,
            Names.CLUSTER_COORDINATION,
            false,
            false,
            StartJoinRequest::new,
            (request, channel, task) -> {
                final DiscoveryNode destination = request.getSourceNode();
                sendJoinRequest(destination, currentTermSupplier.getAsLong(), Optional.of(joinLeaderInTerm.apply(request)));
                channel.sendResponse(Empty.INSTANCE);
            }
        );

        transportService.registerRequestHandler(
            JOIN_PING_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            TransportRequest.Empty::new,
            (request, channel, task) -> channel.sendResponse(Empty.INSTANCE)
        );
    }

    boolean isJoinPending() {
        return pendingOutgoingJoins.isEmpty() == false;
    }

    public void onClusterStateApplied() {
        // we applied a cluster state as LEADER or FOLLOWER which means the NodeConnectionsService has taken ownership of any connections to
        // nodes in the cluster and therefore we can release the connection(s) that we were using for joining
        final List<Releasable> releasables;
        synchronized (joinConnections) {
            if (joinConnections.isEmpty()) {
                return;
            }
            releasables = new ArrayList<>(joinConnections.values());
            joinConnections.clear();
        }

        logger.debug("releasing [{}] connections on successful cluster state application", releasables.size());
        releasables.forEach(Releasables::close);
    }

    private void registerConnection(DiscoveryNode destination, Releasable connectionReference) {
        final Releasable previousConnection;
        synchronized (joinConnections) {
            previousConnection = joinConnections.put(destination, connectionReference);
        }
        Releasables.close(previousConnection);
    }

    private void unregisterAndReleaseConnection(DiscoveryNode destination, Releasable connectionReference) {
        synchronized (joinConnections) {
            joinConnections.remove(destination, connectionReference);
        }
        Releasables.close(connectionReference);
    }

    // package-private for testing
    static class FailedJoinAttempt {
        private final DiscoveryNode destination;
        private final JoinRequest joinRequest;
        private final ElasticsearchException exception;
        private final long timestamp;

        FailedJoinAttempt(DiscoveryNode destination, JoinRequest joinRequest, ElasticsearchException exception) {
            this.destination = destination;
            this.joinRequest = joinRequest;
            this.exception = exception;
            this.timestamp = System.nanoTime();
        }

        void logNow() {
            logger.log(getLogLevel(exception), () -> format("failed to join %s with %s", destination, joinRequest), exception);
        }

        static Level getLogLevel(ElasticsearchException e) {
            Throwable cause = e.unwrapCause();
            if (cause instanceof CoordinationStateRejectedException
                || cause instanceof CircuitBreakingException
                || (cause instanceof Exception causeException && MasterService.isPublishFailureException(causeException))) {
                return Level.DEBUG;
            }
            return Level.INFO;
        }

        void logWarnWithTimestamp() {
            logger.warn(
                () -> format(
                    "last failed join attempt was %s ago, failed to join %s with %s",
                    TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - timestamp)),
                    destination,
                    joinRequest
                ),
                exception
            );
        }
    }

    void logLastFailedJoinAttempt() {
        FailedJoinAttempt attempt = lastFailedJoinAttempt.get();
        if (attempt != null) {
            attempt.logWarnWithTimestamp();
            lastFailedJoinAttempt.compareAndSet(attempt, null);
        }
    }

    public void sendJoinRequest(DiscoveryNode destination, long term, Optional<Join> optionalJoin) {
        assert destination.isMasterNode() : "trying to join master-ineligible " + destination;
        final StatusInfo statusInfo = nodeHealthService.getHealth();
        if (statusInfo.getStatus() == UNHEALTHY) {
            logger.debug("dropping join request to [{}]: [{}]", destination, statusInfo.getInfo());
            return;
        }
        final JoinRequest joinRequest = new JoinRequest(transportService.getLocalNode(), TransportVersion.CURRENT, term, optionalJoin);
        final Tuple<DiscoveryNode, JoinRequest> dedupKey = Tuple.tuple(destination, joinRequest);
        final var pendingJoinInfo = new PendingJoinInfo(transportService.getThreadPool().relativeTimeInMillis());
        if (pendingOutgoingJoins.putIfAbsent(dedupKey, pendingJoinInfo) == null) {

            // If this node is under excessive heap pressure then the state it receives for join validation will trip a circuit breaker and
            // fail the join attempt, resulting in retrying in a loop which makes the master just send a constant stream of cluster states
            // to this node. We try and keep the problem local to this node by checking that we can at least allocate one byte:
            final var breaker = circuitBreakerService.getBreaker(CircuitBreaker.IN_FLIGHT_REQUESTS);
            try {
                breaker.addEstimateBytesAndMaybeBreak(1L, "pre-flight join request");
            } catch (Exception e) {
                pendingJoinInfo.message = PENDING_JOIN_FAILED;
                pendingOutgoingJoins.remove(dedupKey);
                if (e instanceof ElasticsearchException elasticsearchException) {
                    final var attempt = new FailedJoinAttempt(destination, joinRequest, elasticsearchException);
                    attempt.logNow();
                    lastFailedJoinAttempt.set(attempt);
                    assert elasticsearchException instanceof CircuitBreakingException : e; // others shouldn't happen, handle them anyway
                } else {
                    logger.error("join failed during pre-flight circuit breaker check", e);
                    assert false : e; // shouldn't happen, handle it anyway
                }
                return;
            }
            breaker.addWithoutBreaking(-1L);

            logger.debug("attempting to join {} with {}", destination, joinRequest);
            pendingJoinInfo.message = PENDING_JOIN_CONNECTING;
            // Typically we're already connected to the destination at this point, the PeerFinder holds a reference to this connection to
            // keep it open, but we need to acquire our own reference to keep the connection alive through the joining process.
            transportService.connectToNode(destination, new ActionListener<>() {
                @Override
                public void onResponse(Releasable connectionReference) {
                    logger.trace("acquired connection for joining join {} with {}", destination, joinRequest);

                    // Register the connection in joinConnections so it can be released once we successfully apply the cluster state, at
                    // which point the NodeConnectionsService will have taken ownership of it.
                    registerConnection(destination, connectionReference);

                    // It's possible that our cluster applier is still applying an earlier cluster state (maybe stuck waiting on IO), in
                    // which case the master will accept our join and add us to the cluster but we won't be able to apply the joining state
                    // fast enough and will be kicked out of the cluster for lagging, which can happen repeatedly and be a little
                    // disruptive. To avoid this we send the join from the applier thread which ensures that it's not busy doing something
                    // else.
                    pendingJoinInfo.message = PENDING_JOIN_WAITING_APPLIER;
                    clusterApplier.onNewClusterState(
                        "joining " + destination.descriptionWithoutAttributes(),
                        () -> null,
                        new ActionListener<>() {
                            @Override
                            public void onResponse(Void unused) {
                                assert ThreadPool.assertCurrentThreadPool(ClusterApplierService.CLUSTER_UPDATE_THREAD_NAME);
                                pendingJoinInfo.message = PENDING_JOIN_WAITING_RESPONSE;
                                transportService.sendRequest(
                                    destination,
                                    JOIN_ACTION_NAME,
                                    joinRequest,
                                    TransportRequestOptions.of(null, TransportRequestOptions.Type.PING),
                                    new TransportResponseHandler.Empty() {
                                        @Override
                                        public void handleResponse(TransportResponse.Empty response) {
                                            pendingJoinInfo.message = PENDING_JOIN_WAITING_STATE; // only logged if state delayed
                                            pendingOutgoingJoins.remove(dedupKey);
                                            logger.debug("successfully joined {} with {}", destination, joinRequest);
                                            lastFailedJoinAttempt.set(null);
                                        }

                                        @Override
                                        public void handleException(TransportException exp) {
                                            cleanUpOnFailure(exp);
                                        }
                                    }
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                assert false : e; // no-op cluster state update cannot fail
                                cleanUpOnFailure(new TransportException(e));
                            }

                            private void cleanUpOnFailure(TransportException exp) {
                                pendingJoinInfo.message = PENDING_JOIN_FAILED;
                                pendingOutgoingJoins.remove(dedupKey);
                                final var attempt = new FailedJoinAttempt(destination, joinRequest, exp);
                                attempt.logNow();
                                lastFailedJoinAttempt.set(attempt);
                                unregisterAndReleaseConnection(destination, connectionReference);
                            }
                        }
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    pendingJoinInfo.message = PENDING_JOIN_CONNECT_FAILED;
                    pendingOutgoingJoins.remove(dedupKey);
                    final var attempt = new FailedJoinAttempt(
                        destination,
                        joinRequest,
                        new ConnectTransportException(destination, "failed to acquire connection", e)
                    );
                    attempt.logNow();
                    lastFailedJoinAttempt.set(attempt);
                }
            });

        } else {
            logger.debug("already attempting to join {} with request {}, not sending request", destination, joinRequest);
        }
    }

    void sendStartJoinRequest(final StartJoinRequest startJoinRequest, final DiscoveryNode destination) {
        assert startJoinRequest.getSourceNode().isMasterNode()
            : "sending start-join request for master-ineligible " + startJoinRequest.getSourceNode();
        transportService.sendRequest(destination, START_JOIN_ACTION_NAME, startJoinRequest, new TransportResponseHandler.Empty() {
            @Override
            public void handleResponse(TransportResponse.Empty response) {
                logger.debug("successful response to {} from {}", startJoinRequest, destination);
            }

            @Override
            public void handleException(TransportException exp) {
                logger.debug(() -> format("failure in response to %s from %s", startJoinRequest, destination), exp);
            }
        });
    }

    List<JoinStatus> getInFlightJoinStatuses() {
        final var currentTime = transportService.getThreadPool().relativeTimeInMillis();
        final var result = new ArrayList<JoinStatus>(pendingOutgoingJoins.size());
        var maxTerm = Long.MIN_VALUE;
        for (final var entry : pendingOutgoingJoins.entrySet()) {
            final var nodeAndJoinRequest = entry.getKey();
            final var term = nodeAndJoinRequest.v2().getTerm();
            if (maxTerm < term) {
                result.clear();
                maxTerm = term;
            }
            if (term == maxTerm) {
                final var pendingJoinInfo = entry.getValue();
                result.add(
                    new JoinStatus(
                        nodeAndJoinRequest.v1(),
                        term,
                        pendingJoinInfo.message,
                        TimeValue.timeValueMillis(currentTime - pendingJoinInfo.startTimeMillis)
                    )
                );
            }
        }
        return result;
    }

    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, TransportVersion transportVersion, ActionListener<Void> joinListener);

        default void close(Mode newMode) {}
    }

    class LeaderJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, TransportVersion transportVersion, ActionListener<Void> joinListener) {
            final JoinTask task = JoinTask.singleNode(
                sender,
                transportVersion,
                joinReasonService.getJoinReason(sender, Mode.LEADER),
                joinListener,
                currentTermSupplier.getAsLong()
            );
            joinTaskQueue.submitTask("node-join", task, null);
        }

        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    static class InitialJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, TransportVersion transportVersion, ActionListener<Void> joinListener) {
            assert false : "unexpected join from " + sender + " during initialisation";
            joinListener.onFailure(new CoordinationStateRejectedException("join target is not initialised yet"));
        }

        @Override
        public String toString() {
            return "InitialJoinAccumulator";
        }
    }

    static class FollowerJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, TransportVersion transportVersion, ActionListener<Void> joinListener) {
            joinListener.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }

    class CandidateJoinAccumulator implements JoinAccumulator {

        private final Map<DiscoveryNode, Tuple<TransportVersion, ActionListener<Void>>> joinRequestAccumulator = new HashMap<>();
        boolean closed;

        @Override
        public void handleJoinRequest(DiscoveryNode sender, TransportVersion transportVersion, ActionListener<Void> joinListener) {
            assert closed == false : "CandidateJoinAccumulator closed";
            var prev = joinRequestAccumulator.put(sender, Tuple.tuple(transportVersion, joinListener));
            if (prev != null) {
                prev.v2().onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        @Override
        public void close(Mode newMode) {
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            if (newMode == Mode.LEADER) {
                final var joiningTerm = currentTermSupplier.getAsLong();
                final JoinTask joinTask = JoinTask.completingElection(joinRequestAccumulator.entrySet().stream().map(entry -> {
                    final DiscoveryNode discoveryNode = entry.getKey();
                    final var data = entry.getValue();
                    return new JoinTask.NodeJoinTask(
                        discoveryNode,
                        data.v1(),
                        joinReasonService.getJoinReason(discoveryNode, Mode.CANDIDATE),
                        data.v2()
                    );
                }), joiningTerm);
                latestStoredStateSupplier.accept(new ActionListener<>() {
                    @Override
                    public void onResponse(ClusterState latestStoredClusterState) {
                        joinTaskQueue.submitTask(
                            "elected-as-master ([" + joinTask.nodeCount() + "] nodes joined in term " + joiningTerm + ")",
                            joinTask.alsoRefreshState(latestStoredClusterState),
                            null
                        );
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            Strings.format("failed to retrieve latest stored state after winning election in term [%d]", joiningTerm),
                            e
                        );
                        joinRequestAccumulator.values().forEach(joinCallback -> joinCallback.v2().onFailure(e));
                    }
                }, joiningTerm);
            } else {
                assert newMode == Mode.FOLLOWER : newMode;
                joinRequestAccumulator.values()
                    .forEach(joinCallback -> joinCallback.v2().onFailure(new CoordinationStateRejectedException("became follower")));
            }

            // CandidateJoinAccumulator is only closed when becoming leader or follower, otherwise it accumulates all joins received
            // regardless of term.
        }

        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() + ", closed=" + closed + '}';
        }
    }

    private static class PendingJoinInfo {
        final long startTimeMillis;
        volatile String message = PENDING_JOIN_INITIALIZING;

        PendingJoinInfo(long startTimeMillis) {
            this.startTimeMillis = startTimeMillis;
        }
    }

    static final String PENDING_JOIN_INITIALIZING = "initializing";
    static final String PENDING_JOIN_CONNECTING = "waiting to connect";
    static final String PENDING_JOIN_WAITING_APPLIER = "waiting for local cluster applier";
    static final String PENDING_JOIN_WAITING_RESPONSE = "waiting for response";
    static final String PENDING_JOIN_WAITING_STATE = "waiting to receive cluster state";
    static final String PENDING_JOIN_CONNECT_FAILED = "failed to connect";
    static final String PENDING_JOIN_FAILED = "failed";
}
