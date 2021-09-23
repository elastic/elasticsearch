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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;

public class JoinHelper {

    private static final Logger logger = LogManager.getLogger(JoinHelper.class);

    public static final String START_JOIN_ACTION_NAME = "internal:cluster/coordination/start_join";
    public static final String JOIN_ACTION_NAME = "internal:cluster/coordination/join";
    public static final String JOIN_VALIDATE_ACTION_NAME = "internal:cluster/coordination/join/validate";
    public static final String JOIN_PING_ACTION_NAME = "internal:cluster/coordination/join/ping";

    private final MasterService masterService;
    private final TransportService transportService;
    private volatile JoinTaskExecutor joinTaskExecutor;
    private final NodeHealthService nodeHealthService;

    private final Set<Tuple<DiscoveryNode, JoinRequest>> pendingOutgoingJoins = Collections.synchronizedSet(new HashSet<>());
    private final AtomicReference<FailedJoinAttempt> lastFailedJoinAttempt = new AtomicReference<>();
    private final Map<DiscoveryNode, Releasable> joinConnections = new HashMap<>(); // synchronized on itself

    private final Supplier<JoinTaskExecutor> joinTaskExecutorGenerator;

    JoinHelper(
        Settings settings,
        AllocationService allocationService,
        MasterService masterService,
        TransportService transportService,
        LongSupplier currentTermSupplier,
        Supplier<ClusterState> currentStateSupplier,
        BiConsumer<JoinRequest, ActionListener<Void>> joinHandler,
        Function<StartJoinRequest, Join> joinLeaderInTerm,
        Collection<BiConsumer<DiscoveryNode, ClusterState>> joinValidators,
        RerouteService rerouteService,
        NodeHealthService nodeHealthService
    ) {
        this.masterService = masterService;
        this.transportService = transportService;
        this.nodeHealthService = nodeHealthService;
        this.joinTaskExecutorGenerator = () -> new JoinTaskExecutor(allocationService, logger, rerouteService) {

            private final long term = currentTermSupplier.getAsLong();

            @Override
            public ClusterTasksResult<JoinTaskExecutor.Task> execute(ClusterState currentState, List<JoinTaskExecutor.Task> joiningTasks)
                throws Exception {
                // The current state that MasterService uses might have been updated by a (different) master in a higher term already
                // Stop processing the current cluster state update, as there's no point in continuing to compute it as
                // it will later be rejected by Coordinator.publish(...) anyhow
                if (currentState.term() > term) {
                    logger.trace("encountered higher term {} than current {}, there is a newer master", currentState.term(), term);
                    throw new NotMasterException("Higher term encountered (current: " + currentState.term() + " > used: " +
                        term + "), there is a newer master");
                } else if (currentState.nodes().getMasterNodeId() == null && joiningTasks.stream().anyMatch(Task::isBecomeMasterTask)) {
                    assert currentState.term() < term : "there should be at most one become master task per election (= by term)";
                    final CoordinationMetadata coordinationMetadata =
                            CoordinationMetadata.builder(currentState.coordinationMetadata()).term(term).build();
                    final Metadata metadata = Metadata.builder(currentState.metadata()).coordinationMetadata(coordinationMetadata).build();
                    currentState = ClusterState.builder(currentState).metadata(metadata).build();
                } else if (currentState.nodes().isLocalNodeElectedMaster()) {
                    assert currentState.term() == term : "term should be stable for the same master";
                }
                return super.execute(currentState, joiningTasks);
            }

        };

        transportService.registerRequestHandler(
            JOIN_ACTION_NAME,
            ThreadPool.Names.GENERIC,
            false,
            false,
            JoinRequest::new,
            (request, channel, task) -> joinHandler.accept(request,
                new ChannelActionListener<Empty, JoinRequest>(channel, JOIN_ACTION_NAME, request).map(ignored -> Empty.INSTANCE)));

        transportService.registerRequestHandler(START_JOIN_ACTION_NAME, Names.GENERIC, false, false,
            StartJoinRequest::new,
            (request, channel, task) -> {
                final DiscoveryNode destination = request.getSourceNode();
                sendJoinRequest(destination, currentTermSupplier.getAsLong(), Optional.of(joinLeaderInTerm.apply(request)));
                channel.sendResponse(Empty.INSTANCE);
            });

        transportService.registerRequestHandler(
            JOIN_PING_ACTION_NAME,
            ThreadPool.Names.SAME,
            false,
            false,
            TransportRequest.Empty::new,
            (request, channel, task) -> channel.sendResponse(Empty.INSTANCE));

        final String dataPath = Environment.PATH_DATA_SETTING.get(settings);
        transportService.registerRequestHandler(JOIN_VALIDATE_ACTION_NAME,
            ThreadPool.Names.GENERIC, ValidateJoinRequest::new,
            (request, channel, task) -> {
                final ClusterState localState = currentStateSupplier.get();
                if (localState.metadata().clusterUUIDCommitted() &&
                        localState.metadata().clusterUUID().equals(request.getState().metadata().clusterUUID()) == false) {
                    throw new CoordinationStateRejectedException("This node previously joined a cluster with UUID [" +
                            localState.metadata().clusterUUID() + "] and is now trying to join a different cluster with UUID [" +
                            request.getState().metadata().clusterUUID() + "]. This is forbidden and usually indicates an incorrect " +
                            "discovery or cluster bootstrapping configuration. Note that the cluster UUID persists across restarts and " +
                            "can only be changed by deleting the contents of the node's data path [" + dataPath +
                            "] which will also remove any data held by this node.");
                }
                joinValidators.forEach(action -> action.accept(transportService.getLocalNode(), request.getState()));
                channel.sendResponse(Empty.INSTANCE);
            });
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
        private final TransportException exception;
        private final long timestamp;

        FailedJoinAttempt(DiscoveryNode destination, JoinRequest joinRequest, TransportException exception) {
            this.destination = destination;
            this.joinRequest = joinRequest;
            this.exception = exception;
            this.timestamp = System.nanoTime();
        }

        void logNow() {
            logger.log(getLogLevel(exception),
                    () -> new ParameterizedMessage("failed to join {} with {}", destination, joinRequest),
                    exception);
        }

        static Level getLogLevel(TransportException e) {
            Throwable cause = e.unwrapCause();
            if (cause instanceof CoordinationStateRejectedException ||
                cause instanceof FailedToCommitClusterStateException ||
                cause instanceof NotMasterException) {
                return Level.DEBUG;
            }
            return Level.INFO;
        }

        void logWarnWithTimestamp() {
            logger.warn(() -> new ParameterizedMessage("last failed join attempt was {} ago, failed to join {} with {}",
                            TimeValue.timeValueMillis(TimeValue.nsecToMSec(System.nanoTime() - timestamp)),
                            destination,
                            joinRequest),
                    exception);
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
        final JoinRequest joinRequest = new JoinRequest(transportService.getLocalNode(), term, optionalJoin);
        final Tuple<DiscoveryNode, JoinRequest> dedupKey = Tuple.tuple(destination, joinRequest);
        if (pendingOutgoingJoins.add(dedupKey)) {
            logger.debug("attempting to join {} with {}", destination, joinRequest);

            // Typically we're already connected to the destination at this point, the PeerFinder holds a reference to this connection to
            // keep it open, but we need to acquire our own reference to keep the connection alive through the joining process.
            transportService.connectToNode(destination, new ActionListener<Releasable>() {
                @Override
                public void onResponse(Releasable connectionReference) {
                    logger.trace("acquired connection for joining join {} with {}", destination, joinRequest);

                    // Register the connection in joinConnections so it can be released once we successfully apply the cluster state, at
                    // which point the NodeConnectionsService will have taken ownership of it.
                    registerConnection(destination, connectionReference);

                    transportService.sendRequest(
                        destination,
                        JOIN_ACTION_NAME,
                        joinRequest,
                        TransportRequestOptions.of(null, TransportRequestOptions.Type.PING),
                        new TransportResponseHandler.Empty() {
                            @Override
                            public void handleResponse(TransportResponse.Empty response) {
                                pendingOutgoingJoins.remove(dedupKey);
                                logger.debug("successfully joined {} with {}", destination, joinRequest);
                                lastFailedJoinAttempt.set(null);
                            }

                            @Override
                            public void handleException(TransportException exp) {
                                pendingOutgoingJoins.remove(dedupKey);
                                FailedJoinAttempt attempt = new FailedJoinAttempt(destination, joinRequest, exp);
                                attempt.logNow();
                                lastFailedJoinAttempt.set(attempt);
                                unregisterAndReleaseConnection(destination, connectionReference);
                            }
                        });
                }

                @Override
                public void onFailure(Exception e) {
                    pendingOutgoingJoins.remove(dedupKey);
                    FailedJoinAttempt attempt = new FailedJoinAttempt(
                        destination,
                        joinRequest,
                        new ConnectTransportException(destination, "failed to acquire connection", e));
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
                    logger.debug(new ParameterizedMessage("failure in response to {} from {}", startJoinRequest, destination), exp);
                }
            });
    }

    static class JoinTaskListener implements ClusterStateTaskListener {
        private final JoinTaskExecutor.Task task;
        private final ActionListener<Void> joinListener;

        JoinTaskListener(JoinTaskExecutor.Task task, ActionListener<Void> joinListener) {
            this.task = task;
            this.joinListener = joinListener;
        }

        @Override
        public void onFailure(String source, Exception e) {
            joinListener.onFailure(e);
        }

        @Override
        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
            joinListener.onResponse(null);
        }

        @Override
        public String toString() {
            return "JoinTaskListener{task=" + task + "}";
        }
    }

    interface JoinAccumulator {
        void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinListener);

        default void close(Mode newMode) {
        }
    }

    class LeaderJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinListener) {
            final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(sender, "join existing leader");
            assert joinTaskExecutor != null;
            masterService.submitStateUpdateTask("node-join", task, ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new JoinTaskListener(task, joinListener));
        }

        @Override
        public String toString() {
            return "LeaderJoinAccumulator";
        }
    }

    static class InitialJoinAccumulator implements JoinAccumulator {
        @Override
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinListener) {
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
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinListener) {
            joinListener.onFailure(new CoordinationStateRejectedException("join target is a follower"));
        }

        @Override
        public String toString() {
            return "FollowerJoinAccumulator";
        }
    }

    class CandidateJoinAccumulator implements JoinAccumulator {

        private final Map<DiscoveryNode, ActionListener<Void>> joinRequestAccumulator = new HashMap<>();
        boolean closed;

        @Override
        public void handleJoinRequest(DiscoveryNode sender, ActionListener<Void> joinListener) {
            assert closed == false : "CandidateJoinAccumulator closed";
            ActionListener<Void> prev = joinRequestAccumulator.put(sender, joinListener);
            if (prev != null) {
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + sender));
            }
        }

        @Override
        public void close(Mode newMode) {
            assert closed == false : "CandidateJoinAccumulator closed";
            closed = true;
            if (newMode == Mode.LEADER) {
                final Map<JoinTaskExecutor.Task, ClusterStateTaskListener> pendingAsTasks = new LinkedHashMap<>();
                joinRequestAccumulator.forEach((key, value) -> {
                    final JoinTaskExecutor.Task task = new JoinTaskExecutor.Task(key, "elect leader");
                    pendingAsTasks.put(task, new JoinTaskListener(task, value));
                });

                final String stateUpdateSource = "elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";

                pendingAsTasks.put(JoinTaskExecutor.newBecomeMasterTask(), (source, e) -> {
                });
                pendingAsTasks.put(JoinTaskExecutor.newFinishElectionTask(), (source, e) -> {
                });
                joinTaskExecutor = joinTaskExecutorGenerator.get();
                masterService.submitStateUpdateTasks(stateUpdateSource, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor);
            } else {
                assert newMode == Mode.FOLLOWER : newMode;
                joinTaskExecutor = null;
                joinRequestAccumulator.values().forEach(joinCallback -> joinCallback.onFailure(
                    new CoordinationStateRejectedException("became follower")));
            }

            // CandidateJoinAccumulator is only closed when becoming leader or follower, otherwise it accumulates all joins received
            // regardless of term.
        }

        @Override
        public String toString() {
            return "CandidateJoinAccumulator{" + joinRequestAccumulator.keySet() +
                ", closed=" + closed + '}';
        }
    }
}
