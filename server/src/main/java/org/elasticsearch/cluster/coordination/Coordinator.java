package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.ElectMasterService;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.discovery.zen.NodeJoinController;
import org.elasticsearch.discovery.zen.NodeJoinController.JoinTaskExecutor;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponse.Empty;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

//TODO: add a test that sends random joins and checks if the join led to success (the fake masterservice fully manages state)
// i.e. similar to NodeJoinControllerTests
public class Coordinator extends AbstractLifecycleComponent {

    public static final String JOIN_ACTION_NAME = "internal:discovery/zen2/join";

    private final Object mutex = new Object();
    private Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private final SetOnce<CoordinationState> consensusState = new SetOnce<>();
    private final TransportService transportService;
    private final MasterService masterService;


    private final JoinTaskExecutor joinTaskExecutor;
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;

    // similar to NodeJoinController.ElectionContext.joinRequestAccumulator, captures joins on election
    private final Map<DiscoveryNode, MembershipAction.JoinCallback> joinRequestAccumulator = new HashMap<>();


    public Coordinator(Settings settings, TransportService transportService, AllocationService allocationService,
                       MasterService masterService, Supplier<CoordinationState.PersistedState> persistedStateSupplier) {
        super(settings);
        this.transportService = transportService;
        this.masterService = masterService;
        this.persistedStateSupplier = persistedStateSupplier;
        this.mode = Mode.CANDIDATE;
        lastKnownLeader = Optional.empty();
        lastJoin = Optional.empty();
        // disable minimum_master_nodes check
        final ElectMasterService electMasterService = new ElectMasterService(settings) {

            @Override
            public boolean hasEnoughMasterNodes(Iterable<DiscoveryNode> nodes) {
                return true;
            }

            @Override
            public void logMinimumMasterNodesWarningIfNecessary(ClusterState oldState, ClusterState newState) {
                // ignore
            }

        };

        // reuse JoinTaskExecutor implementation from ZenDiscovery, but hack some checks
        this.joinTaskExecutor = new JoinTaskExecutor(allocationService, electMasterService, logger) {

            @Override
            public ClusterTasksResult<DiscoveryNode> execute(ClusterState currentState, List<DiscoveryNode> joiningNodes) throws Exception {
                // This is called when preparing the next cluster state for publication. There is no guarantee that the term we see here is
                // the term under which this state will eventually be published: the current term may be increased after this check due to
                // some other activity. That the term is correct is, however, checked properly during publication, so it is sufficient to
                // check it here on a best-effort basis. This is fine because a concurrent change indicates the existence of another leader
                // in a higher term which will cause this node to stand down.

                final long currentTerm = getCurrentTerm(); // TODO perhaps this can be a volatile read?
                if (currentState.term() != currentTerm) {
                    currentState = ClusterState.builder(currentState).term(currentTerm).build();
                }
                return super.execute(currentState, joiningNodes);
            }

        };
    }

    private void registerTransportActions() {
        transportService.registerRequestHandler(JOIN_ACTION_NAME, Names.GENERIC, false, false,
            JoinRequest::new,
            (request, channel, task) -> handleJoinRequest(request, new MembershipAction.JoinCallback() {
                @Override
                public void onSuccess() {
                    try {
                        channel.sendResponse(Empty.INSTANCE);
                    } catch (IOException e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        channel.sendResponse(e);
                    } catch (Exception inner) {
                        inner.addSuppressed(e);
                        logger.warn("failed to send back failure on join request", inner);
                    }
                }
            }));
    }

    // TODO: maybe add this method in a follow-up.
    private void sendOptionalJoin(DiscoveryNode destination, Optional<Join> optionalJoin) {
        // No synchronisation required
        transportService.sendRequest(destination, JOIN_ACTION_NAME, new JoinRequest(getLocalNode(), optionalJoin),
            new TransportResponseHandler<Empty>() {
                @Override
                public Empty read(StreamInput in) {
                    return Empty.INSTANCE;
                }

                @Override
                public void handleResponse(Empty response) {
                    // No synchronisation required
                    logger.debug("SendJoinResponseHandler: successfully joined {}", destination);
                }

                @Override
                public void handleException(TransportException exp) {
                    // No synchronisation required
                    if (exp.getRootCause() instanceof CoordinationStateRejectedException) {
                        logger.debug("SendJoinResponseHandler: [{}] failed: {}", destination, exp.getRootCause().getMessage());
                    } else {
                        logger.debug(() -> new ParameterizedMessage("SendJoinResponseHandler: [{}] failed", destination), exp);
                    }
                }

                @Override
                public String executor() {
                    return Names.GENERIC;
                }
            });
    }

    // package-visible for testing
    long getCurrentTerm() {
        synchronized (mutex) {
            return consensusState.get().getCurrentTerm();
        }
    }

    // package-visible for testing
    Mode getMode() {
        synchronized (mutex) {
            return mode;
        }
    }

    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        if (getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        return Optional.empty();
    }

    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("joinLeaderInTerm: from [{}] with term {}", startJoinRequest.getSourceNode(), startJoinRequest.getTerm());
        Join join = consensusState.get().handleStartJoin(startJoinRequest);
        lastJoin = Optional.of(join);
        if (mode == Mode.CANDIDATE) {
            // refresh required because current term has changed
            // TODO: heartbeatRequestResponder = new Legislator.HeartbeatRequestResponder();
        } else {
            // becomeCandidate refreshes responders
            becomeCandidate("joinLeaderInTerm");
        }
        return join;
    }

    public void handleJoinRequest(JoinRequest joinRequest, MembershipAction.JoinCallback joinCallback) {

        transportService.connectToNode(joinRequest.getSourceNode());

        synchronized (mutex) {
            handleJoinRequestUnderLock(joinRequest, joinCallback);
        }
    }

    private void handleJoinRequestUnderLock(JoinRequest joinRequest, MembershipAction.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.trace("handleJoinRequestUnderLock: as {}, handling {}", mode, joinRequest);
        if (mode == Mode.LEADER) {
            // submit as cluster state update task
            masterService.submitStateUpdateTask("zen-disco-node-join",
                joinRequest.getSourceNode(), ClusterStateTaskConfig.build(Priority.URGENT),
                joinTaskExecutor, new NodeJoinController.JoinTaskListener(joinCallback, logger));
        } else {
            MembershipAction.JoinCallback prev = joinRequestAccumulator.put(joinRequest.getSourceNode(), joinCallback);
            if (prev != null) {
                prev.onFailure(new CoordinationStateRejectedException("received a newer join from " + joinRequest.getSourceNode()));
            }
        }

        try {
            joinRequest.getOptionalJoin().ifPresent(this::handleJoin);
        } catch (CoordinationStateRejectedException exception) {
            final MembershipAction.JoinCallback callback = joinRequestAccumulator.remove(joinRequest.getSourceNode());
            if (callback == null) {
                logger.trace("handleJoinRequestUnderLock: submitted task to master service, but vote was rejected", exception);
            } else {
                callback.onFailure(exception);
            }
        }
    }

    private void handleJoin(Join join) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        Optional<Join> optionalJoinToSelf = ensureTermAtLeast(getLocalNode(), join.getTerm());

        optionalJoinToSelf.ifPresent(this::handleJoin); // if someone thinks we should be master, let's try to become one

        final CoordinationState state = consensusState.get();
        boolean prevElectionWon = state.electionWon();
        boolean addedJoin = state.handleJoin(join);
        assert !prevElectionWon || state.electionWon(); // we cannot go from won to not won

//        if (addedJoin && mode == Legislator.Mode.LEADER && currentPublication.isPresent() == false) {
//            scheduleReconfigurationIfSuboptimal();
//        }

        if (prevElectionWon == false && state.electionWon()) {
            assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;

            becomeLeader("handleJoin");

            Map<DiscoveryNode, ClusterStateTaskListener> pendingAsTasks = getPendingAsTasks();
            joinRequestAccumulator.clear();

            final String source = "zen-disco-elected-as-master ([" + pendingAsTasks.size() + "] nodes joined)";
            // noop listener, the election finished listener determines result
            pendingAsTasks.put(NodeJoinController.BECOME_MASTER_TASK, (source1, e) -> {
            });
            // TODO: should we take any further action when FINISH_ELECTION_TASK fails?
            pendingAsTasks.put(NodeJoinController.FINISH_ELECTION_TASK, (source1, e) -> {
            });

            try {
                masterService.submitStateUpdateTasks(source, pendingAsTasks, ClusterStateTaskConfig.build(Priority.URGENT),
                    joinTaskExecutor);
            } catch (IllegalStateException e) {
                // TODO fix NodeJoinController and remove this
                assert e.getMessage().contains("is already queued");
                becomeCandidate("handleJoin");
            }
        }
    }

    // copied from NodeJoinController.getPendingAsTasks
    private Map<DiscoveryNode, ClusterStateTaskListener> getPendingAsTasks() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        Map<DiscoveryNode, ClusterStateTaskListener> tasks = new HashMap<>();
        joinRequestAccumulator.forEach((key, value) -> tasks.put(key,
            new NodeJoinController.JoinTaskListener(value, logger)));
        return tasks;
    }

    private void clearJoins() {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        joinRequestAccumulator.values().forEach(
            joinCallback -> joinCallback.onFailure(new CoordinationStateRejectedException("node stepped down as leader")));
        joinRequestAccumulator.clear();
    }

    private void becomeCandidate(String method) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("{}: becoming CANDIDATE (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            mode = Mode.CANDIDATE;

            clearJoins();
        }
    }

    private void becomeLeader(String method) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        assert mode != Mode.LEADER;

        logger.debug("{}: becoming LEADER (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        mode = Mode.LEADER;
        lastKnownLeader = Optional.of(getLocalNode());
    }

    private void becomeFollower(String method, DiscoveryNode leaderNode) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";

        if (mode != Mode.FOLLOWER) {
            logger.debug("{}: becoming FOLLOWER of [{}] (was {}, lastKnownLeader was [{}])", method, leaderNode, mode, lastKnownLeader);

            mode = Mode.FOLLOWER;

            clearJoins();
        }

        lastKnownLeader = Optional.of(leaderNode);
    }

    public DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
    }

    @Override
    protected void doStart() {
        CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
        consensusState.set(new CoordinationState(settings, getLocalNode(), persistedState));
    }

    public void startInitialJoin() {
        synchronized (mutex) {
            becomeCandidate("startInitialJoin");
        }
    }

    @Override
    protected void doStop() {

    }

    @Override
    protected void doClose() {

    }

    public void invariant() {
        synchronized (mutex) {
            if (mode == Mode.LEADER) {
                assert consensusState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
            } else if (mode == Mode.FOLLOWER) {
                assert consensusState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
                assert lastKnownLeader.isPresent() && (lastKnownLeader.get().equals(getLocalNode()) == false);
            } else {
                assert mode == Mode.CANDIDATE;
            }
        }
    }

    public enum Mode {
        CANDIDATE, LEADER, FOLLOWER
    }
}
