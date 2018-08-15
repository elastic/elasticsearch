package org.elasticsearch.cluster.coordination;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.discovery.zen.MembershipAction;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.function.Supplier;

public class Coordinator extends AbstractLifecycleComponent {

    private final Object mutex = new Object();
    private final TransportService transportService;
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;

    private final SetOnce<CoordinationState> consensusState = new SetOnce<>(); // initialized on start-up (see doStart)

    private volatile Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;
    private final JoinHelper joinHelper;

    public Coordinator(Settings settings, TransportService transportService, AllocationService allocationService,
                       MasterService masterService, Supplier<CoordinationState.PersistedState> persistedStateSupplier) {
        super(settings);
        this.transportService = transportService;
        this.persistedStateSupplier = persistedStateSupplier;
        this.mode = null;
        lastKnownLeader = Optional.empty();
        lastJoin = Optional.empty();

        joinHelper = new JoinHelper(settings, allocationService, masterService, transportService, this::getCurrentTerm) {
            @Override
            public void handleJoinRequest(JoinRequest joinRequest, MembershipAction.JoinCallback joinCallback) {
                Coordinator.this.handleJoinRequest(joinRequest, joinCallback);
            }
        };
    }

    // package-visible for testing
    long getCurrentTerm() {
        synchronized (mutex) {
            return consensusState.get().getCurrentTerm();
        }
    }

    // package-visible for testing
    Mode getMode() {
        return mode;
    }

    private Optional<Join> ensureTermAtLeast(DiscoveryNode sourceNode, long targetTerm) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        if (getCurrentTerm() < targetTerm) {
            return Optional.of(joinLeaderInTerm(new StartJoinRequest(sourceNode, targetTerm)));
        }
        return Optional.empty();
    }

    private Join joinLeaderInTerm(StartJoinRequest startJoinRequest) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
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
        assert Thread.holdsLock(mutex) == false;
        transportService.connectToNode(joinRequest.getSourceNode());

        synchronized (mutex) {
            handleJoinRequestUnderLock(joinRequest, joinCallback);
        }
    }

    private void handleJoinRequestUnderLock(JoinRequest joinRequest, MembershipAction.JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
        logger.trace("handleJoinRequestUnderLock: as {}, handling {}", mode, joinRequest);
        if (mode == Mode.LEADER) {
            joinHelper.joinLeader(joinRequest, joinCallback);
        } else {
            joinHelper.addJoinCallback(joinRequest, joinCallback);
        }

        try {
            joinRequest.getOptionalJoin().ifPresent(this::handleJoin);
        } catch (CoordinationStateRejectedException exception) {
            joinHelper.removeAndFail(joinRequest, exception);
        }
    }

    private void handleJoin(Join join) {
        assert Thread.holdsLock(mutex) : "Coordinator mutex not held";
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

            try {
                joinHelper.clearAndSendJoins();
            } catch (IllegalStateException e) {
                // TODO fix NodeJoinController and remove this
                assert e.getMessage().contains("is already queued");
                becomeCandidate("handleJoin");
            }
        }
    }

    private void becomeCandidate(String method) {
        assert Thread.holdsLock(mutex) : "Legislator mutex not held";
        logger.debug("{}: becoming CANDIDATE (was {}, lastKnownLeader was [{}])", method, mode, lastKnownLeader);

        if (mode != Mode.CANDIDATE) {
            mode = Mode.CANDIDATE;

            joinHelper.clearJoins();
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

            joinHelper.clearJoins();
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
