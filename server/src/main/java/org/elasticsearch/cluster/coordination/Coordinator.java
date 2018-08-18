package org.elasticsearch.cluster.coordination;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.coordination.JoinHelper.JoinCallback;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.transport.TransportService;

import java.util.Optional;
import java.util.function.Supplier;

public class Coordinator extends AbstractLifecycleComponent {

    private final TransportService transportService;
    private final JoinHelper joinHelper;
    private final Supplier<CoordinationState.PersistedState> persistedStateSupplier;
    private final Object mutex = new Object();
    private final SetOnce<CoordinationState> coordinationState = new SetOnce<>(); // initialized on start-up (see doStart)

    private volatile Mode mode;
    private Optional<DiscoveryNode> lastKnownLeader;
    private Optional<Join> lastJoin;

    public Coordinator(Settings settings, TransportService transportService, AllocationService allocationService,
                       MasterService masterService, Supplier<CoordinationState.PersistedState> persistedStateSupplier) {
        super(settings);
        this.transportService = transportService;
        this.joinHelper = new JoinHelper(settings, allocationService, masterService, transportService, this::getCurrentTerm,
            this::handleJoinRequest);
        this.persistedStateSupplier = persistedStateSupplier;
        this.lastKnownLeader = Optional.empty();
        this.lastJoin = Optional.empty();
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
        Join join = coordinationState.get().handleStartJoin(startJoinRequest);
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

    public void handleJoinRequest(JoinRequest joinRequest, JoinCallback joinCallback) {
        assert Thread.holdsLock(mutex) == false;
        transportService.connectToNode(joinRequest.getSourceNode());

        synchronized (mutex) {
            handleJoinRequestUnderLock(joinRequest, joinCallback);
        }
    }

    private void handleJoinRequestUnderLock(JoinRequest joinRequest, JoinCallback joinCallback) {
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

        final CoordinationState state = coordinationState.get();
        boolean prevElectionWon = state.electionWon();
        boolean addedJoin = state.handleJoin(join);

        if (prevElectionWon == false && state.electionWon()) {
            assert mode == Mode.CANDIDATE : "expected candidate but was " + mode;
            becomeLeader("handleJoin");
            joinHelper.clearAndSendJoins();
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

    // package-visible for testing
    long getCurrentTerm() {
        synchronized (mutex) {
            return coordinationState.get().getCurrentTerm();
        }
    }

    // package-visible for testing
    Mode getMode() {
        return mode;
    }

    // package-visible for testing
    DiscoveryNode getLocalNode() {
        return transportService.getLocalNode();
    }

    @Override
    protected void doStart() {
        CoordinationState.PersistedState persistedState = persistedStateSupplier.get();
        coordinationState.set(new CoordinationState(settings, getLocalNode(), persistedState));
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
                assert coordinationState.get().electionWon();
                assert lastKnownLeader.isPresent() && lastKnownLeader.get().equals(getLocalNode());
            } else if (mode == Mode.FOLLOWER) {
                assert coordinationState.get().electionWon() == false : getLocalNode() + " is FOLLOWER so electionWon() should be false";
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
