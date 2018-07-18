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
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * The safety core of the consensus algorithm
 */
public class CoordinationState extends AbstractComponent {

    private final DiscoveryNode localNode;

    // persisted state
    private final PersistedState persistedState;

    // transient state
    private NodeCollection joinVotes;
    private boolean startedJoinSinceLastReboot;
    private boolean electionWon;
    private long lastPublishedVersion;
    private VotingConfiguration lastPublishedConfiguration;
    private NodeCollection publishVotes;

    public CoordinationState(Settings settings, DiscoveryNode localNode, PersistedState persistedState) {
        super(settings);

        this.localNode = localNode;

        // persisted state
        this.persistedState = persistedState;

        // transient state
        this.electionWon = false;
        this.joinVotes = new NodeCollection();
        this.startedJoinSinceLastReboot = false;
        this.publishVotes = new NodeCollection();
        this.lastPublishedVersion = 0L;
        this.lastPublishedConfiguration = persistedState.getLastAcceptedConfiguration();
    }

    public long getCurrentTerm() {
        return persistedState.getCurrentTerm();
    }

    public ClusterState getLastAcceptedState() {
        return persistedState.getLastAcceptedState();
    }

    public boolean isElectionQuorum(NodeCollection votes) {
        return votes.isQuorum(getLastCommittedConfiguration()) && votes.isQuorum(getLastAcceptedConfiguration());
    }

    public boolean isPublishQuorum(NodeCollection votes) {
        return votes.isQuorum(getLastCommittedConfiguration()) && votes.isQuorum(lastPublishedConfiguration);
    }

    public VotingConfiguration getLastCommittedConfiguration() {
        return persistedState.getLastCommittedConfiguration();
    }

    public VotingConfiguration getLastAcceptedConfiguration() {
        return persistedState.getLastAcceptedConfiguration();
    }

    public long getLastAcceptedVersion() {
        return persistedState.getLastAcceptedVersion();
    }

    public long getLastAcceptedTerm() {
        return persistedState.getLastAcceptedTerm();
    }

    public boolean electionWon() {
        return electionWon;
    }

    public long getLastPublishedVersion() {
        return lastPublishedVersion;
    }

    public boolean hasElectionQuorum(VotingConfiguration votingConfiguration) {
        return joinVotes.isQuorum(votingConfiguration);
    }

    public boolean containsJoinVote(DiscoveryNode node) {
        return joinVotes.contains(node);
    }

    /**
     * Used to bootstrap a cluster by injecting the initial state and configuration.
     *
     * @param initialState The initial state to use.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public void setInitialState(ClusterState initialState) {

        final long lastAcceptedVersion = persistedState.getLastAcceptedVersion();
        if (lastAcceptedVersion != 0) {
            logger.debug("setInitialState: rejecting since last-accepted version {} > 0", lastAcceptedVersion);
            throw new CoordinationStateRejectedException("initial state already set: last-accepted version now " + lastAcceptedVersion);
        }

        assert persistedState.getLastAcceptedTerm() == 0;
        assert persistedState.getLastAcceptedConfiguration().isEmpty();
        assert persistedState.getLastCommittedConfiguration().isEmpty();
        assert initialState.getLastAcceptedConfiguration().isEmpty() == false;
        assert initialState.getLastCommittedConfiguration().isEmpty() == false;
        assert initialState.term() == 0;
        assert initialState.version() == 1;

        assert lastPublishedVersion == 0;
        assert lastPublishedConfiguration.isEmpty();
        assert electionWon == false;
        assert joinVotes.nodes.isEmpty();
        assert publishVotes.nodes.isEmpty();

        persistedState.setLastAcceptedState(initialState);
    }

    /**
     * May be safely called at any time to move this instance to a new term.
     *
     * @param startJoinRequest The startJoinRequest, specifying the node requesting the join.
     * @return A Join that should be sent to the target node of the join.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public Messages.Join handleStartJoin(Messages.StartJoinRequest startJoinRequest) {
        if (startJoinRequest.getTerm() <= getCurrentTerm()) {
            logger.debug("handleStartJoin: ignored as term provided [{}] not greater than current term [{}]",
                startJoinRequest.getTerm(), getCurrentTerm());
            throw new CoordinationStateRejectedException("incoming term " + startJoinRequest.getTerm() +
                " not greater than than current term " + getCurrentTerm());
        }

        logger.debug("handleStartJoin: updating term from [{}] to [{}]", getCurrentTerm(), startJoinRequest.getTerm());

        persistedState.setCurrentTerm(startJoinRequest.getTerm());
        assert persistedState.getCurrentTerm() == startJoinRequest.getTerm();
        joinVotes = new NodeCollection();
        electionWon = false;
        startedJoinSinceLastReboot = true;
        lastPublishedVersion = 0L;
        lastPublishedConfiguration = persistedState.getLastAcceptedConfiguration();
        publishVotes = new NodeCollection();

        return new Messages.Join(localNode, startJoinRequest.getSourceNode(), getLastAcceptedVersion(), getCurrentTerm(), getLastAcceptedTerm());
    }

    /**
     * May be called on receipt of a Join from the given sourceNode.
     *
     * @param join The Join received.
     * @return true iff this join was not already added
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public boolean handleJoin(Messages.Join join) {
        assert join.getTargetNode().equals(localNode) : "handling join " + join + " for the wrong node " + localNode;

        if (startedJoinSinceLastReboot == false) {
            logger.debug("handleJoin: ignored join as term was not incremented yet after reboot");
            throw new CoordinationStateRejectedException("ignored join as term was not incremented yet after reboot");
        }

        if (join.getTerm() != getCurrentTerm()) {
            logger.debug("handleJoin: ignored join due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), join.getTerm());
            throw new CoordinationStateRejectedException(
                "incoming term " + join.getTerm() + " does not match current term " + getCurrentTerm());
        }

        final long lastAcceptedTerm = getLastAcceptedTerm();
        if (join.getLastAcceptedTerm() > lastAcceptedTerm) {
            logger.debug("handleJoin: ignored join as joiner has better last accepted term (expected: <=[{}], actual: [{}])",
                lastAcceptedTerm, join.getLastAcceptedTerm());
            throw new CoordinationStateRejectedException("incoming last accepted term " + join.getLastAcceptedTerm() +
                " of join higher than current last accepted term " + lastAcceptedTerm);
        }

        if (join.getLastAcceptedTerm() == lastAcceptedTerm && join.getLastAcceptedVersion() > getLastAcceptedVersion()) {
            logger.debug("handleJoin: ignored join due to version mismatch (expected: <=[{}], actual: [{}])",
                getLastAcceptedVersion(), join.getLastAcceptedVersion());
            throw new CoordinationStateRejectedException(
                "incoming version " + join.getLastAcceptedVersion() + " of join higher than current version " + getLastAcceptedVersion());
        }

        if (getLastAcceptedVersion() == 0) {
            // We do not check for an election won on setting the initial configuration, so it would be possible to end up in a state where
            // we have enough join votes to have won the election immediately on setting the initial configuration. It'd be quite
            // complicated to restore all the appropriate invariants when setting the initial configuration (it's not just `electionWon`)
            // so instead we just ignore join votes received prior to receiving the initial configuration.
            logger.debug("handleJoin: ignoring join because initial configuration not set");
            throw new CoordinationStateRejectedException("initial configuration not set");
        }

        boolean added = joinVotes.add(join.getSourceNode());
        boolean prevElectionWon = electionWon;
        electionWon = isElectionQuorum(joinVotes);
        logger.debug("handleJoin: added join {} from [{}] for election, electionWon={} lastAcceptedTerm={} lastAcceptedVersion={}", join,
            join.getSourceNode(), electionWon, lastAcceptedTerm, getLastAcceptedVersion());

        if (electionWon && prevElectionWon == false) {
            lastPublishedVersion = getLastAcceptedVersion();
        }
        return added;
    }

    /**
     * May be called in order to check if the given cluster state can be published
     *
     * @param clusterState The cluster state which to publish.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public Messages.PublishRequest handleClientValue(ClusterState clusterState) {
        if (electionWon == false) {
            logger.debug("handleClientValue: ignored request as election not won");
            throw new CoordinationStateRejectedException("election not won");
        }
        if (lastPublishedVersion != getLastAcceptedVersion()) {
            logger.debug("handleClientValue: cannot start publishing next value before accepting previous one");
            throw new CoordinationStateRejectedException("cannot start publishing next value before accepting previous one");
        }
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handleClientValue: ignored request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }
        if (clusterState.version() <= lastPublishedVersion) {
            logger.debug("handleClientValue: ignored request due to version mismatch " +
                    "(expected: [term {} version >{}], actual: [term {} version {}])",
                getCurrentTerm(), lastPublishedVersion, clusterState.term(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming cluster state version " + clusterState.version() +
                " lower or equal to last published version " + lastPublishedVersion);
        }

        assert getLastCommittedConfiguration().equals(clusterState.getLastCommittedConfiguration()) :
            "last committed configuration should not change";

        if (clusterState.getLastAcceptedConfiguration().equals(getLastAcceptedConfiguration()) == false
            && getLastCommittedConfiguration().equals(getLastAcceptedConfiguration()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration while not already reconfiguring");
            throw new CoordinationStateRejectedException("only allow reconfiguration while not already reconfiguring");
        }
        if (hasElectionQuorum(clusterState.getLastAcceptedConfiguration()) == false) {
            logger.debug("handleClientValue: only allow reconfiguration if join quorum available for new config");
            throw new CoordinationStateRejectedException("only allow reconfiguration if join quorum available for new config");
        }

        lastPublishedVersion = clusterState.version();
        lastPublishedConfiguration = clusterState.getLastAcceptedConfiguration();
        publishVotes = new NodeCollection();

        logger.trace("handleClientValue: processing request for version [{}] and term [{}]", lastPublishedVersion, getCurrentTerm());

        return new Messages.PublishRequest(clusterState);
    }

    /**
     * May be called on receipt of a PublishRequest.
     *
     * @param publishRequest The publish request received.
     * @return A PublishResponse which can be sent back to the sender of the PublishRequest.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public Messages.PublishResponse handlePublishRequest(Messages.PublishRequest publishRequest) {
        final ClusterState clusterState = publishRequest.getAcceptedState();
        if (clusterState.term() != getCurrentTerm()) {
            logger.debug("handlePublishRequest: ignored publish request due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), clusterState.term());
            throw new CoordinationStateRejectedException("incoming term " + clusterState.term() + " does not match current term " +
                getCurrentTerm());
        }
        if (clusterState.term() == getLastAcceptedTerm() && clusterState.version() <= getLastAcceptedVersion()) {
            logger.debug("handlePublishRequest: ignored publish request due to version mismatch (expected: >[{}], actual: [{}])",
                getLastAcceptedVersion(), clusterState.version());
            throw new CoordinationStateRejectedException("incoming version " + clusterState.version() + " older than current version " +
                getLastAcceptedVersion());
        }

        logger.trace("handlePublishRequest: accepting publish request for version [{}] and term [{}]",
            clusterState.version(), clusterState.term());
        persistedState.setLastAcceptedState(clusterState);
        assert persistedState.getLastAcceptedState() == clusterState;

        return new Messages.PublishResponse(clusterState.version(), clusterState.term());
    }

    /**
     * May be called on receipt of a PublishResponse from the given sourceNode.
     *
     * @param sourceNode      The sender of the PublishResponse received.
     * @param publishResponse The PublishResponse received.
     * @return An optional ApplyCommit which, if present, may be broadcast to all peers, indicating that this publication
     * has been accepted at a quorum of peers and is therefore committed.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public Optional<Messages.ApplyCommit> handlePublishResponse(DiscoveryNode sourceNode, Messages.PublishResponse publishResponse) {
        if (electionWon == false) {
            logger.debug("handlePublishResponse: ignored response as election not won");
            throw new CoordinationStateRejectedException("election not won");
        }
        if (publishResponse.getTerm() != getCurrentTerm()) {
            logger.debug("handlePublishResponse: ignored publish response due to term mismatch (expected: [{}], actual: [{}])",
                getCurrentTerm(), publishResponse.getTerm());
            throw new CoordinationStateRejectedException("incoming term " + publishResponse.getTerm()
                + " does not match current term " + getCurrentTerm());
        }
        if (publishResponse.getVersion() != lastPublishedVersion) {
            logger.debug("handlePublishResponse: ignored publish response due to version mismatch (expected: [{}], actual: [{}])",
                lastPublishedVersion, publishResponse.getVersion());
            throw new CoordinationStateRejectedException("incoming version " + publishResponse.getVersion() +
                " does not match current version " + lastPublishedVersion);
        }

        logger.trace("handlePublishResponse: accepted publish response for version [{}] and term [{}] from [{}]",
            publishResponse.getVersion(), publishResponse.getTerm(), sourceNode);
        publishVotes.add(sourceNode);
        if (isPublishQuorum(publishVotes)) {
            logger.trace("handlePublishResponse: value committed for version [{}] and term [{}]",
                publishResponse.getVersion(), publishResponse.getTerm());
            return Optional.of(new Messages.ApplyCommit(localNode, publishResponse.getTerm(), publishResponse.getVersion()));
        }

        return Optional.empty();
    }

    /**
     * May be called on receipt of an ApplyCommit. Updates the committed state accordingly.
     *
     * @param applyCommit The ApplyCommit received.
     * @throws CoordinationStateRejectedException if the arguments were incompatible with the current state of this object.
     */
    public void handleCommit(Messages.ApplyCommit applyCommit) {
        if (applyCommit.getTerm() != getCurrentTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getTerm(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming term " + applyCommit.getTerm() + " does not match current term " +
                getCurrentTerm());
        }
        if (applyCommit.getTerm() != getLastAcceptedTerm()) {
            logger.debug("handleCommit: ignored commit request due to term mismatch " +
                    "(expected: [term {} version {}], actual: [term {} version {}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getTerm(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming term " + applyCommit.getTerm() + " does not match last accepted term " +
                getLastAcceptedTerm());
        }
        if (applyCommit.getVersion() != getLastAcceptedVersion()) {
            logger.debug("handleCommit: ignored commit request due to version mismatch (term {}, expected: [{}], actual: [{}])",
                getLastAcceptedTerm(), getLastAcceptedVersion(), applyCommit.getVersion());
            throw new CoordinationStateRejectedException("incoming version " + applyCommit.getVersion() +
                " does not match current version " + getLastAcceptedVersion());
        }

        logger.trace("handleCommit: applying commit request for term [{}] and version [{}]", applyCommit.getTerm(),
            applyCommit.getVersion());

        persistedState.markLastAcceptedConfigAsCommitted();
        assert getLastCommittedConfiguration().equals(getLastAcceptedConfiguration());
    }

    public interface PersistedState {

        void setCurrentTerm(long currentTerm);

        void setLastAcceptedState(ClusterState clusterState);

        void markLastAcceptedConfigAsCommitted();

        long getCurrentTerm();

        ClusterState getLastAcceptedState();

        default long getLastAcceptedVersion() {
            return getLastAcceptedState().version();
        }

        default long getLastAcceptedTerm() {
            return getLastAcceptedState().term();
        }

        default VotingConfiguration getLastAcceptedConfiguration() {
            return getLastAcceptedState().getLastAcceptedConfiguration();
        }

        default VotingConfiguration getLastCommittedConfiguration() {
            return getLastAcceptedState().getLastCommittedConfiguration();
        }
    }

    public static class InMemoryPersistedState implements PersistedState {

        private long currentTerm;
        private ClusterState acceptedState;

        public InMemoryPersistedState(long term, ClusterState acceptedState) {
            this.currentTerm = term;
            this.acceptedState = acceptedState;

            assert currentTerm >= 0;
            assert getLastAcceptedTerm() <= currentTerm :
                "last accepted term " + getLastAcceptedTerm() + " cannot be above current term " + currentTerm;
        }

        // copy constructor
        public InMemoryPersistedState(PersistedState persistedState) {
            this.currentTerm = persistedState.getCurrentTerm();
            this.acceptedState = persistedState.getLastAcceptedState();
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            assert this.currentTerm <= currentTerm;
            this.currentTerm = currentTerm;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            this.acceptedState = clusterState;
        }

        @Override
        public void markLastAcceptedConfigAsCommitted() {
            this.acceptedState = ClusterState.builder(acceptedState)
                .lastCommittedConfiguration(acceptedState.getLastAcceptedConfiguration())
                .build();
        }

        @Override
        public long getCurrentTerm() {
            return currentTerm;
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return acceptedState;
        }
    }

    /**
     * A collection of nodes, used to calculate quorums.
     */
    public static class NodeCollection {

        private final Map<String, DiscoveryNode> nodes;

        public boolean add(DiscoveryNode sourceNode) {
            // TODO is getId() unique enough or is it user-provided? If the latter, there's a risk of duplicates or of losing votes.
            return nodes.put(sourceNode.getId(), sourceNode) == null;
        }

        public NodeCollection() {
            nodes = new HashMap<>();
        }

        public boolean isQuorum(VotingConfiguration configuration) {
            return configuration.hasQuorum(nodes.keySet());
        }

        public boolean contains(DiscoveryNode node) {
            return nodes.containsKey(node.getId());
        }

        @Override
        public String toString() {
            return "NodeCollection{" + String.join(",", nodes.keySet()) + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            NodeCollection that = (NodeCollection) o;

            return nodes.equals(that.nodes);
        }

        @Override
        public int hashCode() {
            return nodes.hashCode();
        }

    }
}
