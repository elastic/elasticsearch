/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class AtomicRegisterCoordinatorTests extends CoordinatorTests {
    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testExpandsConfigurationWhenGrowingFromThreeToFiveNodesAndShrinksBackToThreeOnFailure() {
        // Voting configuration test
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testDoesNotShrinkConfigurationBelowThreeNodes() {
        // Voting configuration test
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testCanShrinkFromFiveNodesToThree() {
        // Voting configuration test
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testDoesNotShrinkConfigurationBelowFiveNodesIfAutoShrinkDisabled() {
        // Voting configuration test
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testExpandsConfigurationWhenGrowingFromOneNodeToThreeButDoesNotShrink() {
        // Voting configuration test
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testSingleNodeDiscoveryWithoutQuorum() {
        // Voting configuration test
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testReconfiguresToExcludeMasterIneligibleNodesInVotingConfig() {
        // Configuration test
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5644")
    public void testUnhealthyNodesGetsRemoved() {
        // This test checks that the voting configuration shrinks after a node is removed from the cluster
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testLeaderDisconnectionWithDisconnectEventDetectedQuickly() {
        // In this test the leader still has access to the register, therefore it is still considered as a leader.
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testLeaderDisconnectionWithoutDisconnectEventDetectedQuickly() {
        // In this test the leader still has access to the register, therefore it is still considered as a leader.
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testMasterStatsOnFailedUpdate() {
        // In this test the leader still has access to the register, therefore it is still considered as a leader, and it can perform
        // updates.
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testUnhealthyLeaderIsReplaced() {
        // In this test the leader still has access to the register, therefore it is still considered as a leader.
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testUnresponsiveLeaderDetectedEventually() {
        // In this test the leader still has access to the register, therefore it is still considered as a leader.
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testLogsWarningPeriodicallyIfClusterNotFormed() {
        // All nodes have access to the register, therefore it's possible to form a single-node cluster
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testAckListenerReceivesNacksIfLeaderStandsDown() {
        // The leader still has access to the register, therefore it acknowledges the state update
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testAckListenerReceivesNacksIfPublicationTimesOut() {
        // The leader still has access to the register, therefore it acknowledges the state update
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testAppliesNoMasterBlockWritesByDefault() {
        // If the disconnected node is the leader it will continue to have connectivity
        // into the register and therefore the no master block won't be applied
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testAppliesNoMasterBlockWritesIfConfigured() {
        // If the disconnected node is the leader it will continue to have connectivity
        // into the register and therefore the no master block won't be applied
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testAppliesNoMasterBlockAllIfConfigured() {
        // If the disconnected node is the leader it will continue to have connectivity
        // into the register and therefore the no master block won't be applied
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testAppliesNoMasterBlockMetadataWritesIfConfigured() {
        // If the disconnected node is the leader it will continue to have connectivity
        // into the register and therefore the no master block won't be applied
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testClusterCannotFormWithFailingJoinValidation() {
        // A single node can form a cluster in this case
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testReportsConnectBackProblemsDuringJoining() {
        // If the partitioned node is the leader, it still has access
        // to the store, therefore the test fail
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testCannotJoinClusterWithDifferentUUID() {
        // The cluster2 leader is considered dead since we only run the nodes in cluster 1
        // therefore the node coming from cluster 2 ends up taking over the old master in cluster 2
        // TODO: add more checks to avoid forming a mixed cluster between register based and traditional clusters
    }

    @Override
    public void testJoiningNodeReceivesFullState() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            cluster.addNodesAndStabilise(1);
            final Cluster.ClusterNode newNode = cluster.clusterNodes.get(cluster.clusterNodes.size() - 1);
            final PublishClusterStateStats newNodePublishStats = newNode.coordinator.stats().getPublishStats();
            // initial cluster state send when joining
            assertEquals(1L, newNodePublishStats.getFullClusterStateReceivedCount());
            // no reconfiguration
            assertEquals(0, newNodePublishStats.getCompatibleClusterStateDiffReceivedCount());
            assertEquals(0L, newNodePublishStats.getIncompatibleClusterStateDiffReceivedCount());
        }
    }

    @Override
    protected CoordinatorStrategy getCoordinatorStrategy() {
        var atomicRegister = new AtomicRegister();
        var sharedStore = new SharedStore(atomicRegister);
        return new AtomicRegisterCoordinatorStrategy(atomicRegister, sharedStore);
    }

    record HeartBeat(DiscoveryNode leader, long absoluteTimeInMillis) {
        long timeSinceLastHeartbeatInMillis(long nowInMillis) {
            return nowInMillis - absoluteTimeInMillis;
        }
    }

    static class StoreHeartbeatService implements LeaderHeartbeatService {
        private final SharedStore sharedStore;
        private final ThreadPool threadPool;
        private final TimeValue heartbeatFrequency;
        private final TimeValue maxTimeSinceLastHeartbeat;

        private DiscoveryNode currentLeader;
        private long currentTerm;
        private Scheduler.Cancellable heartbeatTask;

        StoreHeartbeatService(
            SharedStore sharedStore,
            ThreadPool threadPool,
            TimeValue heartbeatFrequency,
            TimeValue maxTimeSinceLastHeartbeat
        ) {
            this.sharedStore = sharedStore;
            this.threadPool = threadPool;
            this.heartbeatFrequency = heartbeatFrequency;
            this.maxTimeSinceLastHeartbeat = maxTimeSinceLastHeartbeat;
        }

        @Override
        public void start(DiscoveryNode currentLeader, long term) {
            this.currentLeader = currentLeader;
            this.currentTerm = term;

            sendHeartBeatToStore();
            this.heartbeatTask = threadPool.scheduleWithFixedDelay(
                this::sendHeartBeatToStore,
                heartbeatFrequency,
                ThreadPool.Names.GENERIC
            );
        }

        private void sendHeartBeatToStore() {
            sharedStore.writeHearBeat(currentTerm, new HeartBeat(currentLeader, threadPool.absoluteTimeInMillis()));
        }

        @Override
        public void stop() {
            this.currentLeader = null;
            this.currentTerm = 0;
            var heartBeatTask = this.heartbeatTask;
            if (heartBeatTask != null) {
                heartBeatTask.cancel();
            }
        }

        private Optional<DiscoveryNode> isLeaderInTermAlive(long term) {
            var latestHeartBeat = sharedStore.getHearbeatForTerm(term);
            if (latestHeartBeat == null) {
                return Optional.empty();
            }

            if (maxTimeSinceLastHeartbeat.millis() > latestHeartBeat.timeSinceLastHeartbeatInMillis(threadPool.absoluteTimeInMillis())) {
                return Optional.of(latestHeartBeat.leader());
            } else {
                return Optional.empty();
            }
        }
    }

    class AtomicRegisterCoordinatorStrategy implements CoordinatorStrategy {
        static final Setting<TimeValue> HEARTBEAT_FREQUENCY = Setting.timeSetting(
            "heartbeat_frequency",
            TimeValue.timeValueSeconds(15),
            Setting.Property.NodeScope
        );
        static final Setting<Integer> MAX_MISSED_HEARTBEATS = Setting.intSetting("max_missed_heartbeats", 2, 1, Setting.Property.NodeScope);

        private final AtomicRegister atomicRegister;
        private final SharedStore sharedStore;

        AtomicRegisterCoordinatorStrategy(AtomicRegister atomicRegister, SharedStore sharedStore) {
            this.atomicRegister = atomicRegister;
            this.sharedStore = sharedStore;
        }

        @Override
        public CoordinationServices getCoordinationServices(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            CoordinationState.PersistedState persistedState
        ) {
            final TimeValue heartbeatFrequency = HEARTBEAT_FREQUENCY.get(settings);
            var atomicHeartBeat = new StoreHeartbeatService(
                sharedStore,
                threadPool,
                heartbeatFrequency,
                TimeValue.timeValueMillis(heartbeatFrequency.millis() * MAX_MISSED_HEARTBEATS.get(settings))
            );
            var reconfigurator = new SingleNodeReconfigurator(settings, clusterSettings);
            var quorumStrategy = new AtomicRegisterElectionStrategy(
                atomicRegister,
                sharedStore,
                atomicHeartBeat::isLeaderInTermAlive,
                (AtomicRegisterPersistedState) persistedState
            );
            return new CoordinationServices() {
                @Override
                public ElectionStrategy getQuorumStrategy() {
                    return quorumStrategy;
                }

                @Override
                public Reconfigurator getReconfigurator() {
                    return reconfigurator;
                }

                @Override
                public LeaderHeartbeatService getLeaderHeartbeatService() {
                    return atomicHeartBeat;
                }
            };
        }

        @Override
        public CoordinationState.PersistedState createFreshPersistedState(DiscoveryNode localNode, BooleanSupplier disruptStorage) {
            return new AtomicRegisterPersistedState(localNode, atomicRegister, sharedStore);
        }

        @Override
        public CoordinationState.PersistedState createPersistedStateFromExistingState(
            DiscoveryNode newLocalNode,
            CoordinationState.PersistedState oldState,
            Function<Metadata, Metadata> adaptGlobalMetadata,
            Function<Long, Long> adaptCurrentTerm,
            LongSupplier currentTimeInMillisSupplier,
            NamedWriteableRegistry namedWriteableRegistry,
            BooleanSupplier disruptStorage
        ) {
            return new AtomicRegisterPersistedState(newLocalNode, atomicRegister, sharedStore);
        }

        @Override
        public CoordinationMetadata.VotingConfiguration getInitialConfigurationForNode(
            DiscoveryNode localNode,
            CoordinationMetadata.VotingConfiguration initialConfiguration
        ) {
            return new CoordinationMetadata.VotingConfiguration(Set.of(localNode.getId()));
        }
    }

    static class SingleNodeReconfigurator extends Reconfigurator {
        SingleNodeReconfigurator(Settings settings, ClusterSettings clusterSettings) {
            super(settings, clusterSettings);
        }

        @Override
        public CoordinationMetadata.VotingConfiguration reconfigure(
            Set<DiscoveryNode> liveNodes,
            Set<String> retiredNodeIds,
            DiscoveryNode currentMaster,
            CoordinationMetadata.VotingConfiguration currentConfig
        ) {
            return currentConfig;
        }

        @Override
        public ClusterState maybeReconfigureAfterNewMasterIsElected(ClusterState clusterState) {
            return ClusterState.builder(clusterState)
                .metadata(
                    Metadata.builder(clusterState.metadata())
                        .coordinationMetadata(
                            CoordinationMetadata.builder(clusterState.coordinationMetadata())
                                .lastAcceptedConfiguration(
                                    new CoordinationMetadata.VotingConfiguration(Set.of(clusterState.nodes().getMasterNodeId()))
                                )
                                .build()
                        )
                        .build()
                )
                .build();
        }
    }

    static class AtomicRegisterElectionStrategy extends ElectionStrategy {
        private final Function<Long, Optional<DiscoveryNode>> getLeaderForTermIfAlive;
        private final AtomicRegister register;
        private final SharedStore sharedStore;
        private final AtomicRegisterPersistedState atomicRegisterPersistedState;
        private long lastWonTerm = -1;

        AtomicRegisterElectionStrategy(
            AtomicRegister register,
            SharedStore sharedStore,
            Function<Long, Optional<DiscoveryNode>> getLeaderForTermIfAlive,
            AtomicRegisterPersistedState atomicRegisterPersistedState
        ) {
            this.getLeaderForTermIfAlive = getLeaderForTermIfAlive;
            this.sharedStore = sharedStore;
            this.register = register;
            this.atomicRegisterPersistedState = atomicRegisterPersistedState;
        }

        @Override
        protected boolean satisfiesAdditionalQuorumConstraints(
            DiscoveryNode localNode,
            long localCurrentTerm,
            long localAcceptedTerm,
            long localAcceptedVersion,
            CoordinationMetadata.VotingConfiguration lastCommittedConfiguration,
            CoordinationMetadata.VotingConfiguration lastAcceptedConfiguration,
            CoordinationState.VoteCollection joinVotes
        ) {
            return true;
        }

        @Override
        public boolean isElectionQuorum(
            DiscoveryNode localNode,
            long localCurrentTerm,
            long localAcceptedTerm,
            long localAcceptedVersion,
            CoordinationMetadata.VotingConfiguration lastCommittedConfiguration,
            CoordinationMetadata.VotingConfiguration lastAcceptedConfiguration,
            CoordinationState.VoteCollection joinVotes
        ) {
            if (lastWonTerm == localCurrentTerm) {
                return joinVotes.containsVoteFor(localNode);
            }

            // Safety is guaranteed by the blob store CAS, elect the current node immediately as
            // master and let the blob store decide whether this node should be the master.
            return lastCommittedConfiguration.isEmpty() == false
                && lastAcceptedConfiguration.isEmpty() == false
                && isLatestAcceptedStateOutdated(localAcceptedTerm, localAcceptedVersion) == false
                // if there's a leader that's not the local node wait, otherwise win the election immediately
                // (use the leader node id instead of equals to take into account restarts)
                && getLeaderForTermIfAlive.apply(localCurrentTerm).map(leader -> leader.getId().equals(localNode.getId())).orElse(true)
                && joinVotes.containsVoteFor(localNode);
        }

        private boolean isLatestAcceptedStateOutdated(long localAcceptedTerm, long localAcceptedVersion) {
            final var currentRegisterPersistentState = sharedStore.getLatestClusterState();
            if (currentRegisterPersistentState == null) {
                return false;
            }
            return currentRegisterPersistentState.term() > localAcceptedTerm
                || (currentRegisterPersistentState.term() == localAcceptedTerm
                    && currentRegisterPersistentState.version() != localAcceptedVersion);
        }

        @Override
        public boolean isPublishQuorum(
            CoordinationState.VoteCollection voteCollection,
            CoordinationMetadata.VotingConfiguration lastCommittedConfiguration,
            CoordinationMetadata.VotingConfiguration latestPublishedConfiguration
        ) {
            assert latestPublishedConfiguration.getNodeIds().size() == 1;

            return voteCollection.isQuorum(latestPublishedConfiguration);
        }

        @Override
        public boolean shouldJoinLeaderInTerm(long currentTerm, long targetTerm) {
            return currentTerm < targetTerm
                // Ensure that we can join an existing leader after reading the existing cluster state from the store
                || (currentTerm == targetTerm && currentTerm == atomicRegisterPersistedState.getInitialTermBeforeJoiningALeader());
        }

        @Override
        public void onNewElection(
            DiscoveryNode localNode,
            long proposedTerm,
            ClusterState latestAcceptedState,
            ActionListener<Void> listener
        ) {
            ActionListener.completeWith(listener, () -> {
                final var latestClusterState = sharedStore.getLatestClusterState();
                if (latestClusterState != null && latestClusterState.version() > latestAcceptedState.version()) {
                    throw new CoordinationStateRejectedException("The node has an stale applied cluster state version");
                }

                final var latestAcceptedClusterUUID = latestAcceptedState.metadata().clusterUUID();
                final var proposedNewTermOwner = new TermOwner(localNode, proposedTerm, latestAcceptedClusterUUID);
                final var witness = register.claimTerm(proposedNewTermOwner);
                if (proposedNewTermOwner != witness) {
                    if (witness.clusterUUID().equals(latestAcceptedClusterUUID) == false) {
                        assert latestAcceptedState.metadata().clusterUUIDCommitted() == false;
                        atomicRegisterPersistedState.changeAcceptedClusterUUID(witness.clusterUUID());
                    }
                    throw new CoordinationStateRejectedException("Term " + proposedTerm + " already claimed by another node");
                }
                lastWonTerm = proposedTerm;
                return null;
            });
        }

        @Override
        public boolean isInvalidReconfiguration(
            ClusterState clusterState,
            CoordinationMetadata.VotingConfiguration lastAcceptedConfiguration,
            CoordinationMetadata.VotingConfiguration lastCommittedConfiguration
        ) {
            // TODO: Move into a fixed dummy VotingConfiguration
            return false;
        }

        @Override
        public void beforeCommit(long term, long version) {
            // TODO: add a test to ensure that this gets called
            final var currentTermOwner = register.getTermOwner();
            if (currentTermOwner.term() > term) {
                throw new CoordinationStateRejectedException("Term " + term + " already claimed by another node");
            }
        }
    }

    record PersistentClusterState(long term, long version, Metadata state) {}

    record TermOwner(DiscoveryNode node, long term, String clusterUUID) {
        static TermOwner EMPTY = new TermOwner(null, 0, null);
    }

    static class SharedStore {
        private final Map<Long, PersistentClusterState> clusterStateByTerm = new HashMap<>();
        private final Map<Long, HeartBeat> heartBeatsByTerm = new HashMap<>();
        private final AtomicRegister register;

        SharedStore(AtomicRegister register) {
            this.register = register;
        }

        private void writeClusterState(ClusterState clusterState) {
            clusterStateByTerm.put(
                clusterState.term(),
                new PersistentClusterState(clusterState.term(), clusterState.version(), clusterState.metadata())
            );
        }

        private PersistentClusterState getLatestClusterState() {
            final var termOwner = register.getTermOwner();

            return getClusterStateForTerm(termOwner.term());
        }

        private PersistentClusterState getClusterStateForTerm(long termGoal) {
            for (long term = termGoal; term > 0; term--) {
                var persistedState = clusterStateByTerm.get(term);
                if (persistedState != null) {
                    return persistedState;
                }
            }
            return null;
        }

        private void writeHearBeat(long term, HeartBeat heartBeat) {
            HeartBeat previousHeartbeat = heartBeatsByTerm.put(term, heartBeat);
            assert previousHeartbeat == null || heartBeat.leader().equals(previousHeartbeat.leader());
        }

        private HeartBeat getHearbeatForTerm(long term) {
            return heartBeatsByTerm.get(term);
        }
    }

    static class AtomicRegister {
        private TermOwner currentTermOwner;

        private TermOwner getTermOwner() {
            return Objects.requireNonNullElse(currentTermOwner, TermOwner.EMPTY);
        }

        TermOwner claimTerm(TermOwner proposedNewTermOwner) {
            final var currentTermOwner = getTermOwner();

            if (currentTermOwner.term() >= proposedNewTermOwner.term()
                || (currentTermOwner != TermOwner.EMPTY
                    && currentTermOwner.clusterUUID().equals(proposedNewTermOwner.clusterUUID()) == false)) {
                return currentTermOwner;
            }

            return this.currentTermOwner = proposedNewTermOwner;
        }
    }

    class AtomicRegisterPersistedState implements CoordinationState.PersistedState {
        private final AtomicRegister atomicRegister;
        private final SharedStore sharedStore;
        private long initialTermBeforeJoiningALeader;
        private long currentTerm;
        private ClusterState latestAcceptedState;

        AtomicRegisterPersistedState(DiscoveryNode localNode, AtomicRegister atomicRegister, SharedStore sharedStore) {
            this.atomicRegister = atomicRegister;
            this.sharedStore = sharedStore;
            final var termOwner = atomicRegister.getTermOwner();
            final var currentState = sharedStore.getClusterStateForTerm(termOwner.term());
            if (currentState == null) {
                currentTerm = termOwner.term;
                latestAcceptedState = ClusterStateUpdaters.addStateNotRecoveredBlock(
                    clusterState(
                        0L,
                        0L,
                        localNode,
                        CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG,
                        CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG,
                        0L
                    )
                );
            } else {
                assert termOwner.term() >= currentState.term();
                currentTerm = Math.max(currentState.term, termOwner.term);
                latestAcceptedState = ClusterStateUpdaters.addStateNotRecoveredBlock(
                    ClusterState.builder(new ClusterName("elasticsearch"))
                        .metadata(currentState.state())
                        .version(currentState.version())
                        .nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build())
                        .build()
                );
            }
            initialTermBeforeJoiningALeader = currentTerm;
        }

        @Override
        public long getCurrentTerm() {
            return currentTerm;
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return latestAcceptedState;
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            if (currentTerm == initialTermBeforeJoiningALeader) {
                initialTermBeforeJoiningALeader = -1;
            }
            this.currentTerm = currentTerm;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            if (clusterState.nodes().isLocalNodeElectedMaster()) {
                writeClusterState(clusterState);
            }
            latestAcceptedState = clusterState;
        }

        void writeClusterState(ClusterState state) {
            final var termOwner = atomicRegister.getTermOwner();
            if (termOwner.term() > state.term()) {
                throw new RuntimeException("Conflicting cluster state update");
            }
            sharedStore.writeClusterState(state);
        }

        void changeAcceptedClusterUUID(String clusterUUID) {
            if (latestAcceptedState.metadata().clusterUUID().equals(clusterUUID) == false) {
                latestAcceptedState = ClusterState.builder(latestAcceptedState)
                    .metadata(Metadata.builder(latestAcceptedState.metadata()).clusterUUID(clusterUUID).build())
                    .build();
            }
        }

        long getInitialTermBeforeJoiningALeader() {
            return initialTermBeforeJoiningALeader;
        }

        @Override
        public void close() {
            assertTrue(openPersistedStates.remove(this));
        }
    }
}
