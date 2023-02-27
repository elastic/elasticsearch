/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class AtomicRegisterCoordinatorTests extends CoordinatorTests {
    private KeyedAtomicRegister keyedAtomicRegister;

    @Before
    public void setUpNewRegister() {
        keyedAtomicRegister = new KeyedAtomicRegister();
    }

    @Before
    public void enableCoordinatorRegisterMode() {
        Coordinator.REGISTER_COORDINATION_MODE_ENABLED = true;
    }

    @After
    public void disableCoordinatorRegisterMode() {
        Coordinator.REGISTER_COORDINATION_MODE_ENABLED = false;
    }

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
        return new AtomicRegisterCoordinatorStrategy(keyedAtomicRegister);
    }

    record HeartBeat(DiscoveryNode leader, long absoluteTimeInMillis) {
        long timeSinceLastHeartbeatInMillis(long nowInMillis) {
            return nowInMillis - absoluteTimeInMillis;
        }
    }

    static class AtomicRegisterHeartbeatService implements LeaderHeartbeatService {
        private final KeyedAtomicRegister keyedAtomicRegister;
        private final ThreadPool threadPool;
        private final String registerKey;
        private final TimeValue heartbeatFrequency;
        private final TimeValue maxTimeSinceLastHeartbeat;

        private DiscoveryNode currentLeader;
        private Scheduler.Cancellable heartbeatTask;
        private HeartBeat latestHeartBeat;

        AtomicRegisterHeartbeatService(
            KeyedAtomicRegister keyedAtomicRegister,
            ThreadPool threadPool,
            String registerKey,
            TimeValue heartbeatFrequency,
            TimeValue maxTimeSinceLastHeartbeat
        ) {
            this.keyedAtomicRegister = keyedAtomicRegister;
            this.threadPool = threadPool;
            this.registerKey = registerKey;
            this.heartbeatFrequency = heartbeatFrequency;
            this.maxTimeSinceLastHeartbeat = maxTimeSinceLastHeartbeat;
        }

        @Override
        public void start(DiscoveryNode currentLeader) {
            this.currentLeader = currentLeader;

            sendHeartBeatToBlobStore();
            this.heartbeatTask = threadPool.scheduleWithFixedDelay(
                this::sendHeartBeatToBlobStore,
                heartbeatFrequency,
                ThreadPool.Names.GENERIC
            );
        }

        private void sendHeartBeatToBlobStore() {
            var heartBeat = new HeartBeat(this.currentLeader, threadPool.absoluteTimeInMillis());
            keyedAtomicRegister.setHeartBeat(registerKey, heartBeat);
            this.latestHeartBeat = heartBeat;
        }

        @Override
        public void stop() {
            this.currentLeader = null;
            var heartBeatTask = this.heartbeatTask;
            if (heartBeatTask != null) {
                heartBeatTask.cancel();
            }
            keyedAtomicRegister.removeHeartBeat(registerKey, latestHeartBeat);
        }

        private Optional<DiscoveryNode> getCurrentLeaderFromRegister() {
            var latestHeartBeat = keyedAtomicRegister.latestHeartBeat(registerKey);
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
        static final Setting<String> REGISTER_KEY = Setting.simpleString("register_key", "default", Setting.Property.NodeScope);
        static final Setting<TimeValue> HEARTBEAT_FREQUENCY = Setting.timeSetting(
            "heartbeat_frequency",
            TimeValue.timeValueSeconds(15),
            Setting.Property.NodeScope
        );
        static final Setting<Integer> MAX_MISSED_HEARTBEATS = Setting.intSetting("max_missed_heartbeats", 2, 1, Setting.Property.NodeScope);

        private final KeyedAtomicRegister keyedAtomicRegister;

        AtomicRegisterCoordinatorStrategy(KeyedAtomicRegister keyedAtomicRegister) {
            this.keyedAtomicRegister = keyedAtomicRegister;
        }

        @Override
        public CoordinationServices getCoordinationServices(ThreadPool threadPool, Settings settings, ClusterSettings clusterSettings) {
            final TimeValue heartbeatFrequency = HEARTBEAT_FREQUENCY.get(settings);
            var atomicHeartBeat = new AtomicRegisterHeartbeatService(
                keyedAtomicRegister,
                threadPool,
                REGISTER_KEY.get(settings),
                heartbeatFrequency,
                TimeValue.timeValueMillis(heartbeatFrequency.millis() * MAX_MISSED_HEARTBEATS.get(settings))
            );
            var reconfigurator = new SingleNodeReconfigurator(settings, clusterSettings);
            var quorumStrategy = new AtomicRegisterQuorumStrategy(atomicHeartBeat::getCurrentLeaderFromRegister);
            return new CoordinationServices() {
                @Override
                public QuorumStrategy getQuorumStrategy() {
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
        public CoordinationState.PersistedState createFreshPersistedState(
            DiscoveryNode localNode,
            BooleanSupplier disruptStorage,
            Settings settings
        ) {
            return new AtomicRegisterPersistedState(localNode, REGISTER_KEY.get(settings), keyedAtomicRegister);
        }

        @Override
        public CoordinationState.PersistedState createPersistedStateFromExistingState(
            DiscoveryNode newLocalNode,
            CoordinationState.PersistedState oldState,
            Function<Metadata, Metadata> adaptGlobalMetadata,
            Function<Long, Long> adaptCurrentTerm,
            LongSupplier currentTimeInMillisSupplier,
            NamedWriteableRegistry namedWriteableRegistry,
            BooleanSupplier disruptStorage,
            Settings settings
        ) {
            return new AtomicRegisterPersistedState(newLocalNode, REGISTER_KEY.get(settings), keyedAtomicRegister);
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
            assert currentConfig.hasQuorum(Set.of(currentMaster.getId()));
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
                                .lastCommittedConfiguration(
                                    new CoordinationMetadata.VotingConfiguration(Set.of(clusterState.nodes().getMasterNodeId()))
                                )
                                .build()
                        )
                )
                .build();
        }
    }

    static class AtomicRegisterQuorumStrategy extends QuorumStrategy {
        private final Supplier<Optional<DiscoveryNode>> currentLeaderSupplier;

        AtomicRegisterQuorumStrategy(Supplier<Optional<DiscoveryNode>> currentLeaderSupplier) {
            this.currentLeaderSupplier = currentLeaderSupplier;
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
            // Safety is guaranteed by the blob store CAS, elect the current node immediately as
            // master and let the blob store decide whether this node should be the master.
            return lastCommittedConfiguration.isEmpty() == false && lastAcceptedConfiguration.isEmpty() == false
            // if there's a leader that's not the local node wait, otherwise win the election immediately
                && currentLeaderSupplier.get().map(f -> f.equals(localNode)).orElse(true)
                && joinVotes.containsVoteFor(localNode);
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
        public boolean shouldJoinLeaderInTerm(long currentTerm, long targetTerm, Optional<Join> lastJoin) {
            // Ensure that when we become candidate, and we've read the cluster state from a register we update our info
            return currentTerm < targetTerm || currentTerm == targetTerm && lastJoin.isEmpty();
        }

        @Override
        public boolean isValidStartJoinRequest(StartJoinRequest startJoinRequest, long currentTerm) {
            return startJoinRequest.getTerm() > currentTerm;
        }
    }

    record PersistentClusterState(long term, long version, Metadata state) {
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            PersistentClusterState that = (PersistentClusterState) o;
            return term == that.term
                && version == that.version
                && Objects.equals(state.coordinationMetadata(), that.state().coordinationMetadata());
        }

        @Override
        public int hashCode() {
            return Objects.hash(term, version, state.coordinationMetadata());
        }
    }

    static class KeyedAtomicRegister {
        private final Map<String, PersistentClusterState> clusterStates = new HashMap<>();
        private final Map<String, HeartBeat> heartBeats = new HashMap<>();

        PersistentClusterState readClusterState(String key) {
            return clusterStates.get(key);
        }

        boolean compareAndSetClusterState(String key, PersistentClusterState expectedValue, PersistentClusterState newValue) {
            final var currentValue = clusterStates.get(key);
            if (Objects.equals(expectedValue, currentValue)) {
                clusterStates.put(key, newValue);
                return true;
            } else {
                return false;
            }
        }

        void setHeartBeat(String key, HeartBeat heartBeat) {
            heartBeats.put(key, heartBeat);
        }

        @Nullable
        HeartBeat latestHeartBeat(String key) {
            return heartBeats.get(key);
        }

        void removeHeartBeat(String key, HeartBeat latestHeartBeat) {
            heartBeats.remove(key, latestHeartBeat);
        }
    }

    class AtomicRegisterPersistedState implements CoordinationState.PersistedState {
        private final DiscoveryNode localNode;
        private final KeyedAtomicRegister keyedAtomicRegister;
        private final String registerKey;
        private long currentTerm;
        private ClusterState latestAcceptedState;
        private boolean isLatestAcceptedStateFromRegister;

        AtomicRegisterPersistedState(DiscoveryNode localNode, String registerKey, KeyedAtomicRegister keyedAtomicRegister) {
            this.localNode = localNode;
            this.keyedAtomicRegister = keyedAtomicRegister;
            this.registerKey = registerKey;
            loadClusterStateFromRegister();
        }

        private void loadClusterStateFromRegister() {
            final var currentState = keyedAtomicRegister.readClusterState(registerKey);
            if (currentState == null) {
                currentTerm = 0;
                latestAcceptedState = ClusterStateUpdaters.addStateNotRecoveredBlock(
                    clusterState(
                        0L,
                        0L,
                        this.localNode,
                        CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG,
                        CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG,
                        0L
                    )
                );
                isLatestAcceptedStateFromRegister = false;
            } else {
                currentTerm = Math.max(currentState.term(), currentTerm);
                latestAcceptedState = ClusterStateUpdaters.addStateNotRecoveredBlock(
                    ClusterState.builder(new ClusterName("elasticsearch"))
                        .metadata(currentState.state())
                        .version(currentState.version())
                        .nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build())
                        .build()
                );
                isLatestAcceptedStateFromRegister = true;
            }
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
            this.currentTerm = currentTerm;
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            if (clusterState.nodes().isLocalNodeElectedMaster()) {
                writeClusterState(clusterState);
            }
            this.latestAcceptedState = clusterState;
        }

        void writeClusterState(ClusterState state) {
            final var newPersistedState = new PersistentClusterState(state.term(), state.version(), state.metadata());
            final var expectedPersistedState = isLatestAcceptedStateFromRegister == false
                ? null
                : new PersistentClusterState(latestAcceptedState.term(), latestAcceptedState.version(), latestAcceptedState.metadata());
            assert expectedPersistedState == null || expectedPersistedState.term() <= newPersistedState.term();
            if (keyedAtomicRegister.compareAndSetClusterState(registerKey, expectedPersistedState, newPersistedState) == false) {
                // TODO: call updateMaxTermSeen?
                loadClusterStateFromRegister();
                throw new RuntimeException("Conflicting cluster state update");
            }
            this.isLatestAcceptedStateFromRegister = true;
        }

        @Override
        public void close() {
            assertTrue(openPersistedStates.remove(this));
        }
    }
}
