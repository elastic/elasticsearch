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
import org.elasticsearch.cluster.coordination.stateless.AtomicRegisterPreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.Heartbeat;
import org.elasticsearch.cluster.coordination.stateless.HeartbeatStore;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.MAX_MISSED_HEARTBEATS;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class AtomicRegisterCoordinatorTests extends CoordinatorTests {
    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testUnhealthyNodesGetsRemoved() {
        // This test checks that the voting configuration shrinks after a node is removed from the cluster
        // TODO: rewrite this test without taking into account the final voting configuration
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
        var sharedStore = new SharedStore();
        return new AtomicRegisterCoordinatorStrategy(atomicRegister, sharedStore);
    }

    class AtomicRegisterCoordinatorStrategy implements CoordinatorStrategy {
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
            var atomicHeartbeat = new StoreHeartbeatService(
                sharedStore,
                threadPool,
                heartbeatFrequency,
                TimeValue.timeValueMillis(heartbeatFrequency.millis() * MAX_MISSED_HEARTBEATS.get(settings)),
                atomicRegister::readCurrentTerm
            );
            var reconfigurator = new SingleNodeReconfigurator(settings, clusterSettings);
            var electionStrategy = new AtomicRegisterElectionStrategy(atomicRegister);
            return new CoordinationServices() {
                @Override
                public ElectionStrategy getElectionStrategy() {
                    return electionStrategy;
                }

                @Override
                public Reconfigurator getReconfigurator() {
                    return reconfigurator;
                }

                @Override
                public LeaderHeartbeatService getLeaderHeartbeatService() {
                    return atomicHeartbeat;
                }

                @Override
                public PreVoteCollector.Factory getPreVoteCollectorFactory() {
                    return (
                        transportService,
                        startElection,
                        updateMaxTermSeen,
                        electionStrategy,
                        nodeHealthService) -> new AtomicRegisterPreVoteCollector(atomicHeartbeat, startElection);
                }
            };
        }

        @Override
        public CoordinationState.PersistedState createFreshPersistedState(
            DiscoveryNode localNode,
            BooleanSupplier disruptStorage,
            ThreadPool threadPool
        ) {
            return new AtomicRegisterPersistedState(localNode, sharedStore);
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
            ThreadPool threadPool
        ) {
            return new AtomicRegisterPersistedState(newLocalNode, sharedStore);
        }
    }

    static class AtomicRegisterElectionStrategy extends ElectionStrategy {
        private final AtomicRegister register;

        AtomicRegisterElectionStrategy(AtomicRegister register) {
            this.register = register;
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
            assert lastCommittedConfiguration.isEmpty() == false;
            assert lastAcceptedConfiguration.isEmpty() == false;

            // Safety is guaranteed by the blob store CAS which guaranteed that we only create one StartJoinRequest per term, so elect as
            // the master the current node as soon as it has voted for itself.
            return joinVotes.containsVoteFor(localNode);
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
        public void onNewElection(DiscoveryNode localNode, long proposedTerm, ActionListener<StartJoinRequest> listener) {
            ActionListener.completeWith(listener, () -> {
                final var currentTerm = register.readCurrentTerm();
                final var electionTerm = Math.max(proposedTerm, currentTerm + 1);
                final var witness = register.compareAndExchange(currentTerm, electionTerm);
                if (witness != currentTerm) {
                    throw new CoordinationStateRejectedException("could not claim " + electionTerm + ", current term is " + witness);
                }
                return new StartJoinRequest(localNode, electionTerm);
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
        public void beforeCommit(long term, long version, ActionListener<Void> listener) {
            // TODO: add a test to ensure that this gets called
            final var currentTerm = register.readCurrentTerm();
            if (currentTerm == term) {
                listener.onResponse(null);
            } else {
                assert term < currentTerm : term + " vs " + currentTerm;
                listener.onFailure(
                    new CoordinationStateRejectedException(
                        Strings.format(
                            "could not commit cluster state version %d in term %d, current term is now %d",
                            version,
                            term,
                            currentTerm
                        )
                    )
                );
            }
        }
    }

    record PersistentClusterState(long term, long version, Metadata state) {}

    private static class SharedStore implements HeartbeatStore {
        private final Map<Long, PersistentClusterState> clusterStateByTerm = new HashMap<>();
        private Heartbeat heartbeat;

        private void writeClusterState(ClusterState clusterState) {
            clusterStateByTerm.put(
                clusterState.term(),
                new PersistentClusterState(clusterState.term(), clusterState.version(), clusterState.metadata())
            );
        }

        void getClusterStateForTerm(long termGoal, ActionListener<PersistentClusterState> listener) {
            ActionListener.completeWith(listener, () -> {
                for (long term = termGoal; term > 0; term--) {
                    var persistedState = clusterStateByTerm.get(term);
                    if (persistedState != null) {
                        return persistedState;
                    }
                }
                return null;
            });
        }

        @Override
        public void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
            this.heartbeat = newHeartbeat;
            listener.onResponse(null);
        }

        @Override
        public void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
            listener.onResponse(heartbeat);
        }
    }

    private static class AtomicRegister {
        private long currentTerm;

        long readCurrentTerm() {
            return currentTerm;
        }

        long compareAndExchange(long expected, long updated) {
            final var witness = currentTerm;
            if (currentTerm == expected) {
                currentTerm = updated;
            }
            return witness;
        }
    }

    class AtomicRegisterPersistedState implements CoordinationState.PersistedState {
        private final DiscoveryNode localNode;
        private final SharedStore sharedStore;
        private long currentTerm;
        private ClusterState latestAcceptedState;

        AtomicRegisterPersistedState(DiscoveryNode localNode, SharedStore sharedStore) {
            this.localNode = localNode;
            this.sharedStore = sharedStore;
            this.latestAcceptedState = ClusterStateUpdaters.addStateNotRecoveredBlock(
                clusterState(
                    0L,
                    0L,
                    localNode,
                    CoordinationMetadata.VotingConfiguration.of(localNode),
                    CoordinationMetadata.VotingConfiguration.of(localNode),
                    0L
                )
            );
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
                sharedStore.writeClusterState(clusterState);
            }
            latestAcceptedState = clusterState;
        }

        @Override
        public void close() {
            assertTrue(openPersistedStates.remove(this));
        }

        @Override
        public void getLatestStoredState(long term, ActionListener<ClusterState> listener) {
            sharedStore.getClusterStateForTerm(term - 1, listener.map(latestClusterState -> {
                if (latestClusterState == null) {
                    return null;
                }

                if (isLatestAcceptedStateStale(latestClusterState) == false) {
                    return null;
                }

                if (latestClusterState.term() > currentTerm) {
                    return null;
                }

                return ClusterStateUpdaters.recoverClusterBlocks(
                    ClusterStateUpdaters.addStateNotRecoveredBlock(
                        ClusterState.builder(ClusterName.DEFAULT)
                            .metadata(
                                Metadata.builder(latestClusterState.state())
                                    .coordinationMetadata(
                                        new CoordinationMetadata(
                                            latestClusterState.term(),
                                            // Keep the previous configuration so the assertions don't complain about a different committed
                                            // configuration, we'll change it right away
                                            latestAcceptedState.getLastCommittedConfiguration(),
                                            CoordinationMetadata.VotingConfiguration.of(localNode),
                                            Set.of()
                                        )
                                    )
                            )
                            .version(latestClusterState.version())
                            .nodes(DiscoveryNodes.builder(latestAcceptedState.nodes()).masterNodeId(null))
                            .build()
                    )
                );
            }));
        }

        boolean isLatestAcceptedStateStale(PersistentClusterState latestClusterState) {
            return latestClusterState.state().clusterUUID().equals(latestAcceptedState.metadata().clusterUUID()) == false
                || latestClusterState.term() > latestAcceptedState.term()
                || (latestClusterState.term() == latestAcceptedState.term()
                    && latestClusterState.version() > latestAcceptedState.version());
        }
    }
}
