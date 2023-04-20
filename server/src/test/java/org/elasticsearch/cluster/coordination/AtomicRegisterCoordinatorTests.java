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
import org.elasticsearch.cluster.coordination.stateless.DisruptibleHeartbeatStore;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.MAX_MISSED_HEARTBEATS;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class AtomicRegisterCoordinatorTests extends CoordinatorTests {

    @Override
    public void testLeaderDisconnectionWithDisconnectEventDetectedQuickly() {
        // must allow a little extra time for the heartbeat to expire before the election can happen
        testLeaderDisconnectionWithDisconnectEventDetectedQuickly(
            Settings.builder()
                .put(MAX_MISSED_HEARTBEATS.getKey(), 1)
                .put(HEARTBEAT_FREQUENCY.getKey(), TimeValue.timeValueSeconds(1))
                .build(),
            TimeValue.timeValueSeconds(1)
        );
    }

    @Override
    @AwaitsFix(bugUrl = "ES-5645")
    public void testUnhealthyLeaderIsReplaced() {
        // In this test the leader still has access to the register, therefore it is still considered as a leader.
    }

    @Override
    @TestLogging(
        reason = "testing ClusterFormationFailureHelper logging",
        value = "org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper:WARN"
    )
    public void testLogsWarningPeriodicallyIfClusterNotFormed() {
        testLogsWarningPeriodicallyIfClusterNotFormed(
            "master not discovered or elected yet, an election requires a node with id [",
            nodeId -> "*have discovered possible quorum *" + nodeId + "*discovery will continue*"
        );
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
    public void testUnhealthyNodesGetsRemoved() {
        // the test still applies with an atomic register, except for the assertions about the voting configuration
        testUnhealthyNodesGetsRemoved(false);
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
        return new AtomicRegisterCoordinatorStrategy();
    }

    class AtomicRegisterCoordinatorStrategy implements CoordinatorStrategy {
        private final AtomicLong currentTermRef = new AtomicLong();
        private final AtomicReference<Heartbeat> heartBeatRef = new AtomicReference<>();
        private final SharedStore sharedStore = new SharedStore();

        @Override
        public CoordinationServices getCoordinationServices(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            CoordinationState.PersistedState persistedState,
            BooleanSupplier isDisruptedSupplier
        ) {
            final TimeValue heartbeatFrequency = HEARTBEAT_FREQUENCY.get(settings);
            final var atomicRegister = new AtomicRegister(currentTermRef, isDisruptedSupplier);
            final var atomicHeartbeat = new StoreHeartbeatService(new DisruptibleHeartbeatStore(new SharedHeartbeatStore(heartBeatRef)) {
                @Override
                protected boolean isDisrupted() {
                    return isDisruptedSupplier.getAsBoolean();
                }
            },
                threadPool,
                heartbeatFrequency,
                TimeValue.timeValueMillis(heartbeatFrequency.millis() * MAX_MISSED_HEARTBEATS.get(settings)),
                listener -> ActionListener.completeWith(listener, () -> OptionalLong.of(atomicRegister.readCurrentTerm()))
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
                        nodeHealthService,
                        leaderHeartbeatService) -> new AtomicRegisterPreVoteCollector(atomicHeartbeat, startElection);
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
            ActionListener.completeWith(listener, () -> {
                final var currentTerm = register.readCurrentTerm();
                if (currentTerm == term) {
                    return null;
                } else {
                    assert term < currentTerm : term + " vs " + currentTerm;
                    throw new CoordinationStateRejectedException(
                        Strings.format(
                            "could not commit cluster state version %d in term %d, current term is now %d",
                            version,
                            term,
                            currentTerm
                        )
                    );
                }
            });
        }
    }

    record PersistentClusterState(long term, long version, Metadata state) {}

    private static class SharedStore {
        private final Map<Long, PersistentClusterState> clusterStateByTerm = new HashMap<>();

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
    }

    private static class SharedHeartbeatStore implements HeartbeatStore {

        private final AtomicReference<Heartbeat> hearbeatRef;

        SharedHeartbeatStore(AtomicReference<Heartbeat> hearbeatRef) {
            this.hearbeatRef = hearbeatRef;
        }

        @Override
        public void writeHeartbeat(Heartbeat newHeartbeat, ActionListener<Void> listener) {
            hearbeatRef.set(newHeartbeat);
            listener.onResponse(null);
        }

        @Override
        public void readLatestHeartbeat(ActionListener<Heartbeat> listener) {
            listener.onResponse(hearbeatRef.get());
        }
    }

    private static class AtomicRegister {
        private final AtomicLong currentTermRef;
        private final BooleanSupplier isDisruptedSupplier;

        AtomicRegister(AtomicLong currentTermRef, BooleanSupplier isDisruptedSupplier) {
            this.currentTermRef = currentTermRef;
            this.isDisruptedSupplier = isDisruptedSupplier;
        }

        private boolean isDisrupted() {
            return isDisruptedSupplier.getAsBoolean();
        }

        long readCurrentTerm() throws IOException {
            if (isDisrupted()) {
                throw new IOException("simulating disrupted access to shared store");
            }
            return currentTermRef.get();
        }

        long compareAndExchange(long expected, long updated) throws IOException {
            if (isDisrupted()) {
                throw new IOException("simulating disrupted access to shared store");
            }
            return currentTermRef.compareAndExchange(expected, updated);
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
