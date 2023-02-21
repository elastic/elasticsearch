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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class BlobStoreBasedCoordinatorTests extends CoordinatorTests {
    private BlobStore blobStore;

    @Before
    public void setUpNewBlobStore() {
        blobStore = new BlobStore();
    }

    @Override
    protected CoordinatorStrategy getCoordinatorStrategy() {
        return new BlobStoreCoordinatorStrategy(blobStore);
    }

    class BlobStoreCoordinatorStrategy implements CoordinatorStrategy {
        private final BlobStore blobStore;

        BlobStoreCoordinatorStrategy(BlobStore blobStore) {
            this.blobStore = blobStore;
        }

        @Override
        public ElectionStrategy getElectionStrategy() {
            return new SingleNodeElectionStrategy();
        }

        @Override
        public Reconfigurator getReconfigurator(Settings settings, ClusterSettings clusterSettings) {
            return new SingleNodeReconfigurator(settings, clusterSettings);
        }

        @Override
        public CoordinationState.PersistedState createFreshPersistedState(DiscoveryNode localNode, BooleanSupplier disruptStorage) {
            return new BlobStorePersistedState(localNode, blobStore);
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
            return new BlobStorePersistedState(newLocalNode, blobStore);
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
                                .build()
                        )
                )
                .build();
        }
    }

    static class SingleNodeElectionStrategy extends ElectionStrategy {
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
            // Safety is guaranteed by the blob store CAS, elect the current node immediately as
            // master and let the blob store decide whether this node should be the master.
            return lastAcceptedConfiguration.isEmpty() == false
                && lastAcceptedConfiguration.isEmpty() == false
                && joinVotes.containsVoteFor(localNode);
        }
    }

    record PersistentClusterState(long term, long version, Metadata state) {}

    static class BlobStore {
        private final AtomicReference<PersistentClusterState> clusterState = new AtomicReference<>();

        PersistentClusterState readClusterState() {
            return clusterState.get();
        }

        boolean compareAndSetClusterState(PersistentClusterState expectedValue, PersistentClusterState newValue) {
            return clusterState.compareAndSet(expectedValue, newValue);
        }
    }

    class BlobStorePersistedState implements CoordinationState.PersistedState {
        private final BlobStore blobStore;
        private long currentTerm;
        private ClusterState latestAcceptedState;
        private PersistentClusterState latestPersistedState;

        BlobStorePersistedState(DiscoveryNode localNode, BlobStore blobStore) {
            this.blobStore = blobStore;
            var currentState = blobStore.readClusterState();
            if (currentState == null) {
                this.currentTerm = 0;
                this.latestAcceptedState = ClusterStateUpdaters.addStateNotRecoveredBlock(
                    clusterState(
                        0L,
                        0L,
                        localNode,
                        CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG,
                        CoordinationMetadata.VotingConfiguration.EMPTY_CONFIG,
                        0L
                    )
                );
                this.latestPersistedState = null;
            } else {
                this.currentTerm = currentState.term();
                this.latestAcceptedState = ClusterStateUpdaters.addStateNotRecoveredBlock(
                    ClusterState.builder(new ClusterName("elasticsearch"))
                        .metadata(currentState.state())
                        .version(currentState.version())
                        .nodes(DiscoveryNodes.builder().localNodeId(localNode.getId()).add(localNode).build())
                        .build()
                );
                this.latestPersistedState = currentState;
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
            final var newPersistedState = new PersistentClusterState(currentTerm, state.version(), state.metadata());
            if (blobStore.compareAndSetClusterState(latestPersistedState, newPersistedState) == false) {
                latestPersistedState = blobStore.readClusterState();
                throw new RuntimeException("Conflicting cluster state update");
            }
            latestPersistedState = newPersistedState;
        }

        @Override
        public void markLastAcceptedStateAsCommitted() {
            CoordinationState.PersistedState.super.markLastAcceptedStateAsCommitted();
            var state = latestAcceptedState;
            if (state.nodes().isLocalNodeElectedMaster() == false) {
                latestPersistedState = new PersistentClusterState(currentTerm, state.version(), state.metadata());
            }
        }

        @Override
        public void close() {
            assertTrue(openPersistedStates.remove(this));
        }
    }
}
