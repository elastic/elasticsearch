/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.AtomicRegisterCoordinatorTests;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.AtomicRegisterCoordinatorTests.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.coordination.AtomicRegisterCoordinatorTests.StoreHeartbeatService.MAX_MISSED_HEARTBEATS;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class StatelessCoordinationTests extends AtomicRegisterCoordinatorTests {
    FakeStatelessNode statelessNode;

    @Before
    public void setUpBlobStore() throws IOException {
        this.statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry());
    }

    @After
    public void tearDownBlobStore() {
        IOUtils.closeWhileHandlingException(statelessNode);
    }

    @Override
    protected CoordinatorStrategy getCoordinatorStrategy() {
        final var inMemoryClusterStateStore = new InMemoryClusterStateStore();
        final var inMemoryHeartBeatStore = new InMemoryHeartBeatStore();
        return new StatelessCoordinatorStrategy(
            inMemoryClusterStateStore,
            inMemoryHeartBeatStore,
            statelessNode.objectStoreService.getTermLeaseBlobContainer()
        );
    }

    class StatelessCoordinatorStrategy implements CoordinatorStrategy {
        private final ClusterStateStore clusterStateStore;
        private final HeartbeatStore heartBeatStore;
        private final BlobContainer termLeaseContainer;

        StatelessCoordinatorStrategy(ClusterStateStore clusterStateStore, HeartbeatStore heartBeatStore, BlobContainer termLeaseContainer) {
            this.clusterStateStore = clusterStateStore;
            this.heartBeatStore = heartBeatStore;
            this.termLeaseContainer = termLeaseContainer;
        }

        @Override
        public CoordinationServices getCoordinationServices(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            CoordinationState.PersistedState persistedState
        ) {
            final var statelessElectionStrategy = new StatelessElectionStrategy(termLeaseContainer);
            final var heartbeatFrequency = HEARTBEAT_FREQUENCY.get(settings);
            final var storeHeartbeatService = new StoreHeartbeatService(
                heartBeatStore,
                threadPool,
                heartbeatFrequency,
                TimeValue.timeValueMillis(heartbeatFrequency.millis() * MAX_MISSED_HEARTBEATS.get(settings)),
                () -> PlainActionFuture.get(statelessElectionStrategy::getCurrentLeaseTerm).orElse(0L)
            );

            return new CoordinationServices() {
                @Override
                public ElectionStrategy getElectionStrategy() {
                    return statelessElectionStrategy;
                }

                @Override
                public Reconfigurator getReconfigurator() {
                    return new SingleNodeReconfigurator(settings, clusterSettings);
                }

                @Override
                public LeaderHeartbeatService getLeaderHeartbeatService() {
                    return storeHeartbeatService;
                }

                @Override
                public PreVoteCollector.Factory getPreVoteCollectorFactory() {
                    return (
                        transportService,
                        startElection,
                        updateMaxTermSeen,
                        electionStrategy,
                        nodeHealthService) -> new AtomicRegisterPreVoteCollector(storeHeartbeatService, startElection);
                }
            };
        }

        @Override
        public CoordinationState.PersistedState createFreshPersistedState(
            DiscoveryNode localNode,
            BooleanSupplier disruptStorage,
            ThreadPool threadPool
        ) {
            return new StatelessPersistedState(emptyClusterState(localNode), 0, clusterStateStore) {
                @Override
                public void close() throws IOException {
                    super.close();
                    assertTrue(openPersistedStates.remove(this));
                }
            };
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
            return new StatelessPersistedState(emptyClusterState(newLocalNode), 0, clusterStateStore) {
                @Override
                public void close() throws IOException {
                    super.close();
                    assertTrue(openPersistedStates.remove(this));
                }
            };
        }
    }

    private ClusterState emptyClusterState(DiscoveryNode localNode) {
        return ClusterStateUpdaters.addStateNotRecoveredBlock(
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

    private static class InMemoryHeartBeatStore implements AtomicRegisterCoordinatorTests.HeartbeatStore {
        private Heartbeat heartbeat;

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
}
