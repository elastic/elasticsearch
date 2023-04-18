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
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.stateless.AtomicRegisterPreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.Heartbeat;
import org.elasticsearch.cluster.coordination.stateless.HeartbeatStore;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.MAX_MISSED_HEARTBEATS;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class StatelessCoordinationTests extends AtomicRegisterCoordinatorTests {
    @Override
    protected CoordinatorStrategy getCoordinatorStrategy() {
        final var inMemoryHeartBeatStore = new InMemoryHeartBeatStore();
        try {
            var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry());
            return new StatelessCoordinatorStrategy(
                inMemoryHeartBeatStore,
                statelessNode.objectStoreService.getTermLeaseBlobContainer(),
                statelessNode
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    class StatelessCoordinatorStrategy implements CoordinatorStrategy {
        private final HeartbeatStore heartBeatStore;
        private final BlobContainer termLeaseContainer;
        private final FakeStatelessNode statelessNode;

        StatelessCoordinatorStrategy(HeartbeatStore heartBeatStore, BlobContainer termLeaseContainer, FakeStatelessNode statelessNode) {
            this.heartBeatStore = heartBeatStore;
            this.termLeaseContainer = termLeaseContainer;
            this.statelessNode = statelessNode;
        }

        @Override
        public CoordinationServices getCoordinationServices(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            CoordinationState.PersistedState persistedState
        ) {
            final var statelessElectionStrategy = new StatelessElectionStrategy(() -> termLeaseContainer, threadPool) {
                @Override
                protected String getExecutorName() {
                    return ThreadPool.Names.SAME;
                }
            };
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
                        nodeHealthService,
                        leaderHeartbeatService) -> new AtomicRegisterPreVoteCollector(storeHeartbeatService, startElection);
                }
            };
        }

        @Override
        public CoordinationState.PersistedState createFreshPersistedState(
            DiscoveryNode localNode,
            BooleanSupplier disruptStorage,
            ThreadPool threadPool
        ) {
            return getPersistedState(localNode, threadPool);
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
            return getPersistedState(newLocalNode, threadPool);
        }

        private CoordinationState.PersistedState getPersistedState(DiscoveryNode localNode, ThreadPool threadPool) {
            try {
                final var nodeEnvironment = newNodeEnvironment();
                final var persistedClusterStateService = new StatelessPersistedClusterStateService(
                    nodeEnvironment,
                    xContentRegistry(),
                    new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                    threadPool::relativeTimeInMillis,
                    () -> statelessNode.objectStoreService,
                    threadPool
                ) {
                    @Override
                    protected String getUploadsThreadPool() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    protected String getDownloadsThreadPool() {
                        return ThreadPool.Names.SAME;
                    }
                };
                return new FilterPersistedState(persistedClusterStateService.createPersistedState(Settings.EMPTY, localNode)) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        assertTrue(openPersistedStates.remove(this));
                        IOUtils.close(nodeEnvironment);
                    }
                };
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            IOUtils.closeWhileHandlingException(statelessNode);
        }
    }

    static class FilterPersistedState implements CoordinationState.PersistedState {
        private final CoordinationState.PersistedState delegate;

        FilterPersistedState(CoordinationState.PersistedState delegate) {
            this.delegate = delegate;
        }

        @Override
        public long getCurrentTerm() {
            return delegate.getCurrentTerm();
        }

        @Override
        public ClusterState getLastAcceptedState() {
            return delegate.getLastAcceptedState();
        }

        @Override
        public void setCurrentTerm(long currentTerm) {
            delegate.setCurrentTerm(currentTerm);
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            delegate.setLastAcceptedState(clusterState);
        }

        @Override
        public void getLatestStoredState(long term, ActionListener<ClusterState> listener) {
            delegate.getLatestStoredState(term, listener);
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }

    private static class InMemoryHeartBeatStore implements HeartbeatStore {
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
