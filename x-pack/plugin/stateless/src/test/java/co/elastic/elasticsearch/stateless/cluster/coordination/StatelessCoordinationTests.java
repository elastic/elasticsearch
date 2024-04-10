/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import co.elastic.elasticsearch.stateless.test.FakeStatelessNode;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase;
import org.elasticsearch.cluster.coordination.AtomicRegisterCoordinatorTests;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.Coordinator;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.LeaderHeartbeatService;
import org.elasticsearch.cluster.coordination.PreVoteCollector;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.coordination.stateless.AtomicRegisterPreVoteCollector;
import org.elasticsearch.cluster.coordination.stateless.DisruptibleHeartbeatStore;
import org.elasticsearch.cluster.coordination.stateless.Heartbeat;
import org.elasticsearch.cluster.coordination.stateless.HeartbeatStore;
import org.elasticsearch.cluster.coordination.stateless.SingleNodeReconfigurator;
import org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.version.CompatibilityVersionsUtils;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.FilterBlobContainer;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;

import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.DEFAULT_CLUSTER_STATE_UPDATE_DELAY;
import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.DEFAULT_ELECTION_DELAY;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.HEARTBEAT_FREQUENCY;
import static org.elasticsearch.cluster.coordination.stateless.StoreHeartbeatService.MAX_MISSED_HEARTBEATS;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeLong;
import static org.elasticsearch.test.MockLogAppender.assertThatLogger;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class StatelessCoordinationTests extends AtomicRegisterCoordinatorTests {
    private static final Logger logger = LogManager.getLogger(StatelessCoordinationTests.class);

    @Override
    protected CoordinatorStrategy createCoordinatorStrategy() {
        final var inMemoryHeartBeatStore = new InMemoryHeartBeatStore();
        try {
            var statelessNode = new FakeStatelessNode(this::newEnvironment, this::newNodeEnvironment, xContentRegistry());
            return new StatelessCoordinatorStrategy(
                inMemoryHeartBeatStore,
                statelessNode.objectStoreService.getClusterStateBlobContainer(),
                statelessNode
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void testWarnLoggingOnRegisterFailures() {
        // This test injects failures using some features specific to the AtomicRegisterCoordinatorStrategy so it doesn't make sense here
    }

    /**
     * A node that is in the process of shutting down can try to run for election, but should fail locally with an error message due to shut
     * down in progress. This test sets a node as shutting down (in the cluster state), forces it to run for election, and then verifies
     * that an error message gets logged and the node refuses to run for election.
     */
    @TestLogging(reason = "test uses a Coordinator log msg", value = "org.elasticsearch.cluster.coordination.Coordinator:TRACE")
    public void testShutdownLogMessageDuringElectionAttempt() {
        // There are two nodes so that a non-leader can be set to shutdown and then made to try to run for election.
        try (AbstractCoordinatorTestCase.Cluster cluster = new AbstractCoordinatorTestCase.Cluster(randomIntBetween(2, 5))) {
            logger.info("---> Running a test cluster of [" + cluster.size() + "] nodes");
            cluster.runRandomly();
            cluster.stabilise();

            final AbstractCoordinatorTestCase.Cluster.ClusterNode leader = cluster.getAnyLeader();
            final AbstractCoordinatorTestCase.Cluster.ClusterNode nonLeader = cluster.getAnyNodeExcept(leader);
            ClusterState originalClusterState = cluster.getAnyLeader().getLastAppliedClusterState();

            // Update the cluster state to say that nonLeader is shutting down. We do not need to worry about shutdown actually taking
            // place because the base test class' ClusterNode implementation doesn't include shutdown logic.
            {
                SingleNodeShutdownMetadata.Type type = SingleNodeShutdownMetadata.Type.RESTART;
                NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(
                    Collections.singletonMap(
                        nonLeader.getLocalNode().getId(),
                        SingleNodeShutdownMetadata.builder()
                            .setNodeId(nonLeader.getLocalNode().getId())
                            .setReason("shutdown for a unit test")
                            .setType(type)
                            .setStartedAtMillis(randomNonNegativeLong())
                            .setGracePeriod(null)
                            .build()
                    )
                );
                final ClusterState shutdownClusterState = ClusterState.builder(originalClusterState)
                    .metadata(
                        Metadata.builder(originalClusterState.metadata())
                            .putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata)
                            .build()
                    )
                    .version(originalClusterState.version() + 1)
                    .build();

                leader.submitUpdateTask("set node shutting down", (currentClusterState) -> shutdownClusterState, (e) -> {});
                cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing node shutdown update");
            }

            assertThatLogger(() -> {
                // Force the nonLeader node to become a candidate for master election. This will trigger the election code and the log
                // message saying that the node is ineligible for election during shutdown
                AbstractCoordinatorTestCase.Cluster.becomeCandidate(nonLeader, "forcedForTesting");
                cluster.runFor(DEFAULT_ELECTION_DELAY, "waiting for node to try to run for election");
            },
                Coordinator.class,
                new MockLogAppender.SeenEventExpectation(
                    "log emitted by Coordinator when shut down is in progress",
                    Coordinator.class.getCanonicalName(),
                    Level.TRACE,
                    "skip prevoting as local node may not win election (node is ineligible for election during shutdown)*"
                )
            );
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
            CoordinationState.PersistedState persistedState,
            DisruptibleRegisterConnection disruptibleRegisterConnection
        ) {
            final var statelessElectionStrategy = new StatelessElectionStrategy(() -> new FilterBlobContainer(termLeaseContainer) {
                @Override
                protected BlobContainer wrapChild(BlobContainer child) {
                    throw new AssertionError("should not obtain child");
                }

                @Override
                public void compareAndExchangeRegister(
                    OperationPurpose purpose,
                    String key,
                    BytesReference expected,
                    BytesReference updated,
                    ActionListener<OptionalBytesReference> listener
                ) {
                    disruptibleRegisterConnection.runDisrupted(
                        listener,
                        l -> super.compareAndExchangeRegister(purpose, key, expected, updated, l)
                    );
                }

                @Override
                public void compareAndSetRegister(
                    OperationPurpose purpose,
                    String key,
                    BytesReference expected,
                    BytesReference updated,
                    ActionListener<Boolean> listener
                ) {
                    disruptibleRegisterConnection.runDisrupted(
                        listener,
                        l -> super.compareAndSetRegister(purpose, key, expected, updated, l)
                    );
                }

                @Override
                public void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
                    disruptibleRegisterConnection.runDisrupted(listener, l -> super.getRegister(purpose, key, l));
                }
            }, threadPool) {
                @Override
                protected Executor getExecutor() {
                    return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                }
            };
            final var heartbeatFrequency = HEARTBEAT_FREQUENCY.get(settings);
            final var storeHeartbeatService = new StoreHeartbeatService(
                new DisruptibleHeartbeatStore(heartBeatStore, disruptibleRegisterConnection),
                threadPool,
                heartbeatFrequency,
                TimeValue.timeValueMillis(heartbeatFrequency.millis() * MAX_MISSED_HEARTBEATS.get(settings)),
                statelessElectionStrategy::getCurrentLeaseTerm
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
                    () -> statelessNode.electionStrategy,
                    () -> statelessNode.objectStoreService,
                    threadPool,
                    CompatibilityVersionsUtils.staticCurrent()
                ) {
                    @Override
                    protected Executor getClusterStateUploadsThreadPool() {
                        return EsExecutors.DIRECT_EXECUTOR_SERVICE;
                    }

                    @Override
                    protected Executor getClusterStateDownloadsThreadPool() {
                        return EsExecutors.DIRECT_EXECUTOR_SERVICE;
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

        @Override
        public boolean verifyElectionSchedulerState(Cluster.ClusterNode clusterNode) {
            // If the register is disrupted then today we remain HEALTHY and retry via the election mechanism. We should use a different
            // retry mechanism instead. See https://github.com/elastic/elasticsearch/issues/98488.
            return (clusterNode.isRegisterDisconnected() || clusterNode.isRegisterBlackholed()) == false;
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
