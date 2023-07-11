/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.coordination.ClusterFormationInfoAction;
import org.elasticsearch.action.admin.cluster.coordination.CoordinationDiagnosticsAction;
import org.elasticsearch.action.admin.cluster.coordination.MasterHistoryAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsAction;
import org.elasticsearch.action.admin.cluster.node.hotthreads.TransportNodesHotThreadsAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.ClusterNode;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.History;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.SequentialSpec;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.BatchedRerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MockGatewayMetaState;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.transport.DisruptableMockTransport;
import org.elasticsearch.transport.DisruptableMockTransport.ConnectionStatus;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.DEFAULT_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.BOOTSTRAP_PLACEHOLDER_PREFIX;
import static org.elasticsearch.cluster.coordination.CoordinationStateTestCluster.clusterState;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.FOLLOWER;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.LEADER;
import static org.elasticsearch.cluster.coordination.Coordinator.PUBLISH_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_DURATION_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ID;
import static org.elasticsearch.cluster.coordination.Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.elasticsearch.transport.TransportSettings.CONNECT_TIMEOUT;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

public class AbstractCoordinatorTestCase extends ESTestCase {

    protected final List<NodeEnvironment> nodeEnvironments = new ArrayList<>();
    protected final Set<CoordinationState.PersistedState> openPersistedStates = new HashSet<>();

    protected final AtomicInteger nextNodeIndex = new AtomicInteger();

    @Before
    public void resetNodeIndexBeforeEachTest() {
        nextNodeIndex.set(0);
    }

    @After
    public void closeNodeEnvironmentsAfterEachTest() {
        for (NodeEnvironment nodeEnvironment : nodeEnvironments) {
            nodeEnvironment.close();
        }
        nodeEnvironments.clear();
    }

    @After
    public void assertAllPersistedStatesClosed() {
        assertThat(openPersistedStates, empty());
    }

    @Before
    public void resetPortCounterBeforeEachTest() {
        resetPortCounter();
    }

    // check that runRandomly leads to reproducible results
    public void testRepeatableTests() throws Exception {
        final Callable<Long> test = () -> {
            resetNodeIndexBeforeEachTest();
            try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
                cluster.runRandomly();
                final long afterRunRandomly = value(cluster.getAnyNode().getLastAppliedClusterState());
                cluster.stabilise();
                final long afterStabilisation = value(cluster.getAnyNode().getLastAppliedClusterState());
                return afterRunRandomly ^ afterStabilisation;
            }
        };
        final long seed = randomLong();
        logger.info("First run with seed [{}]", seed);
        final long result1 = RandomizedContext.current().runWithPrivateRandomness(seed, test);
        logger.info("Second run with seed [{}]", seed);
        final long result2 = RandomizedContext.current().runWithPrivateRandomness(seed, test);
        assertEquals(result1, result2);
    }

    protected static long defaultMillis(Setting<TimeValue> setting) {
        return setting.get(Settings.EMPTY).millis() + DEFAULT_DELAY_VARIABILITY;
    }

    protected static int defaultInt(Setting<Integer> setting) {
        return setting.get(Settings.EMPTY);
    }

    // Updating the cluster state involves the following delays:
    // 1. submit the task to the master service
    // 2. state publisher task on master
    // 3. master sends out PublishRequests to nodes
    // 4. master receives PublishResponses from nodes
    // 5. master sends ApplyCommitRequests to nodes
    // 6. nodes apply committed cluster state
    // 7. master receives ApplyCommitResponses
    // 8. apply committed state on master (last one to apply cluster state)
    // 9. complete the publication listener back on the master service thread
    public static final int CLUSTER_STATE_UPDATE_NUMBER_OF_DELAYS = 9;
    public static final long DEFAULT_CLUSTER_STATE_UPDATE_DELAY = CLUSTER_STATE_UPDATE_NUMBER_OF_DELAYS * DEFAULT_DELAY_VARIABILITY;

    private static final int ELECTION_RETRIES = 10;

    // The time it takes to complete an election
    public static final long DEFAULT_ELECTION_DELAY
    // Pinging all peers twice should be enough to discover all nodes
        = defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2
            // Then wait for an election to be scheduled; we allow enough time for retries to allow for collisions
            + defaultMillis(ELECTION_INITIAL_TIMEOUT_SETTING) * ELECTION_RETRIES + defaultMillis(ELECTION_BACK_OFF_TIME_SETTING)
                * ELECTION_RETRIES * (ELECTION_RETRIES - 1) / 2 + defaultMillis(ELECTION_DURATION_SETTING) * ELECTION_RETRIES
            // Allow two round-trip for pre-voting and voting
            + 4 * DEFAULT_DELAY_VARIABILITY
            // Then a commit of the new leader's first cluster state
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY;

    public static final long DEFAULT_STABILISATION_TIME =
        // If leader just blackholed, need to wait for this to be detected
        (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)) * defaultInt(
            LEADER_CHECK_RETRY_COUNT_SETTING
        )
            // then wait for a follower to be promoted to leader
            + DEFAULT_ELECTION_DELAY
            // perhaps there is an election collision requiring another publication (which times out) and a term bump
            + defaultMillis(PUBLISH_TIMEOUT_SETTING) + DEFAULT_ELECTION_DELAY
            // very rarely there is another election collision requiring another publication (which times out) and a term bump
            + defaultMillis(PUBLISH_TIMEOUT_SETTING) + DEFAULT_ELECTION_DELAY
            // then wait for the new leader to notice that the old leader is unresponsive
            + (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)) * defaultInt(
                FOLLOWER_CHECK_RETRY_COUNT_SETTING
            )
            // then wait for the new leader to commit a state without the old leader
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            // then wait for the join validation service to become idle
            + defaultMillis(JoinValidationService.JOIN_VALIDATION_CACHE_TIMEOUT_SETTING);

    public class Cluster implements Releasable {

        static final long EXTREME_DELAY_VARIABILITY = 10000L;
        static final long DEFAULT_DELAY_VARIABILITY = 100L;

        final List<ClusterNode> clusterNodes;
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();
        private boolean disruptStorage;

        final VotingConfiguration initialConfiguration;

        private final Set<String> disconnectedNodes = new HashSet<>();
        private final Set<String> blackholedNodes = new HashSet<>();
        private final Set<Tuple<String, String>> blackholedConnections = new HashSet<>();
        private final Map<Long, ClusterState> committedStatesByVersion = new HashMap<>();
        private final History history = new History();
        private final CountingPageCacheRecycler countingPageCacheRecycler;
        private final Recycler<BytesRef> recycler;
        private final NodeHealthService nodeHealthService;
        private final CoordinatorStrategy coordinatorStrategy;

        @Nullable // null means construct a list from all the current nodes
        private List<TransportAddress> seedHostsList;

        Cluster(int initialNodeCount) {
            this(initialNodeCount, true, Settings.EMPTY);
        }

        public Cluster(int initialNodeCount, boolean allNodesMasterEligible, Settings nodeSettings) {
            this(initialNodeCount, allNodesMasterEligible, nodeSettings, () -> new StatusInfo(HEALTHY, "healthy-info"));
        }

        Cluster(int initialNodeCount, boolean allNodesMasterEligible, Settings nodeSettings, NodeHealthService nodeHealthService) {
            this.nodeHealthService = nodeHealthService;
            this.countingPageCacheRecycler = new CountingPageCacheRecycler();
            this.recycler = new BytesRefRecycler(countingPageCacheRecycler);
            deterministicTaskQueue.setExecutionDelayVariabilityMillis(DEFAULT_DELAY_VARIABILITY);
            this.coordinatorStrategy = createCoordinatorStrategy();

            assertThat(initialNodeCount, greaterThan(0));

            final Set<String> masterEligibleNodeIds = Sets.newHashSetWithExpectedSize(initialNodeCount);
            clusterNodes = new ArrayList<>(initialNodeCount);
            for (int i = 0; i < initialNodeCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(
                    nextNodeIndex.getAndIncrement(),
                    allNodesMasterEligible || i == 0 || randomBoolean(),
                    nodeSettings,
                    nodeHealthService
                );
                clusterNodes.add(clusterNode);
                if (clusterNode.getLocalNode().isMasterNode()) {
                    masterEligibleNodeIds.add(clusterNode.getId());
                }
            }

            initialConfiguration = new VotingConfiguration(
                new HashSet<>(randomSubsetOf(randomIntBetween(1, masterEligibleNodeIds.size()), masterEligibleNodeIds))
            );

            logger.info(
                "--> creating cluster of {} nodes (master-eligible nodes: {}) with initial configuration {}",
                initialNodeCount,
                masterEligibleNodeIds,
                initialConfiguration
            );
        }

        void addNodesAndStabilise(int newNodesCount) {

            // The stabilisation time bound is O(#new nodes) which isn't ideal; it's possible that the real bound is O(1) since node-join
            // events are batched together, but in practice we have not seen problems in this area so have not invested the time needed to
            // investigate this more closely.

            addNodes(newNodesCount);
            stabilise(
                // The first pinging discovers the master
                defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING)
                    // One message delay to send a join
                    + DEFAULT_DELAY_VARIABILITY
                    // Commit a new cluster state with the new node(s). Might be split into multiple commits, and each might need a
                    // followup reconfiguration
                    + newNodesCount * 2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );
        }

        List<ClusterNode> addNodes(int newNodesCount) {
            logger.info("--> adding {} nodes", newNodesCount);

            final List<ClusterNode> addedNodes = new ArrayList<>();
            for (int i = 0; i < newNodesCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(nextNodeIndex.getAndIncrement(), true, Settings.EMPTY, nodeHealthService);
                addedNodes.add(clusterNode);
            }
            clusterNodes.addAll(addedNodes);
            return addedNodes;
        }

        int size() {
            return clusterNodes.size();
        }

        public void runRandomly() {
            runRandomly(true, true, EXTREME_DELAY_VARIABILITY);
        }

        /**
         * @param allowReboots whether to randomly reboot the nodes during the process, losing all transient state. Usually true.
         * @param coolDown whether to set the delay variability back to {@link Cluster#DEFAULT_DELAY_VARIABILITY} and drain all
         *                 disrupted events from the queue before returning. Usually true.
         * @param delayVariability the delay variability to use while running randomly. Usually {@link Cluster#EXTREME_DELAY_VARIABILITY}.
         */
        void runRandomly(boolean allowReboots, boolean coolDown, long delayVariability) {

            assertThat("may reconnect disconnected nodes, probably unexpected", disconnectedNodes, empty());
            assertThat("may reconnect blackholed nodes, probably unexpected", blackholedNodes, empty());

            final List<Runnable> cleanupActions = new ArrayList<>();
            cleanupActions.add(disconnectedNodes::clear);
            cleanupActions.add(blackholedNodes::clear);
            cleanupActions.add(() -> disruptStorage = false);

            final int randomSteps = scaledRandomIntBetween(10, 10000);
            final int keyRange = randomSteps / 50; // for randomized writes and reads
            logger.info("--> start of safety phase of at least [{}] steps with delay variability of [{}ms]", randomSteps, delayVariability);

            deterministicTaskQueue.setExecutionDelayVariabilityMillis(delayVariability);
            disruptStorage = true;
            int step = 0;
            long finishTime = -1;

            while (finishTime == -1 || deterministicTaskQueue.getCurrentTimeMillis() <= finishTime) {
                step++;
                final int thisStep = step; // for lambdas

                if (randomSteps <= step && finishTime == -1) {
                    if (coolDown) {
                        // Heal all nodes BEFORE finishTime is set so it can take into account any pending disruption that
                        // would prevent the cluster to reach a stable state after cooling down. Additionally, avoid any new disruptions
                        // to happen in this phase.
                        // See #61711 for a particular instance where having unhealthy nodes while cooling down can be problematic.
                        disconnectedNodes.clear();
                        blackholedNodes.clear();
                        deterministicTaskQueue.setExecutionDelayVariabilityMillis(DEFAULT_DELAY_VARIABILITY);
                        logger.debug("----> [runRandomly {}] reducing delay variability and running until [{}ms]", step, finishTime);
                    } else {
                        logger.debug(
                            "----> [runRandomly {}] running until [{}ms] with delay variability of [{}ms]",
                            step,
                            finishTime,
                            deterministicTaskQueue.getExecutionDelayVariabilityMillis()
                        );
                    }
                    finishTime = deterministicTaskQueue.getLatestDeferredExecutionTime();
                }

                try {
                    if (finishTime == -1 && randomBoolean() && randomBoolean() && randomBoolean()) {
                        final ClusterNode clusterNode = getAnyNodePreferringLeaders();
                        final int key = randomIntBetween(0, keyRange);
                        final int newValue = randomInt();
                        clusterNode.onNode(() -> {
                            logger.debug(
                                "----> [runRandomly {}] proposing new value [{}={}] to [{}]",
                                thisStep,
                                key,
                                newValue,
                                clusterNode.getId()
                            );
                            clusterNode.submitValue(key, newValue);
                        }).run();
                    } else if (finishTime == -1 && randomBoolean() && randomBoolean() && randomBoolean()) {
                        final ClusterNode clusterNode = getAnyNodePreferringLeaders();
                        final int key = randomIntBetween(0, keyRange);
                        clusterNode.onNode(() -> {
                            logger.debug("----> [runRandomly {}] reading value from [{}]", thisStep, clusterNode.getId());
                            clusterNode.readValue(key);
                        }).run();
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNodePreferringLeaders();
                        final boolean autoShrinkVotingConfiguration = randomBoolean();
                        clusterNode.onNode(() -> {
                            logger.debug(
                                "----> [runRandomly {}] setting auto-shrink configuration to {} on {}",
                                thisStep,
                                autoShrinkVotingConfiguration,
                                clusterNode.getId()
                            );
                            clusterNode.submitSetAutoShrinkVotingConfiguration(autoShrinkVotingConfiguration);
                        }).run();
                    } else if (allowReboots && rarely()) {
                        // reboot random node
                        final ClusterNode clusterNode = getAnyNode();
                        logger.debug("----> [runRandomly {}] rebooting [{}]", thisStep, clusterNode.getId());
                        clusterNode.close();
                        clusterNodes.forEach(cn -> deterministicTaskQueue.scheduleNow(cn.onNode(new Runnable() {
                            @Override
                            public void run() {
                                cn.transportService.disconnectFromNode(clusterNode.getLocalNode());
                            }

                            @Override
                            public String toString() {
                                return "disconnect from " + clusterNode.getLocalNode() + " after shutdown";
                            }
                        })));
                        clusterNodes.replaceAll(cn -> cn == clusterNode ? cn.restartedNode() : cn);
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNode();
                        clusterNode.onNode(() -> {
                            logger.debug("----> [runRandomly {}] forcing {} to become candidate", thisStep, clusterNode.getId());
                            synchronized (clusterNode.coordinator.mutex) {
                                clusterNode.coordinator.becomeCandidate("runRandomly");
                            }
                        }).run();
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNode();

                        switch (randomInt(2)) {
                            case 0:
                                if (clusterNode.heal()) {
                                    logger.debug("----> [runRandomly {}] healing {}", step, clusterNode.getId());
                                }
                                break;
                            case 1:
                                // Avoid disruptions during cool down period
                                if (finishTime == -1 && clusterNode.disconnect()) {
                                    logger.debug("----> [runRandomly {}] disconnecting {}", step, clusterNode.getId());
                                }
                                break;
                            case 2:
                                // Avoid disruptions during cool down period
                                if (finishTime == -1 && clusterNode.blackhole()) {
                                    logger.debug("----> [runRandomly {}] blackholing {}", step, clusterNode.getId());
                                }
                                break;
                        }
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNode();
                        logger.debug("----> [runRandomly {}] applying initial configuration on {}", step, clusterNode.getId());
                        clusterNode.applyInitialConfiguration();
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNode();
                        logger.debug("----> [runRandomly {}] completing blackholed requests sent by {}", step, clusterNode.getId());
                        clusterNode.deliverBlackholedRequests();
                    } else {
                        if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                            deterministicTaskQueue.advanceTime();
                        } else if (deterministicTaskQueue.hasRunnableTasks()) {
                            deterministicTaskQueue.runRandomTask();
                        }
                    }
                } catch (CoordinationStateRejectedException | UncheckedIOException ignored) {
                    // This is ok: it just means a message couldn't currently be handled.
                }

                assertConsistentStates();
            }

            logger.debug("delivering pending blackholed requests");
            clusterNodes.forEach(ClusterNode::deliverBlackholedRequests);
            logger.debug("running {} cleanup actions", cleanupActions.size());
            cleanupActions.forEach(Runnable::run);
            logger.debug("finished running cleanup actions");
        }

        private void assertConsistentStates() {
            for (final ClusterNode clusterNode : clusterNodes) {
                clusterNode.coordinator.invariant();
            }
            updateCommittedStates();
        }

        private void updateCommittedStates() {
            for (final ClusterNode clusterNode : clusterNodes) {
                ClusterState applierState = clusterNode.coordinator.getApplierState();
                ClusterState storedState = committedStatesByVersion.get(applierState.getVersion());
                if (storedState == null) {
                    committedStatesByVersion.put(applierState.getVersion(), applierState);
                } else {
                    if (value(applierState) != value(storedState)) {
                        fail("expected " + applierState + " but got " + storedState);
                    }
                }
            }
        }

        public void stabilise() {
            stabilise(DEFAULT_STABILISATION_TIME, true);
        }

        public void stabilise(long stabilisationDurationMillis) {
            stabilise(stabilisationDurationMillis, false);
        }

        private void stabilise(long stabilisationDurationMillis, boolean expectIdleJoinValidationService) {
            assertThat(
                "stabilisation requires default delay variability (and proper cleanup of raised variability)",
                deterministicTaskQueue.getExecutionDelayVariabilityMillis(),
                lessThanOrEqualTo(DEFAULT_DELAY_VARIABILITY)
            );
            assertFalse("stabilisation requires stable storage", disruptStorage);

            bootstrapIfNecessary();

            runFor(stabilisationDurationMillis, "stabilising");

            // There's no timeout on the register check when committing a publication, so if there's a blackholed former-master then we must
            // complete that check exceptionally. Relates https://github.com/elastic/elasticsearch/issues/80400.
            // noinspection ReplaceInefficientStreamCount using .count() to run the filter on every node
            while (clusterNodes.stream().filter(ClusterNode::deliverBlackholedRegisterRequests).count() != 0L) {
                logger.debug("--> delivering blackholed register requests");
                // one delay to schedule the request failure, but this forks back to CLUSTER_COORDINATION so a second delay is needed
                runFor(DEFAULT_DELAY_VARIABILITY * 2, "re-stabilising");
            }

            final ClusterNode leader = getAnyLeader();
            final long leaderTerm = leader.coordinator.getCurrentTerm();

            final int pendingTaskCount = leader.getPendingTaskCount();
            runFor((pendingTaskCount + 1) * DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "draining task queue");

            final Matcher<Long> isEqualToLeaderVersion = equalTo(leader.coordinator.getLastAcceptedState().getVersion());
            final String leaderId = leader.getId();

            assertTrue(leaderId + " has been bootstrapped", leader.coordinator.isInitialConfigurationSet());
            assertTrue(leaderId + " exists in its last-applied state", leader.getLastAppliedClusterState().getNodes().nodeExists(leaderId));
            assertThat(
                leaderId + " has no NO_MASTER_BLOCK",
                leader.getLastAppliedClusterState().blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID),
                equalTo(false)
            );
            assertThat(
                leaderId + " has no STATE_NOT_RECOVERED_BLOCK",
                leader.getLastAppliedClusterState().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK),
                equalTo(false)
            );
            assertThat(leaderId + " has applied its state ", leader.getLastAppliedClusterState().getVersion(), isEqualToLeaderVersion);

            final var clusterUuid = leader.getLastAppliedClusterState().metadata().clusterUUID();

            for (final ClusterNode clusterNode : clusterNodes) {
                final String nodeId = clusterNode.getId();
                assertFalse(nodeId + " should not have an active publication", clusterNode.coordinator.publicationInProgress());

                if (clusterNode == leader) {
                    assertThat(nodeId + " is still the leader", clusterNode.coordinator.getMode(), is(LEADER));
                    assertThat(nodeId + " did not change term", clusterNode.coordinator.getCurrentTerm(), is(leaderTerm));
                    continue;
                }

                if (isConnectedPair(leader, clusterNode)) {
                    assertThat(nodeId + " is a follower of " + leaderId, clusterNode.coordinator.getMode(), is(FOLLOWER));
                    assertThat(nodeId + " has the same term as " + leaderId, clusterNode.coordinator.getCurrentTerm(), is(leaderTerm));
                    assertFalse(
                        nodeId + " is not a missing vote for  " + leaderId,
                        leader.coordinator.missingJoinVoteFrom(clusterNode.getLocalNode())
                    );
                    assertThat(
                        nodeId + " has the same accepted state as " + leaderId,
                        clusterNode.coordinator.getLastAcceptedState().getVersion(),
                        isEqualToLeaderVersion
                    );
                    if (clusterNode.getClusterStateApplyResponse() == ClusterStateApplyResponse.SUCCEED) {
                        assertThat(
                            nodeId + " has the same applied state as " + leaderId,
                            clusterNode.getLastAppliedClusterState().getVersion(),
                            isEqualToLeaderVersion
                        );
                        assertTrue(
                            nodeId + " is in its own latest applied state",
                            clusterNode.getLastAppliedClusterState().getNodes().nodeExists(nodeId)
                        );
                    }
                    assertTrue(
                        nodeId + " is in the latest applied state on " + leaderId,
                        leader.getLastAppliedClusterState().getNodes().nodeExists(nodeId)
                    );
                    assertTrue(nodeId + " has been bootstrapped", clusterNode.coordinator.isInitialConfigurationSet());
                    assertThat(
                        nodeId + " has correct master",
                        clusterNode.getLastAppliedClusterState().nodes().getMasterNode(),
                        equalTo(leader.getLocalNode())
                    );
                    assertThat(
                        nodeId + " has no NO_MASTER_BLOCK",
                        clusterNode.getLastAppliedClusterState().blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID),
                        equalTo(false)
                    );
                    assertThat(
                        nodeId + " has no STATE_NOT_RECOVERED_BLOCK",
                        clusterNode.getLastAppliedClusterState().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK),
                        equalTo(false)
                    );
                    assertTrue(
                        nodeId + " has locked into the cluster",
                        clusterNode.getLastAppliedClusterState().metadata().clusterUUIDCommitted()
                    );
                    assertThat(
                        nodeId + " has the correct cluster UUID",
                        clusterNode.getLastAppliedClusterState().metadata().clusterUUID(),
                        equalTo(clusterUuid)
                    );

                    for (final ClusterNode otherNode : clusterNodes) {
                        if (isConnectedPair(leader, otherNode) && isConnectedPair(otherNode, clusterNode)) {
                            assertTrue(
                                otherNode.getId() + " is connected to healthy node " + clusterNode.getId(),
                                otherNode.transportService.nodeConnected(clusterNode.localNode)
                            );
                        }
                    }
                } else {
                    assertThat(nodeId + " is not following " + leaderId, clusterNode.coordinator.getMode(), is(CANDIDATE));
                    assertThat(nodeId + " has no master", clusterNode.getLastAppliedClusterState().nodes().getMasterNode(), nullValue());
                    assertThat(
                        nodeId + " has NO_MASTER_BLOCK",
                        clusterNode.getLastAppliedClusterState().blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID),
                        equalTo(true)
                    );
                    assertFalse(
                        nodeId + " is not in the applied state on " + leaderId,
                        leader.getLastAppliedClusterState().getNodes().nodeExists(nodeId)
                    );

                    for (final ClusterNode otherNode : clusterNodes) {
                        if (isConnectedPair(leader, otherNode)) {
                            assertFalse(
                                otherNode.getId() + " is not connected to removed node " + clusterNode.getId(),
                                otherNode.transportService.nodeConnected(clusterNode.localNode)
                            );
                        }
                    }
                }

                if (expectIdleJoinValidationService) {
                    // Tests run stabilise(long stabilisationDurationMillis) to assert timely recovery from a disruption. There's no need
                    // to wait for the JoinValidationService cache to be cleared in these cases, we have enough checks that this eventually
                    // happens anyway.
                    assertTrue(nodeId + " has an idle join validation service", clusterNode.coordinator.hasIdleJoinValidationService());
                }
            }

            final Set<String> connectedNodeIds = clusterNodes.stream()
                .filter(n -> isConnectedPair(leader, n))
                .map(ClusterNode::getId)
                .collect(Collectors.toSet());

            assertThat(leader.getLastAppliedClusterState().getNodes().getSize(), equalTo(connectedNodeIds.size()));

            final ClusterState lastAcceptedState = leader.coordinator.getLastAcceptedState();
            final VotingConfiguration lastCommittedConfiguration = lastAcceptedState.getLastCommittedConfiguration();
            assertTrue(
                connectedNodeIds + " should be a quorum of " + lastCommittedConfiguration,
                lastCommittedConfiguration.hasQuorum(connectedNodeIds)
            );
            assertThat(
                "leader " + leader.getLocalNode() + " should be part of voting configuration " + lastCommittedConfiguration,
                lastCommittedConfiguration.getNodeIds(),
                Matchers.hasItem(leader.getLocalNode().getId())
            );

            assertThat(
                "no reconfiguration is in progress",
                lastAcceptedState.getLastCommittedConfiguration(),
                equalTo(lastAcceptedState.getLastAcceptedConfiguration())
            );
            assertThat(
                "current configuration is already optimal",
                leader.improveConfiguration(lastAcceptedState),
                sameInstance(lastAcceptedState)
            );

            logger.info("checking linearizability of history with size {}: {}", history.size(), history);
            final AtomicBoolean abort = new AtomicBoolean();
            // Large histories can be problematic and have the linearizability checker run OOM
            // Bound the time how long the checker can run on such histories (Values empirically determined)
            final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY, "test-scheduler");
            try {
                if (history.size() > 300) {
                    scheduler.schedule(() -> abort.set(true), 10, TimeUnit.SECONDS);
                }
                final boolean linearizable = LinearizabilityChecker.isLinearizable(spec, history, i -> null, abort::get);
                if (abort.get() == false) {
                    assertTrue("history not linearizable: " + history, linearizable);
                }
            } finally {
                ThreadPool.terminate(scheduler, 1, TimeUnit.SECONDS);
            }
            logger.info("linearizability check completed");
        }

        void bootstrapIfNecessary() {
            if (clusterNodes.stream().allMatch(ClusterNode::isNotUsefullyBootstrapped)) {
                assertThat("setting initial configuration may fail with disconnected nodes", disconnectedNodes, empty());
                assertThat("setting initial configuration may fail with blackholed nodes", blackholedNodes, empty());
                runFor(defaultMillis(CONNECT_TIMEOUT) + // may be in a prior connection attempt which has been blackholed
                    defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2, "discovery prior to setting initial configuration");
                final ClusterNode bootstrapNode = getAnyBootstrappableNode();
                bootstrapNode.applyInitialConfiguration();
            } else {
                logger.info("setting initial configuration not required");
            }
        }

        void runFor(long runDurationMillis, String description) {
            final long endTime = deterministicTaskQueue.getCurrentTimeMillis() + runDurationMillis;
            logger.info("--> runFor({}ms) running until [{}ms]: {}", runDurationMillis, endTime, description);

            while (deterministicTaskQueue.getCurrentTimeMillis() < endTime) {

                while (deterministicTaskQueue.hasRunnableTasks()) {
                    try {
                        deterministicTaskQueue.runRandomTask();
                    } catch (CoordinationStateRejectedException e) {
                        logger.debug("ignoring benign exception thrown when stabilising", e);
                    }
                    for (final ClusterNode clusterNode : clusterNodes) {
                        clusterNode.coordinator.invariant();
                    }
                    updateCommittedStates();
                }

                if (deterministicTaskQueue.hasDeferredTasks() == false) {
                    // A 1-node cluster has no need for fault detection etc so will eventually run out of things to do.
                    assert clusterNodes.size() == 1 : clusterNodes.size();
                    break;
                }

                deterministicTaskQueue.advanceTime();
            }

            logger.info("--> runFor({}ms) completed run until [{}ms]: {}", runDurationMillis, endTime, description);
        }

        private boolean isConnectedPair(ClusterNode n1, ClusterNode n2) {
            return n1 == n2
                || (getConnectionStatus(n1.getLocalNode(), n2.getLocalNode()) == ConnectionStatus.CONNECTED
                    && getConnectionStatus(n2.getLocalNode(), n1.getLocalNode()) == ConnectionStatus.CONNECTED)
                    && (n1.nodeHealthService.getHealth().getStatus() == HEALTHY && n2.nodeHealthService.getHealth().getStatus() == HEALTHY);
        }

        public ClusterNode getAnyLeader() {
            List<ClusterNode> allLeaders = clusterNodes.stream().filter(ClusterNode::isLeader).collect(Collectors.toList());
            assertThat("leaders", allLeaders, not(empty()));
            return randomFrom(allLeaders);
        }

        private final ConnectionStatus preferredUnknownNodeConnectionStatus = randomFrom(
            ConnectionStatus.DISCONNECTED,
            ConnectionStatus.BLACK_HOLE
        );

        private ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination) {
            ConnectionStatus connectionStatus;
            if (blackholedNodes.contains(sender.getId()) || blackholedNodes.contains(destination.getId())) {
                connectionStatus = ConnectionStatus.BLACK_HOLE;
            } else if (disconnectedNodes.contains(sender.getId()) || disconnectedNodes.contains(destination.getId())) {
                connectionStatus = ConnectionStatus.DISCONNECTED;
            } else if (blackholedConnections.contains(Tuple.tuple(sender.getId(), destination.getId()))) {
                connectionStatus = ConnectionStatus.BLACK_HOLE_REQUESTS_ONLY;
            } else if (nodeExists(sender) && nodeExists(destination)) {
                connectionStatus = ConnectionStatus.CONNECTED;
            } else {
                connectionStatus = usually()
                    ? preferredUnknownNodeConnectionStatus
                    : randomFrom(ConnectionStatus.DISCONNECTED, ConnectionStatus.BLACK_HOLE);
            }
            return connectionStatus;
        }

        boolean nodeExists(DiscoveryNode node) {
            return clusterNodes.stream().anyMatch(cn -> cn.getLocalNode().equals(node));
        }

        ClusterNode getAnyBootstrappableNode() {
            return randomFrom(
                clusterNodes.stream()
                    .filter(n -> n.getLocalNode().isMasterNode())
                    .filter(n -> initialConfiguration.getNodeIds().contains(n.getLocalNode().getId()))
                    .collect(Collectors.toList())
            );
        }

        ClusterNode getAnyNode() {
            return getAnyNodeExcept();
        }

        ClusterNode getAnyNodeExcept(ClusterNode... clusterNodesToExclude) {
            List<ClusterNode> filteredNodes = getAllNodesExcept(clusterNodesToExclude);
            assert filteredNodes.isEmpty() == false;
            return randomFrom(filteredNodes);
        }

        List<ClusterNode> getAllNodesExcept(ClusterNode... clusterNodesToExclude) {
            Set<String> forbiddenIds = Arrays.stream(clusterNodesToExclude).map(ClusterNode::getId).collect(Collectors.toSet());
            return this.clusterNodes.stream().filter(n -> forbiddenIds.contains(n.getId()) == false).collect(Collectors.toList());
        }

        ClusterNode getAnyNodePreferringLeaders() {
            for (int i = 0; i < 3; i++) {
                ClusterNode clusterNode = getAnyNode();
                if (clusterNode.coordinator.getMode() == LEADER) {
                    return clusterNode;
                }
            }
            return getAnyNode();
        }

        void setEmptySeedHostsList() {
            seedHostsList = emptyList();
        }

        void blackholeConnectionsFrom(ClusterNode sender, ClusterNode destination) {
            blackholedConnections.add(Tuple.tuple(sender.getId(), destination.getId()));
        }

        void clearBlackholedConnections() {
            blackholedConnections.clear();
        }

        CoordinatorStrategy getCoordinatorStrategy() {
            return coordinatorStrategy;
        }

        @Override
        public void close() {
            // noinspection ReplaceInefficientStreamCount using .count() to run the filter on every node
            while (clusterNodes.stream().filter(ClusterNode::deliverBlackholedRequests).count() != 0L) {
                logger.debug("--> stabilising again after delivering blackholed requests");
                stabilise(DEFAULT_STABILISATION_TIME);
            }

            clusterNodes.forEach(ClusterNode::close);

            // Closing nodes may spawn some other background cleanup tasks that must also be run
            runFor(DEFAULT_DELAY_VARIABILITY, "accumulate close-time tasks");
            deterministicTaskQueue.runAllRunnableTasks();

            countingPageCacheRecycler.assertAllPagesReleased();

            coordinatorStrategy.close();
        }

        protected List<NamedWriteableRegistry.Entry> extraNamedWriteables() {
            return emptyList();
        }

        private NamedWriteableRegistry getNamedWriteableRegistry() {
            return new NamedWriteableRegistry(
                Stream.concat(ClusterModule.getNamedWriteables().stream(), extraNamedWriteables().stream()).collect(Collectors.toList())
            );
        }

        CoordinationState.PersistedState createFreshPersistedState(DiscoveryNode localNode, ThreadPool threadPool) {
            return coordinatorStrategy.createFreshPersistedState(localNode, () -> disruptStorage, threadPool);
        }

        CoordinationState.PersistedState createPersistedStateFromExistingState(
            DiscoveryNode newLocalNode,
            CoordinationState.PersistedState oldState,
            Function<Metadata, Metadata> adaptGlobalMetadata,
            Function<Long, Long> adaptCurrentTerm,
            ThreadPool threadPool
        ) {
            return coordinatorStrategy.createPersistedStateFromExistingState(
                newLocalNode,
                oldState,
                adaptGlobalMetadata,
                adaptCurrentTerm,
                deterministicTaskQueue::getCurrentTimeMillis,
                getNamedWriteableRegistry(),
                () -> disruptStorage,
                threadPool
            );
        }

        public class ClusterNode {
            private final Logger logger = LogManager.getLogger(ClusterNode.class);

            private final int nodeIndex;
            Coordinator coordinator;
            private final DiscoveryNode localNode;
            final CoordinationState.PersistedState persistedState;
            final Settings nodeSettings;
            private AckedFakeThreadPoolMasterService masterService;
            private DisruptableClusterApplierService clusterApplierService;
            private ClusterService clusterService;
            TransportService transportService;
            private MasterHistoryService masterHistoryService;
            CoordinationDiagnosticsService coordinationDiagnosticsService;
            StableMasterHealthIndicatorService stableMasterHealthIndicatorService;
            private DisruptableMockTransport mockTransport;
            private final NodeHealthService nodeHealthService;
            List<BiConsumer<DiscoveryNode, ClusterState>> extraJoinValidators = new ArrayList<>();
            private ClearableRecycler clearableRecycler;
            private List<Runnable> blackholedRegisterOperations = new ArrayList<>();

            ClusterNode(int nodeIndex, boolean masterEligible, Settings nodeSettings, NodeHealthService nodeHealthService) {
                this(
                    nodeIndex,
                    createDiscoveryNode(nodeIndex, masterEligible),
                    Cluster.this::createFreshPersistedState,
                    nodeSettings,
                    nodeHealthService
                );
            }

            ClusterNode(
                int nodeIndex,
                DiscoveryNode localNode,
                BiFunction<DiscoveryNode, ThreadPool, CoordinationState.PersistedState> persistedStateSupplier,
                Settings nodeSettings,
                NodeHealthService nodeHealthService
            ) {
                this.nodeHealthService = nodeHealthService;
                this.nodeIndex = nodeIndex;
                this.localNode = localNode;
                this.nodeSettings = nodeSettings;
                final ThreadPool threadPool = deterministicTaskQueue.getThreadPool(this::onNode);
                persistedState = persistedStateSupplier.apply(localNode, threadPool);
                assertTrue("must use a fresh PersistedState", openPersistedStates.add(persistedState));
                boolean success = false;
                try {
                    DeterministicTaskQueue.onNodeLog(localNode, () -> setUp(threadPool)).run();
                    success = true;
                } finally {
                    if (success == false) {
                        IOUtils.closeWhileHandlingException(persistedState); // removes it from openPersistedStates
                    }
                }
            }

            private void setUp(ThreadPool threadPool) {
                clearableRecycler = new ClearableRecycler(recycler);
                mockTransport = new DisruptableMockTransport(localNode, deterministicTaskQueue) {
                    @Override
                    protected void execute(Runnable runnable) {
                        deterministicTaskQueue.scheduleNow(onNode(runnable));
                    }

                    @Override
                    protected ConnectionStatus getConnectionStatus(DiscoveryNode destination) {
                        return Cluster.this.getConnectionStatus(getLocalNode(), destination);
                    }

                    @Override
                    protected Optional<DisruptableMockTransport> getDisruptableMockTransport(TransportAddress address) {
                        return clusterNodes.stream()
                            .map(cn -> cn.mockTransport)
                            .filter(transport -> transport.getLocalNode().getAddress().equals(address))
                            .findAny();
                    }

                    @Override
                    protected void onSendRequest(
                        long requestId,
                        String action,
                        TransportRequest request,
                        TransportRequestOptions options,
                        DisruptableMockTransport destinationTransport
                    ) {
                        final TransportRequestOptions.Type chanType = options.type();
                        switch (action) {
                            case JoinHelper.JOIN_ACTION_NAME, FollowersChecker.FOLLOWER_CHECK_ACTION_NAME,
                                LeaderChecker.LEADER_CHECK_ACTION_NAME -> assertThat(
                                    action,
                                    chanType,
                                    equalTo(TransportRequestOptions.Type.PING)
                                );
                            case JoinValidationService.JOIN_VALIDATE_ACTION_NAME, PublicationTransportHandler.PUBLISH_STATE_ACTION_NAME,
                                Coordinator.COMMIT_STATE_ACTION_NAME -> assertThat(
                                    action,
                                    chanType,
                                    equalTo(TransportRequestOptions.Type.STATE)
                                );
                            case JoinHelper.JOIN_PING_ACTION_NAME -> assertThat(
                                action,
                                chanType,
                                oneOf(TransportRequestOptions.Type.STATE, TransportRequestOptions.Type.PING)
                            );
                            default -> assertThat(action, chanType, equalTo(TransportRequestOptions.Type.REG));
                        }

                        super.onSendRequest(requestId, action, request, options, destinationTransport);
                    }

                    @Override
                    public RecyclerBytesStreamOutput newNetworkBytesStream() {
                        return new RecyclerBytesStreamOutput(clearableRecycler);
                    }
                };
                final Settings settings = nodeSettings.hasValue(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey())
                    ? nodeSettings
                    : Settings.builder()
                        .put(nodeSettings)
                        .putList(
                            ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(),
                            ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY)
                        )
                        .build(); // suppress auto-bootstrap
                transportService = mockTransport.createTransportService(
                    settings,
                    threadPool,
                    getTransportInterceptor(localNode, threadPool),
                    a -> localNode,
                    null,
                    emptySet()
                );
                masterService = new AckedFakeThreadPoolMasterService(
                    localNode.getId(),
                    "test",
                    threadPool,
                    runnable -> deterministicTaskQueue.scheduleNow(onNode(runnable))
                );
                final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                clusterApplierService = new DisruptableClusterApplierService(
                    localNode.getId(),
                    localNode.getEphemeralId(),
                    settings,
                    clusterSettings,
                    deterministicTaskQueue,
                    threadPool
                );
                clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);
                masterHistoryService = new MasterHistoryService(transportService, threadPool, clusterService);
                clusterService.setNodeConnectionsService(
                    new NodeConnectionsService(clusterService.getSettings(), threadPool, transportService)
                );
                final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators = Collections.singletonList(
                    (dn, cs) -> extraJoinValidators.forEach(validator -> validator.accept(dn, cs))
                );
                final AllocationService allocationService = ESAllocationTestCase.createAllocationService(Settings.EMPTY);
                final NodeClient client = new NodeClient(Settings.EMPTY, threadPool);
                final var coordinationServices = coordinatorStrategy.getCoordinationServices(
                    threadPool,
                    settings,
                    clusterSettings,
                    persistedState,
                    new NodeDisruptibleRegisterConnection()
                );
                coordinator = new Coordinator(
                    "test_node",
                    settings,
                    clusterSettings,
                    transportService,
                    client,
                    getNamedWriteableRegistry(),
                    allocationService,
                    masterService,
                    this::getPersistedState,
                    Cluster.this::provideSeedHosts,
                    clusterApplierService,
                    onJoinValidators,
                    Randomness.get(),
                    (s, p, r) -> {},
                    coordinationServices.getElectionStrategy(),
                    nodeHealthService,
                    new NoneCircuitBreakerService(),
                    coordinationServices.getReconfigurator(),
                    coordinationServices.getLeaderHeartbeatService(),
                    coordinationServices.getPreVoteCollectorFactory()
                );
                coordinationDiagnosticsService = new CoordinationDiagnosticsService(
                    clusterService,
                    transportService,
                    coordinator,
                    masterHistoryService
                );
                client.initialize(
                    Map.of(
                        NodesHotThreadsAction.INSTANCE,
                        new TransportNodesHotThreadsAction(threadPool, clusterService, transportService, new ActionFilters(emptySet())),
                        MasterHistoryAction.INSTANCE,
                        new MasterHistoryAction.TransportAction(transportService, new ActionFilters(Set.of()), masterHistoryService),
                        ClusterFormationInfoAction.INSTANCE,
                        new ClusterFormationInfoAction.TransportAction(transportService, new ActionFilters(Set.of()), coordinator),
                        CoordinationDiagnosticsAction.INSTANCE,
                        new CoordinationDiagnosticsAction.TransportAction(
                            transportService,
                            new ActionFilters(Set.of()),
                            coordinationDiagnosticsService
                        )
                    ),
                    transportService.getTaskManager(),
                    localNode::getId,
                    transportService.getLocalNodeConnection(),
                    null,
                    getNamedWriteableRegistry()
                );
                stableMasterHealthIndicatorService = new StableMasterHealthIndicatorService(coordinationDiagnosticsService, clusterService);
                masterService.setClusterStatePublisher(coordinator);
                final GatewayService gatewayService = new GatewayService(
                    settings,
                    new BatchedRerouteService(clusterService, allocationService::reroute),
                    clusterService,
                    TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY,
                    threadPool
                );

                logger.trace("starting up [{}]", localNode);
                transportService.start();
                transportService.acceptIncomingRequests();
                coordinator.start();
                gatewayService.start();
                clusterService.start();
                coordinationDiagnosticsService.start();
                coordinator.startInitialJoin();
            }

            void close() {
                assertThat("must add nodes to a cluster before closing them", clusterNodes, hasItem(ClusterNode.this));
                onNode(() -> {
                    logger.trace("closing");
                    coordinator.stop();
                    clusterService.stop();
                    // transportService.stop(); // does blocking stuff :/
                    clusterService.close();
                    coordinator.close();
                    // transportService.close(); // does blocking stuff :/
                }).run();
            }

            ClusterNode restartedNode() {
                return restartedNode(Function.identity(), Function.identity(), nodeSettings);
            }

            ClusterNode restartedNode(
                Function<Metadata, Metadata> adaptGlobalMetadata,
                Function<Long, Long> adaptCurrentTerm,
                Settings settings
            ) {
                final DiscoveryNode newLocalNode = DiscoveryNodeUtils.builder(localNode.getId())
                    .name(localNode.getName())
                    .ephemeralId(UUIDs.randomBase64UUID(random()))  // generated deterministically for repeatable tests
                    .address(randomBoolean() ? buildNewFakeTransportAddress() : localNode.getAddress())
                    .roles(localNode.isMasterNode() && DiscoveryNode.isMasterNode(settings) ? ALL_ROLES_EXCEPT_VOTING_ONLY : emptySet())
                    .build();
                try {
                    return new ClusterNode(
                        nodeIndex,
                        newLocalNode,
                        (node, threadPool) -> createPersistedStateFromExistingState(
                            newLocalNode,
                            persistedState,
                            adaptGlobalMetadata,
                            adaptCurrentTerm,
                            threadPool
                        ),
                        settings,
                        nodeHealthService
                    );
                } finally {
                    clearableRecycler.clear();
                }
            }

            private CoordinationState.PersistedState getPersistedState() {
                return persistedState;
            }

            String getId() {
                return localNode.getId();
            }

            public DiscoveryNode getLocalNode() {
                return localNode;
            }

            boolean isLeader() {
                return coordinator.getMode() == LEADER;
            }

            boolean isCandidate() {
                return coordinator.getMode() == CANDIDATE;
            }

            ClusterState improveConfiguration(ClusterState currentState) {
                synchronized (coordinator.mutex) {
                    return coordinator.improveConfiguration(currentState);
                }
            }

            void setClusterStateApplyResponse(ClusterStateApplyResponse clusterStateApplyResponse) {
                clusterApplierService.clusterStateApplyResponse = clusterStateApplyResponse;
            }

            ClusterStateApplyResponse getClusterStateApplyResponse() {
                return clusterApplierService.clusterStateApplyResponse;
            }

            Runnable onNode(Runnable runnable) {
                final Runnable wrapped = DeterministicTaskQueue.onNodeLog(localNode, runnable);
                return new Runnable() {
                    @Override
                    public void run() {
                        if (clusterNodes.contains(ClusterNode.this)) {
                            wrapped.run();
                        } else if (runnable instanceof DisruptableMockTransport.RebootSensitiveRunnable rebootSensitiveRunnable) {
                            logger.trace(
                                "completing reboot-sensitive runnable {} from node {} as node has been removed from cluster",
                                runnable,
                                localNode
                            );
                            rebootSensitiveRunnable.ifRebooted();
                        } else {
                            logger.trace("ignoring runnable {} from node {} as node has been removed from cluster", runnable, localNode);
                        }
                    }

                    @Override
                    public String toString() {
                        return wrapped.toString();
                    }
                };
            }

            void submitSetAutoShrinkVotingConfiguration(final boolean autoShrinkVotingConfiguration) {
                submitUpdateTask(
                    "set master nodes failure tolerance [" + autoShrinkVotingConfiguration + "]",
                    cs -> ClusterState.builder(cs)
                        .metadata(
                            Metadata.builder(cs.metadata())
                                .persistentSettings(
                                    Settings.builder()
                                        .put(cs.metadata().persistentSettings())
                                        .put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), autoShrinkVotingConfiguration)
                                        .build()
                                )
                                .build()
                        )
                        .build(),
                    (e) -> {}
                );
            }

            AckCollector submitValue(final long value) {
                return submitValue(0, value);
            }

            AckCollector submitValue(final int key, final long value) {
                final int eventId = history.invoke(new Tuple<>(key, value));
                return submitUpdateTask(
                    "new value [" + key + "=" + value + "]",
                    cs -> setValue(cs, key, value),
                    new CoordinatorTestClusterStateUpdateTask() {
                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            history.respond(eventId, value(oldState, key));
                        }

                        @Override
                        public void onFailure(Exception e) {
                            if (e instanceof FailedToCommitClusterStateException == false) {
                                // In this case, we know for sure that event was not processed by the system and will not change history.
                                // Therefore remove event to help avoid bloated history and state space explosion in linearizability
                                // checker.
                                history.remove(eventId);
                            }

                            // Else do not remove event from history, the write might still take effect. Instead, complete history when
                            // checking
                            // for linearizability.
                        }
                    }
                );
            }

            void readValue(int key) {
                final int eventId = history.invoke(new Tuple<>(key, null));
                submitUpdateTask("read value", cs -> ClusterState.builder(cs).build(), new CoordinatorTestClusterStateUpdateTask() {
                    @Override
                    public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                        history.respond(eventId, value(newState, key));
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // reads do not change state
                        // remove event to help avoid bloated history and state space explosion in linearizability checker
                        history.remove(eventId);
                    }
                });
            }

            AckCollector submitUpdateTask(
                String source,
                UnaryOperator<ClusterState> clusterStateUpdate,
                CoordinatorTestClusterStateUpdateTask taskListener
            ) {
                final AckCollector ackCollector = new AckCollector();
                onNode(() -> {
                    logger.trace("[{}] submitUpdateTask: enqueueing [{}]", localNode.getId(), source);
                    final long submittedTerm = coordinator.getCurrentTerm();
                    masterService.submitUnbatchedStateUpdateTask(source, new ClusterStateUpdateTask() {

                        @Override
                        public ClusterState execute(ClusterState currentState) {
                            assertThat(currentState.term(), greaterThanOrEqualTo(submittedTerm));
                            masterService.nextAckCollector = ackCollector;
                            return clusterStateUpdate.apply(currentState);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            logger.debug("publication failed", e);
                            taskListener.onFailure(e);
                        }

                        @Override
                        public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                            updateCommittedStates();
                            ClusterState state = committedStatesByVersion.get(newState.version());
                            assertNotNull("State not committed : " + newState, state);
                            final var notRecovered = state.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK);
                            assertEquals(notRecovered, newState.blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK));
                            if (notRecovered == false) {
                                assertStateEquals(state, newState);
                            }
                            logger.trace("successfully published: [{}]", newState);
                            taskListener.clusterStateProcessed(oldState, newState);
                        }
                    });
                }).run();
                return ackCollector;
            }

            @Override
            public String toString() {
                return localNode.toString();
            }

            boolean heal() {
                boolean unBlackholed = blackholedNodes.remove(localNode.getId());
                boolean unDisconnected = disconnectedNodes.remove(localNode.getId());
                assert unBlackholed == false || unDisconnected == false;
                return unBlackholed || unDisconnected;
            }

            boolean disconnect() {
                boolean unBlackholed = blackholedNodes.remove(localNode.getId());
                boolean disconnected = disconnectedNodes.add(localNode.getId());
                assert disconnected || unBlackholed == false;
                return disconnected;
            }

            boolean blackhole() {
                boolean unDisconnected = disconnectedNodes.remove(localNode.getId());
                boolean blackholed = blackholedNodes.add(localNode.getId());
                assert blackholed || unDisconnected == false;
                return blackholed;
            }

            void onDisconnectEventFrom(ClusterNode clusterNode) {
                transportService.disconnectFromNode(clusterNode.localNode);
            }

            ClusterState getLastAppliedClusterState() {
                return clusterApplierService.state();
            }

            void addActionBlock(@SuppressWarnings("SameParameterValue") String actionName) {
                mockTransport.addActionBlock(actionName);
            }

            void clearActionBlocks() {
                mockTransport.clearActionBlocks();
            }

            void applyInitialConfiguration() {
                onNode(() -> {
                    final Set<String> nodeIdsWithPlaceholders = new HashSet<>(initialConfiguration.getNodeIds());
                    Stream.generate(() -> BOOTSTRAP_PLACEHOLDER_PREFIX + UUIDs.randomBase64UUID(random()))
                        .limit((Math.max(initialConfiguration.getNodeIds().size(), 2) - 1) / 2)
                        .forEach(nodeIdsWithPlaceholders::add);
                    final Set<String> nodeIds = new HashSet<>(
                        randomSubsetOf(initialConfiguration.getNodeIds().size(), nodeIdsWithPlaceholders)
                    );
                    // initial configuration should not have a place holder for local node
                    if (initialConfiguration.getNodeIds().contains(localNode.getId()) && nodeIds.contains(localNode.getId()) == false) {
                        nodeIds.remove(nodeIds.iterator().next());
                        nodeIds.add(localNode.getId());
                    }
                    final VotingConfiguration configurationWithPlaceholders = new VotingConfiguration(nodeIds);
                    try {
                        coordinator.setInitialConfiguration(configurationWithPlaceholders);
                        logger.info("successfully set initial configuration to {}", configurationWithPlaceholders);
                    } catch (CoordinationStateRejectedException e) {
                        logger.info(() -> format("failed to set initial configuration to %s", configurationWithPlaceholders), e);
                    }
                }).run();
            }

            private boolean isNotUsefullyBootstrapped() {
                return getLocalNode().isMasterNode() == false || coordinator.isInitialConfigurationSet() == false;
            }

            void allowClusterStateApplicationFailure() {
                clusterApplierService.allowClusterStateApplicationFailure();
                masterService.allowPublicationFailure();
            }

            boolean deliverBlackholedRequests() {
                final boolean deliveredBlackholedRequests = mockTransport.deliverBlackholedRequests();
                final boolean deliveredBlackholedRegisterRequests = deliverBlackholedRegisterRequests();
                return deliveredBlackholedRequests || deliveredBlackholedRegisterRequests;
            }

            boolean deliverBlackholedRegisterRequests() {
                if (blackholedRegisterOperations.isEmpty()) {
                    return false;
                } else {
                    blackholedRegisterOperations.forEach(deterministicTaskQueue::scheduleNow);
                    blackholedRegisterOperations.clear();
                    return true;
                }
            }

            int getPendingTaskCount() {
                return masterService.numberOfPendingTasks();
            }

            private class NodeDisruptibleRegisterConnection implements DisruptibleRegisterConnection {
                @Override
                public <R> void runDisrupted(ActionListener<R> listener, Consumer<ActionListener<R>> consumer) {
                    if (isDisconnected()) {
                        listener.onFailure(new IOException("simulated disrupted connection to register"));
                    } else if (isBlackholed()) {
                        final var exception = new IOException("simulated eventual failure to blackholed register");
                        logger.trace(() -> Strings.format("delaying failure of register request for [%s]", listener), exception);
                        blackholedRegisterOperations.add(onNode(new DisruptableMockTransport.RebootSensitiveRunnable() {
                            @Override
                            public void ifRebooted() {
                                run();
                            }

                            @Override
                            public void run() {
                                listener.onFailure(exception);
                            }

                            @Override
                            public String toString() {
                                return "simulated eventual failure to blackholed register: " + listener;
                            }
                        }));
                    } else {
                        consumer.accept(listener);
                    }
                }

                @Override
                public <R> void runDisruptedOrDrop(ActionListener<R> listener, Consumer<ActionListener<R>> consumer) {
                    if (isDisconnected()) {
                        listener.onFailure(new IOException("simulated disrupted connection to register"));
                    } else if (isBlackholed()) {
                        logger.trace(() -> Strings.format("dropping register request for [%s]", listener));
                    } else {
                        consumer.accept(listener);
                    }
                }

                private boolean isDisconnected() {
                    return disconnectedNodes.contains(localNode.getId())
                        || nodeHealthService.getHealth().getStatus() != HEALTHY
                        || (disruptStorage && rarely());
                }

                private boolean isBlackholed() {
                    return blackholedNodes.contains(localNode.getId());
                }
            }
        }

        private List<TransportAddress> provideSeedHosts(SeedHostsProvider.HostsResolver ignored) {
            return seedHostsList != null
                ? seedHostsList
                : clusterNodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getAddress).collect(Collectors.toList());
        }
    }

    protected TransportInterceptor getTransportInterceptor(DiscoveryNode localNode, ThreadPool threadPool) {
        return NOOP_TRANSPORT_INTERCEPTOR;
    }

    public interface DisruptibleRegisterConnection {

        /**
         * If disconnected, fail the listener; if blackholed then retain the listener for future failure; otherwise pass the listener to the
         * consumer.
         */
        <R> void runDisrupted(ActionListener<R> listener, Consumer<ActionListener<R>> consumer);

        /**
         * If disconnected, fail the listener; if blackholed then do nothing; otherwise pass the listener to the consumer.
         */
        <R> void runDisruptedOrDrop(ActionListener<R> listener, Consumer<ActionListener<R>> consumer);
    }

    protected interface CoordinatorStrategy extends Closeable {
        CoordinationServices getCoordinationServices(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            CoordinationState.PersistedState persistedState,
            DisruptibleRegisterConnection disruptibleRegisterConnection
        );

        CoordinationState.PersistedState createFreshPersistedState(
            DiscoveryNode localNode,
            BooleanSupplier disruptStorage,
            ThreadPool threadPool
        );

        CoordinationState.PersistedState createPersistedStateFromExistingState(
            DiscoveryNode newLocalNode,
            CoordinationState.PersistedState oldState,
            Function<Metadata, Metadata> adaptGlobalMetadata,
            Function<Long, Long> adaptCurrentTerm,
            LongSupplier currentTimeInMillisSupplier,
            NamedWriteableRegistry namedWriteableRegistry,
            BooleanSupplier disruptStorage,
            ThreadPool threadPool
        );

        default void close() {};
    }

    protected interface CoordinationServices {
        ElectionStrategy getElectionStrategy();

        Reconfigurator getReconfigurator();

        LeaderHeartbeatService getLeaderHeartbeatService();

        PreVoteCollector.Factory getPreVoteCollectorFactory();
    }

    public class DefaultCoordinatorStrategy implements CoordinatorStrategy {
        private final ElectionStrategy electionStrategy;

        public DefaultCoordinatorStrategy() {
            this(ElectionStrategy.DEFAULT_INSTANCE);
        }

        public DefaultCoordinatorStrategy(ElectionStrategy electionStrategy) {
            this.electionStrategy = electionStrategy;
        }

        @Override
        public CoordinationServices getCoordinationServices(
            ThreadPool threadPool,
            Settings settings,
            ClusterSettings clusterSettings,
            CoordinationState.PersistedState persistedState,
            DisruptibleRegisterConnection disruptibleRegisterConnection
        ) {
            return new CoordinationServices() {
                @Override
                public ElectionStrategy getElectionStrategy() {
                    return electionStrategy;
                }

                @Override
                public Reconfigurator getReconfigurator() {
                    return new Reconfigurator(settings, clusterSettings);
                }

                @Override
                public LeaderHeartbeatService getLeaderHeartbeatService() {
                    return LeaderHeartbeatService.NO_OP;
                }

                @Override
                public PreVoteCollector.Factory getPreVoteCollectorFactory() {
                    return StatefulPreVoteCollector::new;
                }
            };
        }

        @Override
        public CoordinationState.PersistedState createFreshPersistedState(
            DiscoveryNode localNode,
            BooleanSupplier disruptStorage,
            ThreadPool threadPool
        ) {
            return new MockPersistedState(localNode, disruptStorage);
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
            assert oldState instanceof MockPersistedState : oldState.getClass();
            return new MockPersistedState(
                newLocalNode,
                (MockPersistedState) oldState,
                adaptGlobalMetadata,
                adaptCurrentTerm,
                currentTimeInMillisSupplier,
                namedWriteableRegistry,
                disruptStorage
            );
        }
    }

    protected CoordinatorStrategy createCoordinatorStrategy() {
        return new DefaultCoordinatorStrategy();
    }

    static class AckCollector implements ClusterStatePublisher.AckListener {

        private final Set<DiscoveryNode> ackedNodes = new HashSet<>();
        private final List<DiscoveryNode> successfulNodes = new ArrayList<>();
        private final List<DiscoveryNode> unsuccessfulNodes = new ArrayList<>();

        @Override
        public void onCommit(TimeValue commitTime) {}

        @Override
        public void onNodeAck(DiscoveryNode node, Exception e) {
            assertTrue("duplicate ack from " + node, ackedNodes.add(node));
            if (e == null) {
                successfulNodes.add(node);
            } else {
                unsuccessfulNodes.add(node);
            }
        }

        boolean hasAckedSuccessfully(ClusterNode clusterNode) {
            return successfulNodes.contains(clusterNode.localNode);
        }

        boolean hasAckedUnsuccessfully(ClusterNode clusterNode) {
            return unsuccessfulNodes.contains(clusterNode.localNode);
        }

        boolean hasAcked(ClusterNode clusterNode) {
            return ackedNodes.contains(clusterNode.localNode);
        }

        int getSuccessfulAckIndex(ClusterNode clusterNode) {
            assert successfulNodes.contains(clusterNode.localNode) : "get index of " + clusterNode;
            return successfulNodes.indexOf(clusterNode.localNode);
        }
    }

    static class AckedFakeThreadPoolMasterService extends FakeThreadPoolMasterService {

        AckCollector nextAckCollector = new AckCollector();
        boolean publicationMayFail = false;

        AckedFakeThreadPoolMasterService(
            String nodeName,
            String serviceName,
            ThreadPool threadPool,
            Consumer<Runnable> onTaskAvailableToRun
        ) {
            super(nodeName, threadPool, onTaskAvailableToRun);
        }

        @Override
        protected ClusterStatePublisher.AckListener wrapAckListener(ClusterStatePublisher.AckListener ackListener) {
            final AckCollector ackCollector = nextAckCollector;
            nextAckCollector = new AckCollector();
            return new ClusterStatePublisher.AckListener() {
                @Override
                public void onCommit(TimeValue commitTime) {
                    ackCollector.onCommit(commitTime);
                    ackListener.onCommit(commitTime);
                }

                @Override
                public void onNodeAck(DiscoveryNode node, Exception e) {
                    ackCollector.onNodeAck(node, e);
                    ackListener.onNodeAck(node, e);
                }
            };
        }

        public void allowPublicationFailure() {
            publicationMayFail = true;
        }

        @Override
        protected boolean publicationMayFail() {
            return publicationMayFail;
        }
    }

    static class DisruptableClusterApplierService extends ClusterApplierService {
        private final String nodeName;
        private final String nodeId;
        private final DeterministicTaskQueue deterministicTaskQueue;
        private final ThreadPool threadPool;
        ClusterStateApplyResponse clusterStateApplyResponse = ClusterStateApplyResponse.SUCCEED;
        private boolean applicationMayFail;

        DisruptableClusterApplierService(
            String nodeName,
            String nodeId,
            Settings settings,
            ClusterSettings clusterSettings,
            DeterministicTaskQueue deterministicTaskQueue,
            ThreadPool threadPool
        ) {
            super(nodeName, settings, clusterSettings, threadPool);
            this.nodeName = nodeName;
            this.nodeId = nodeId;
            this.deterministicTaskQueue = deterministicTaskQueue;
            this.threadPool = threadPool;
            addStateApplier(event -> {
                switch (clusterStateApplyResponse) {
                    case SUCCEED, HANG -> {
                        final ClusterState oldClusterState = event.previousState();
                        final ClusterState newClusterState = event.state();
                        assert oldClusterState.version() <= newClusterState.version()
                            : "updating cluster state from version " + oldClusterState + " to stale version " + newClusterState;
                    }
                    case FAIL -> throw new ElasticsearchException("simulated cluster state applier failure");
                }
            });
        }

        @Override
        protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
            return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor(command -> new Runnable() {
                @Override
                public void run() {
                    try (var ignored = DeterministicTaskQueue.getLogContext('{' + nodeName + "}{" + nodeId + '}')) {
                        command.run();
                    }
                }

                @Override
                public String toString() {
                    return "DisruptableClusterApplierService[" + command + "]";
                }
            });
        }

        @Override
        public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ActionListener<Void> listener) {
            if (clusterStateApplyResponse == ClusterStateApplyResponse.HANG) {
                if (randomBoolean()) {
                    // apply cluster state, but don't notify listener
                    super.onNewClusterState(source, clusterStateSupplier, ActionListener.noop());
                }
            } else {
                super.onNewClusterState(source, clusterStateSupplier, listener);
            }
        }

        @Override
        protected void connectToNodesAndWait(ClusterState newClusterState) {
            connectToNodesAsync(newClusterState, () -> {
                // no need to block waiting for handshakes etc. to complete, it's enough to let the NodeConnectionsService take charge of
                // these connections
            });
        }

        @Override
        protected boolean applicationMayFail() {
            return this.applicationMayFail;
        }

        void allowClusterStateApplicationFailure() {
            this.applicationMayFail = true;
        }
    }

    protected static final Set<DiscoveryNodeRole> ALL_ROLES_EXCEPT_VOTING_ONLY = DiscoveryNodeRole.roles()
        .stream()
        .filter(r -> r.equals(DiscoveryNodeRole.VOTING_ONLY_NODE_ROLE) == false)
        .collect(Collectors.toUnmodifiableSet());

    protected DiscoveryNode createDiscoveryNode(int nodeIndex, boolean masterEligible) {
        return DiscoveryNodeUtils.builder("node" + nodeIndex)
            .ephemeralId(UUIDs.randomBase64UUID(random()))  // generated deterministically for repeatable tests
            .roles(masterEligible ? ALL_ROLES_EXCEPT_VOTING_ONLY : emptySet())
            .build();
    }

    /**
     * How to behave with a new cluster state
     */
    enum ClusterStateApplyResponse {
        /**
         * Apply the state (default)
         */
        SUCCEED,

        /**
         * Reject the state with an exception.
         */
        FAIL,

        /**
         * Never respond either way.
         */
        HANG,
    }

    public ClusterState setValue(ClusterState clusterState, int key, long value) {
        return ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .persistentSettings(
                        Settings.builder().put(clusterState.metadata().persistentSettings()).put("value_" + key, value).build()
                    )
                    .build()
            )
            .build();
    }

    public long value(ClusterState clusterState) {
        return value(clusterState, 0);
    }

    public long value(ClusterState clusterState, int key) {
        return clusterState.metadata().persistentSettings().getAsLong("value_" + key, 0L);
    }

    public void assertStateEquals(ClusterState clusterState1, ClusterState clusterState2) {
        assertEquals(clusterState1.version(), clusterState2.version());
        assertEquals(clusterState1.term(), clusterState2.term());
        assertEquals(keySet(clusterState1), keySet(clusterState2));
        for (int key : keySet(clusterState1)) {
            assertEquals(value(clusterState1, key), value(clusterState2, key));
        }
    }

    public Set<Integer> keySet(ClusterState clusterState) {
        return clusterState.metadata()
            .persistentSettings()
            .keySet()
            .stream()
            .filter(s -> s.startsWith("value_"))
            .map(s -> Integer.valueOf(s.substring("value_".length())))
            .collect(Collectors.toSet());
    }

    /**
     * Simple register model. Writes are modeled by providing an integer input. Reads are modeled by providing null as input.
     * Responses that time out are modeled by returning null. Successful writes return the previous value of the register.
     */
    private final SequentialSpec spec = new LinearizabilityChecker.KeyedSpec() {
        @Override
        public Object getKey(Object value) {
            // noinspection rawtypes
            return ((Tuple) value).v1();
        }

        @Override
        public Object getValue(Object value) {
            // noinspection rawtypes
            return ((Tuple) value).v2();
        }

        @Override
        public Object initialState() {
            return 0L;
        }

        @Override
        public Optional<Object> nextState(Object currentState, Object input, Object output) {
            // null input is read, non-null is write
            if (input == null) {
                // history is completed with null, simulating timeout, which assumes that read went through
                if (output == null || currentState.equals(output)) {
                    return Optional.of(currentState);
                }
            } else {
                if (output == null || currentState.equals(output)) {
                    // history is completed with null, simulating timeout, which assumes that write went through
                    return Optional.of(input);
                }
            }
            return Optional.empty();
        }
    };

    public void testRegisterSpecConsistency() {
        assertThat(spec.initialState(), equalTo(0L));
        assertThat(spec.nextState(7, 42, 7), equalTo(Optional.of(42))); // successful write 42 returns previous value 7
        assertThat(spec.nextState(7, 42, null), equalTo(Optional.of(42))); // write 42 times out
        assertThat(spec.nextState(7, null, 7), equalTo(Optional.of(7))); // successful read
        assertThat(spec.nextState(7, null, null), equalTo(Optional.of(7))); // read times out
        assertThat(spec.nextState(7, null, 42), equalTo(Optional.empty()));
    }

    /**
     * A wrapper around a {@link Recycler<BytesRef>} which tracks the refs it allocates so they can be released if the node reboots.
     */
    static class ClearableRecycler implements Recycler<BytesRef> {

        private final Recycler<BytesRef> delegate;
        private final Set<V<BytesRef>> trackedRefs = new HashSet<>();

        ClearableRecycler(Recycler<BytesRef> delegate) {
            this.delegate = delegate;
        }

        @Override
        public V<BytesRef> obtain() {
            V<BytesRef> innerRef = delegate.obtain();
            final V<BytesRef> trackedRef = new V<>() {
                @Override
                public BytesRef v() {
                    return innerRef.v();
                }

                @Override
                public boolean isRecycled() {
                    return innerRef.isRecycled();
                }

                @Override
                public void close() {
                    if (trackedRefs.remove(this)) {
                        innerRef.close();
                    }
                }
            };
            trackedRefs.add(trackedRef);
            return trackedRef;
        }

        /**
         * Release all tracked refs as if the node rebooted.
         */
        void clear() {
            for (V<BytesRef> trackedRef : List.copyOf(trackedRefs)) {
                trackedRef.close();
            }
            assert trackedRefs.isEmpty() : trackedRefs;
        }

    }

    public interface CoordinatorTestClusterStateUpdateTask extends ClusterStateTaskListener {
        default void clusterStateProcessed(ClusterState oldState, ClusterState newState) {}
    }

    class MockPersistedState implements CoordinationState.PersistedState {
        private final CoordinationState.PersistedState delegate;
        private final NodeEnvironment nodeEnvironment;
        private final BooleanSupplier disruptStorage;

        MockPersistedState(DiscoveryNode localNode, BooleanSupplier disruptStorage) {
            this.disruptStorage = disruptStorage;
            try {
                if (rarely()) {
                    nodeEnvironment = newNodeEnvironment();
                    nodeEnvironments.add(nodeEnvironment);
                    final MockGatewayMetaState gatewayMetaState = new MockGatewayMetaState(localNode);
                    gatewayMetaState.start(Settings.EMPTY, nodeEnvironment, xContentRegistry());
                    delegate = gatewayMetaState.getPersistedState();
                } else {
                    nodeEnvironment = null;
                    delegate = new InMemoryPersistedState(
                        0L,
                        ClusterStateUpdaters.addStateNotRecoveredBlock(
                            clusterState(0L, 0L, localNode, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 0L)
                        )
                    );
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to create MockPersistedState", e);
            }
        }

        MockPersistedState(
            DiscoveryNode newLocalNode,
            MockPersistedState oldState,
            Function<Metadata, Metadata> adaptGlobalMetadata,
            Function<Long, Long> adaptCurrentTerm,
            LongSupplier currentTimeInMillisSupplier,
            NamedWriteableRegistry namedWriteableRegistry,
            BooleanSupplier disruptStorage
        ) {
            this.disruptStorage = disruptStorage;
            try {
                if (oldState.nodeEnvironment != null) {
                    nodeEnvironment = oldState.nodeEnvironment;
                    final Metadata updatedMetadata = adaptGlobalMetadata.apply(oldState.getLastAcceptedState().metadata());
                    final long updatedTerm = adaptCurrentTerm.apply(oldState.getCurrentTerm());

                    final Settings.Builder writerSettings = Settings.builder();
                    if (randomBoolean()) {
                        writerSettings.put(
                            PersistedClusterStateService.DOCUMENT_PAGE_SIZE.getKey(),
                            ByteSizeValue.ofBytes(randomLongBetween(1, 1024))
                        );
                    }

                    if (updatedMetadata != oldState.getLastAcceptedState().metadata() || updatedTerm != oldState.getCurrentTerm()) {
                        try (
                            PersistedClusterStateService.Writer writer = new PersistedClusterStateService(
                                nodeEnvironment,
                                xContentRegistry(),
                                new ClusterSettings(writerSettings.build(), ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                                currentTimeInMillisSupplier
                            ).createWriter()
                        ) {
                            writer.writeFullStateAndCommit(
                                updatedTerm,
                                ClusterState.builder(oldState.getLastAcceptedState()).metadata(updatedMetadata).build()
                            );
                        }
                    }
                    final MockGatewayMetaState gatewayMetaState = new MockGatewayMetaState(newLocalNode);
                    gatewayMetaState.start(Settings.EMPTY, nodeEnvironment, xContentRegistry());
                    delegate = gatewayMetaState.getPersistedState();
                } else {
                    nodeEnvironment = null;
                    BytesStreamOutput outStream = new BytesStreamOutput();
                    outStream.setTransportVersion(TransportVersion.current());

                    final long persistedCurrentTerm;

                    if ( // node is master-ineligible either before or after the restart ...
                    (oldState.getLastAcceptedState().nodes().getLocalNode().isMasterNode() && newLocalNode.isMasterNode()) == false
                        // ... and it's accepted some non-initial state so we can roll back ...
                        && (oldState.getLastAcceptedState().term() > 0L || oldState.getLastAcceptedState().version() > 0L)
                        // ... and we're feeling lucky ...
                        && randomBoolean()) {

                        // ... then we might not have reliably persisted the cluster state, so emulate a rollback

                        persistedCurrentTerm = randomLongBetween(0L, oldState.getCurrentTerm());
                        final long lastAcceptedTerm = oldState.getLastAcceptedState().term();
                        final long lastAcceptedVersion = oldState.getLastAcceptedState().version();

                        final long newLastAcceptedTerm;
                        final long newLastAcceptedVersion;

                        if (lastAcceptedVersion == 0L) {
                            newLastAcceptedTerm = randomLongBetween(0L, Math.min(persistedCurrentTerm, lastAcceptedTerm - 1));
                            newLastAcceptedVersion = randomNonNegativeLong();
                        } else {
                            newLastAcceptedTerm = randomLongBetween(0L, Math.min(persistedCurrentTerm, lastAcceptedTerm));
                            newLastAcceptedVersion = randomLongBetween(
                                0L,
                                newLastAcceptedTerm == lastAcceptedTerm ? lastAcceptedVersion - 1 : Long.MAX_VALUE
                            );
                        }
                        final VotingConfiguration newVotingConfiguration = new VotingConfiguration(
                            randomBoolean() ? emptySet() : singleton(randomAlphaOfLength(10))
                        );
                        final long newValue = randomLong();

                        logger.trace(
                            "rolling back persisted cluster state on master-ineligible node [{}]: "
                                + "previously currentTerm={}, lastAcceptedTerm={}, lastAcceptedVersion={} "
                                + "but now currentTerm={}, lastAcceptedTerm={}, lastAcceptedVersion={}",
                            newLocalNode,
                            oldState.getCurrentTerm(),
                            lastAcceptedTerm,
                            lastAcceptedVersion,
                            persistedCurrentTerm,
                            newLastAcceptedTerm,
                            newLastAcceptedVersion
                        );

                        clusterState(
                            newLastAcceptedTerm,
                            newLastAcceptedVersion,
                            newLocalNode,
                            newVotingConfiguration,
                            newVotingConfiguration,
                            newValue
                        ).writeTo(outStream);
                    } else {
                        persistedCurrentTerm = oldState.getCurrentTerm();
                        final Metadata updatedMetadata = adaptGlobalMetadata.apply(oldState.getLastAcceptedState().metadata());
                        if (updatedMetadata != oldState.getLastAcceptedState().metadata()) {
                            ClusterState.builder(oldState.getLastAcceptedState()).metadata(updatedMetadata).build().writeTo(outStream);
                        } else {
                            oldState.getLastAcceptedState().writeTo(outStream);
                        }
                    }

                    final StreamInput inStream = new NamedWriteableAwareStreamInput(
                        outStream.bytes().streamInput(),
                        namedWriteableRegistry
                    );
                    // adapt cluster state to new localNode instance and add blocks
                    delegate = new InMemoryPersistedState(
                        adaptCurrentTerm.apply(persistedCurrentTerm),
                        ClusterStateUpdaters.addStateNotRecoveredBlock(ClusterState.readFrom(inStream, newLocalNode))
                    );
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to create MockPersistedState", e);
            }
        }

        private void possiblyFail(String description) {
            if (disruptStorage.getAsBoolean() && rarely()) {
                logger.trace("simulating IO exception [{}]", description);
                // In the real-life IOError might be thrown, for example if state fsync fails.
                // This will require node restart and we're not emulating it here.
                throw new UncheckedIOException(new IOException("simulated IO exception [" + description + ']'));
            }
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
            possiblyFail("before writing term of " + currentTerm);
            delegate.setCurrentTerm(currentTerm);
        }

        @Override
        public void setLastAcceptedState(ClusterState clusterState) {
            possiblyFail("before writing last-accepted state of term=" + clusterState.term() + ", version=" + clusterState.version());
            delegate.setLastAcceptedState(clusterState);
        }

        @Override
        public void close() {
            assertTrue(openPersistedStates.remove(this));
            try {
                delegate.close();
            } catch (IOException e) {
                throw new AssertionError("unexpected", e);
            }
        }
    }
}
