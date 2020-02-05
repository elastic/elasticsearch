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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.ClusterNode;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.History;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.SequentialSpec;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.SeedHostsProvider;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MockGatewayMetaState;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.test.disruption.DisruptableMockTransport.ConnectionStatus;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.After;
import org.junit.Before;

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
import java.util.function.Consumer;
import java.util.function.Function;
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
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
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
import static org.hamcrest.Matchers.sameInstance;

public class AbstractCoordinatorTestCase extends ESTestCase {

    protected final List<NodeEnvironment> nodeEnvironments = new ArrayList<>();
    protected final Set<Cluster.MockPersistedState> openPersistedStates = new HashSet<>();

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

    // Updating the cluster state involves up to 7 delays:
    // 1. submit the task to the master service
    // 2. send PublishRequest
    // 3. receive PublishResponse
    // 4. send ApplyCommitRequest
    // 5. apply committed cluster state
    // 6. receive ApplyCommitResponse
    // 7. apply committed state on master (last one to apply cluster state)
    public static final long DEFAULT_CLUSTER_STATE_UPDATE_DELAY = 7 * DEFAULT_DELAY_VARIABILITY;

    private static final int ELECTION_RETRIES = 10;

    // The time it takes to complete an election
    public static final long DEFAULT_ELECTION_DELAY
        // Pinging all peers twice should be enough to discover all nodes
        = defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2
        // Then wait for an election to be scheduled; we allow enough time for retries to allow for collisions
        + defaultMillis(ELECTION_INITIAL_TIMEOUT_SETTING) * ELECTION_RETRIES
        + defaultMillis(ELECTION_BACK_OFF_TIME_SETTING) * ELECTION_RETRIES * (ELECTION_RETRIES - 1) / 2
        + defaultMillis(ELECTION_DURATION_SETTING) * ELECTION_RETRIES
        // Allow two round-trip for pre-voting and voting
        + 4 * DEFAULT_DELAY_VARIABILITY
        // Then a commit of the new leader's first cluster state
        + DEFAULT_CLUSTER_STATE_UPDATE_DELAY;

    public static final long DEFAULT_STABILISATION_TIME =
        // If leader just blackholed, need to wait for this to be detected
        (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING))
            * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
            // then wait for a follower to be promoted to leader
            + DEFAULT_ELECTION_DELAY
            // perhaps there is an election collision requiring another publication (which times out) and a term bump
            + defaultMillis(PUBLISH_TIMEOUT_SETTING) + DEFAULT_ELECTION_DELAY
            // then wait for the new leader to notice that the old leader is unresponsive
            + (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
            * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
            // then wait for the new leader to commit a state without the old leader
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY;

    class Cluster implements Releasable {

        static final long EXTREME_DELAY_VARIABILITY = 10000L;
        static final long DEFAULT_DELAY_VARIABILITY = 100L;

        final List<ClusterNode> clusterNodes;
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            // TODO does ThreadPool need a node name any more?
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "deterministic-task-queue").build(), random());
        private boolean disruptStorage;

        final VotingConfiguration initialConfiguration;

        private final Set<String> disconnectedNodes = new HashSet<>();
        private final Set<String> blackholedNodes = new HashSet<>();
        private final Set<Tuple<String,String>> blackholedConnections = new HashSet<>();
        private final Map<Long, ClusterState> committedStatesByVersion = new HashMap<>();
        private final LinearizabilityChecker linearizabilityChecker = new LinearizabilityChecker();
        private final History history = new History();

        private final Function<DiscoveryNode, MockPersistedState> defaultPersistedStateSupplier = MockPersistedState::new;

        @Nullable // null means construct a list from all the current nodes
        private List<TransportAddress> seedHostsList;

        Cluster(int initialNodeCount) {
            this(initialNodeCount, true, Settings.EMPTY);
        }

        Cluster(int initialNodeCount, boolean allNodesMasterEligible, Settings nodeSettings) {
            deterministicTaskQueue.setExecutionDelayVariabilityMillis(DEFAULT_DELAY_VARIABILITY);

            assertThat(initialNodeCount, greaterThan(0));

            final Set<String> masterEligibleNodeIds = new HashSet<>(initialNodeCount);
            clusterNodes = new ArrayList<>(initialNodeCount);
            for (int i = 0; i < initialNodeCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(nextNodeIndex.getAndIncrement(),
                    allNodesMasterEligible || i == 0 || randomBoolean(), nodeSettings);
                clusterNodes.add(clusterNode);
                if (clusterNode.getLocalNode().isMasterNode()) {
                    masterEligibleNodeIds.add(clusterNode.getId());
                }
            }

            initialConfiguration = new VotingConfiguration(new HashSet<>(
                randomSubsetOf(randomIntBetween(1, masterEligibleNodeIds.size()), masterEligibleNodeIds)));

            logger.info("--> creating cluster of {} nodes (master-eligible nodes: {}) with initial configuration {}",
                initialNodeCount, masterEligibleNodeIds, initialConfiguration);
        }

        List<ClusterNode> addNodesAndStabilise(int newNodesCount) {
            final List<ClusterNode> addedNodes = addNodes(newNodesCount);
            stabilise(
                // The first pinging discovers the master
                defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING)
                    // One message delay to send a join
                    + DEFAULT_DELAY_VARIABILITY
                    // Commit a new cluster state with the new node(s). Might be split into multiple commits, and each might need a
                    // followup reconfiguration
                    + newNodesCount * 2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            // TODO Investigate whether 4 publications is sufficient due to batching? A bound linear in the number of nodes isn't great.
            return addedNodes;
        }

        List<ClusterNode> addNodes(int newNodesCount) {
            logger.info("--> adding {} nodes", newNodesCount);

            final List<ClusterNode> addedNodes = new ArrayList<>();
            for (int i = 0; i < newNodesCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(nextNodeIndex.getAndIncrement(), true, Settings.EMPTY);
                addedNodes.add(clusterNode);
            }
            clusterNodes.addAll(addedNodes);
            return addedNodes;
        }

        int size() {
            return clusterNodes.size();
        }

        void runRandomly() {
            runRandomly(true);
        }

        void runRandomly(boolean allowReboots) {

            // TODO supporting (preserving?) existing disruptions needs implementing if needed, for now we just forbid it
            assertThat("may reconnect disconnected nodes, probably unexpected", disconnectedNodes, empty());
            assertThat("may reconnect blackholed nodes, probably unexpected", blackholedNodes, empty());

            final List<Runnable> cleanupActions = new ArrayList<>();
            cleanupActions.add(disconnectedNodes::clear);
            cleanupActions.add(blackholedNodes::clear);
            cleanupActions.add(() -> disruptStorage = false);

            final int randomSteps = scaledRandomIntBetween(10, 10000);
            final int keyRange = randomSteps / 50; // for randomized writes and reads
            logger.info("--> start of safety phase of at least [{}] steps", randomSteps);

            deterministicTaskQueue.setExecutionDelayVariabilityMillis(EXTREME_DELAY_VARIABILITY);
            disruptStorage = true;
            int step = 0;
            long finishTime = -1;

            while (finishTime == -1 || deterministicTaskQueue.getCurrentTimeMillis() <= finishTime) {
                step++;
                final int thisStep = step; // for lambdas

                if (randomSteps <= step && finishTime == -1) {
                    finishTime = deterministicTaskQueue.getLatestDeferredExecutionTime();
                    deterministicTaskQueue.setExecutionDelayVariabilityMillis(DEFAULT_DELAY_VARIABILITY);
                    logger.debug("----> [runRandomly {}] reducing delay variability and running until [{}ms]", step, finishTime);
                }

                try {
                    if (finishTime == -1 && randomBoolean() && randomBoolean() && randomBoolean()) {
                        final ClusterNode clusterNode = getAnyNodePreferringLeaders();
                        final int key = randomIntBetween(0, keyRange);
                        final int newValue = randomInt();
                        clusterNode.onNode(() -> {
                            logger.debug("----> [runRandomly {}] proposing new value [{}] to [{}]",
                                thisStep, newValue, clusterNode.getId());
                            clusterNode.submitValue(key, newValue);
                        }).run();
                    } else if (finishTime == -1 && randomBoolean() && randomBoolean() && randomBoolean()) {
                        final ClusterNode clusterNode = getAnyNodePreferringLeaders();
                        final int key = randomIntBetween(0, keyRange);
                        clusterNode.onNode(() -> {
                            logger.debug("----> [runRandomly {}] reading value from [{}]",
                                thisStep, clusterNode.getId());
                            clusterNode.readValue(key);
                        }).run();
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNodePreferringLeaders();
                        final boolean autoShrinkVotingConfiguration = randomBoolean();
                        clusterNode.onNode(
                            () -> {
                                logger.debug("----> [runRandomly {}] setting auto-shrink configuration to {} on {}",
                                    thisStep, autoShrinkVotingConfiguration, clusterNode.getId());
                                clusterNode.submitSetAutoShrinkVotingConfiguration(autoShrinkVotingConfiguration);
                            }).run();
                    } else if (allowReboots && rarely()) {
                        // reboot random node
                        final ClusterNode clusterNode = getAnyNode();
                        logger.debug("----> [runRandomly {}] rebooting [{}]", thisStep, clusterNode.getId());
                        clusterNode.close();
                        clusterNodes.forEach(
                            cn -> deterministicTaskQueue.scheduleNow(cn.onNode(
                                new Runnable() {
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
                                if (clusterNode.disconnect()) {
                                    logger.debug("----> [runRandomly {}] disconnecting {}", step, clusterNode.getId());
                                }
                                break;
                            case 2:
                                if (clusterNode.blackhole()) {
                                    logger.debug("----> [runRandomly {}] blackholing {}", step, clusterNode.getId());
                                }
                                break;
                        }
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNode();
                        logger.debug("----> [runRandomly {}] applying initial configuration on {}", step, clusterNode.getId());
                        clusterNode.applyInitialConfiguration();
                    } else {
                        if (deterministicTaskQueue.hasDeferredTasks() && randomBoolean()) {
                            deterministicTaskQueue.advanceTime();
                        } else if (deterministicTaskQueue.hasRunnableTasks()) {
                            deterministicTaskQueue.runRandomTask();
                        }
                    }

                    // TODO other random steps:
                    // - reboot a node
                    // - abdicate leadership

                } catch (CoordinationStateRejectedException | UncheckedIOException ignored) {
                    // This is ok: it just means a message couldn't currently be handled.
                }

                assertConsistentStates();
            }

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
                    assertEquals("expected " + applierState + " but got " + storedState,
                        value(applierState), value(storedState));
                }
            }
        }

        void stabilise() {
            stabilise(DEFAULT_STABILISATION_TIME);
        }

        void stabilise(long stabilisationDurationMillis) {
            assertThat("stabilisation requires default delay variability (and proper cleanup of raised variability)",
                deterministicTaskQueue.getExecutionDelayVariabilityMillis(), lessThanOrEqualTo(DEFAULT_DELAY_VARIABILITY));
            assertFalse("stabilisation requires stable storage", disruptStorage);

            bootstrapIfNecessary();

            runFor(stabilisationDurationMillis, "stabilising");

            final ClusterNode leader = getAnyLeader();
            final long leaderTerm = leader.coordinator.getCurrentTerm();

            final int pendingTaskCount = leader.masterService.getFakeMasterServicePendingTaskCount();
            runFor((pendingTaskCount + 1) * DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "draining task queue");

            final Matcher<Long> isEqualToLeaderVersion = equalTo(leader.coordinator.getLastAcceptedState().getVersion());
            final String leaderId = leader.getId();

            assertTrue(leaderId + " has been bootstrapped", leader.coordinator.isInitialConfigurationSet());
            assertTrue(leaderId + " exists in its last-applied state", leader.getLastAppliedClusterState().getNodes().nodeExists(leaderId));
            assertThat(leaderId + " has no NO_MASTER_BLOCK",
                leader.getLastAppliedClusterState().blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID), equalTo(false));
            assertThat(leaderId + " has no STATE_NOT_RECOVERED_BLOCK",
                leader.getLastAppliedClusterState().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK), equalTo(false));
            assertThat(leaderId + " has applied its state ", leader.getLastAppliedClusterState().getVersion(), isEqualToLeaderVersion);

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
                    assertFalse(nodeId + " is not a missing vote for  " + leaderId,
                            leader.coordinator.missingJoinVoteFrom(clusterNode.getLocalNode()));
                    assertThat(nodeId + " has the same accepted state as " + leaderId,
                        clusterNode.coordinator.getLastAcceptedState().getVersion(), isEqualToLeaderVersion);
                    if (clusterNode.getClusterStateApplyResponse() == ClusterStateApplyResponse.SUCCEED) {
                        assertThat(nodeId + " has the same applied state as " + leaderId,
                            clusterNode.getLastAppliedClusterState().getVersion(), isEqualToLeaderVersion);
                        assertTrue(nodeId + " is in its own latest applied state",
                            clusterNode.getLastAppliedClusterState().getNodes().nodeExists(nodeId));
                    }
                    assertTrue(nodeId + " is in the latest applied state on " + leaderId,
                        leader.getLastAppliedClusterState().getNodes().nodeExists(nodeId));
                    assertTrue(nodeId + " has been bootstrapped", clusterNode.coordinator.isInitialConfigurationSet());
                    assertThat(nodeId + " has correct master", clusterNode.getLastAppliedClusterState().nodes().getMasterNode(),
                        equalTo(leader.getLocalNode()));
                    assertThat(nodeId + " has no NO_MASTER_BLOCK",
                        clusterNode.getLastAppliedClusterState().blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID), equalTo(false));
                    assertThat(nodeId + " has no STATE_NOT_RECOVERED_BLOCK",
                        clusterNode.getLastAppliedClusterState().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK), equalTo(false));
                } else {
                    assertThat(nodeId + " is not following " + leaderId, clusterNode.coordinator.getMode(), is(CANDIDATE));
                    assertThat(nodeId + " has no master", clusterNode.getLastAppliedClusterState().nodes().getMasterNode(), nullValue());
                    assertThat(nodeId + " has NO_MASTER_BLOCK",
                        clusterNode.getLastAppliedClusterState().blocks().hasGlobalBlockWithId(NO_MASTER_BLOCK_ID), equalTo(true));
                    assertFalse(nodeId + " is not in the applied state on " + leaderId,
                        leader.getLastAppliedClusterState().getNodes().nodeExists(nodeId));
                }
            }

            final Set<String> connectedNodeIds
                = clusterNodes.stream().filter(n -> isConnectedPair(leader, n)).map(ClusterNode::getId).collect(Collectors.toSet());

            assertThat(leader.getLastAppliedClusterState().getNodes().getSize(), equalTo(connectedNodeIds.size()));

            final ClusterState lastAcceptedState = leader.coordinator.getLastAcceptedState();
            final VotingConfiguration lastCommittedConfiguration = lastAcceptedState.getLastCommittedConfiguration();
            assertTrue(connectedNodeIds + " should be a quorum of " + lastCommittedConfiguration,
                lastCommittedConfiguration.hasQuorum(connectedNodeIds));
            assertThat("leader " + leader.getLocalNode() + " should be part of voting configuration " + lastCommittedConfiguration,
                lastCommittedConfiguration.getNodeIds(), IsCollectionContaining.hasItem(leader.getLocalNode().getId()));

            assertThat("no reconfiguration is in progress",
                lastAcceptedState.getLastCommittedConfiguration(), equalTo(lastAcceptedState.getLastAcceptedConfiguration()));
            assertThat("current configuration is already optimal",
                leader.improveConfiguration(lastAcceptedState), sameInstance(lastAcceptedState));

            logger.info("checking linearizability of history with size {}: {}", history.size(), history);
            final AtomicBoolean abort = new AtomicBoolean();
            // Large histories can be problematic and have the linearizability checker run OOM
            // Bound the time how long the checker can run on such histories (Values empirically determined)
            final ScheduledThreadPoolExecutor scheduler = Scheduler.initScheduler(Settings.EMPTY);
            try {
                if (history.size() > 300) {
                    scheduler.schedule(() -> abort.set(true), 10, TimeUnit.SECONDS);
                }
                final boolean linearizable = linearizabilityChecker.isLinearizable(spec, history, i -> null, abort::get);
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
            return n1 == n2 ||
                (getConnectionStatus(n1.getLocalNode(), n2.getLocalNode()) == ConnectionStatus.CONNECTED
                    && getConnectionStatus(n2.getLocalNode(), n1.getLocalNode()) == ConnectionStatus.CONNECTED);
        }

        ClusterNode getAnyLeader() {
            List<ClusterNode> allLeaders = clusterNodes.stream().filter(ClusterNode::isLeader).collect(Collectors.toList());
            assertThat("leaders", allLeaders, not(empty()));
            return randomFrom(allLeaders);
        }

        private final ConnectionStatus preferredUnknownNodeConnectionStatus =
            randomFrom(ConnectionStatus.DISCONNECTED, ConnectionStatus.BLACK_HOLE);

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
                connectionStatus = usually() ? preferredUnknownNodeConnectionStatus :
                    randomFrom(ConnectionStatus.DISCONNECTED, ConnectionStatus.BLACK_HOLE);
            }
            return connectionStatus;
        }

        boolean nodeExists(DiscoveryNode node) {
            return clusterNodes.stream().anyMatch(cn -> cn.getLocalNode().equals(node));
        }

        ClusterNode getAnyBootstrappableNode() {
            return randomFrom(clusterNodes.stream().filter(n -> n.getLocalNode().isMasterNode())
                .filter(n -> initialConfiguration.getNodeIds().contains(n.getLocalNode().getId()))
                .collect(Collectors.toList()));
        }

        ClusterNode getAnyNode() {
            return getAnyNodeExcept();
        }

        ClusterNode getAnyNodeExcept(ClusterNode... clusterNodes) {
            List<ClusterNode> filteredNodes = getAllNodesExcept(clusterNodes);
            assert filteredNodes.isEmpty() == false;
            return randomFrom(filteredNodes);
        }

        List<ClusterNode> getAllNodesExcept(ClusterNode... clusterNodes) {
            Set<String> forbiddenIds = Arrays.stream(clusterNodes).map(ClusterNode::getId).collect(Collectors.toSet());
            List<ClusterNode> acceptableNodes
                = this.clusterNodes.stream().filter(n -> forbiddenIds.contains(n.getId()) == false).collect(Collectors.toList());
            return acceptableNodes;
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

        @Override
        public void close() {
            clusterNodes.forEach(ClusterNode::close);
        }

        class MockPersistedState implements CoordinationState.PersistedState {
            private final CoordinationState.PersistedState delegate;
            private final NodeEnvironment nodeEnvironment;

            MockPersistedState(DiscoveryNode localNode) {
                try {
                    if (rarely()) {
                        nodeEnvironment = newNodeEnvironment();
                        nodeEnvironments.add(nodeEnvironment);
                        final MockGatewayMetaState gatewayMetaState = new MockGatewayMetaState(localNode);
                        gatewayMetaState.start(Settings.EMPTY, nodeEnvironment, xContentRegistry());
                        delegate = gatewayMetaState.getPersistedState();
                    } else {
                        nodeEnvironment = null;
                        delegate = new InMemoryPersistedState(0L,
                            ClusterStateUpdaters.addStateNotRecoveredBlock(
                                clusterState(0L, 0L, localNode, VotingConfiguration.EMPTY_CONFIG,
                                    VotingConfiguration.EMPTY_CONFIG, 0L)));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Unable to create MockPersistedState", e);
                }
            }

            MockPersistedState(DiscoveryNode newLocalNode, MockPersistedState oldState,
                               Function<MetaData, MetaData> adaptGlobalMetaData, Function<Long, Long> adaptCurrentTerm) {
                try {
                    if (oldState.nodeEnvironment != null) {
                        nodeEnvironment = oldState.nodeEnvironment;
                        final MetaData updatedMetaData = adaptGlobalMetaData.apply(oldState.getLastAcceptedState().metaData());
                        final long updatedTerm = adaptCurrentTerm.apply(oldState.getCurrentTerm());
                        if (updatedMetaData != oldState.getLastAcceptedState().metaData() || updatedTerm != oldState.getCurrentTerm()) {
                            try (PersistedClusterStateService.Writer writer =
                                     new PersistedClusterStateService(nodeEnvironment, xContentRegistry(), BigArrays.NON_RECYCLING_INSTANCE,
                                         new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                                         deterministicTaskQueue::getCurrentTimeMillis)
                                         .createWriter()) {
                                writer.writeFullStateAndCommit(updatedTerm,
                                    ClusterState.builder(oldState.getLastAcceptedState()).metaData(updatedMetaData).build());
                            }
                        }
                        final MockGatewayMetaState gatewayMetaState = new MockGatewayMetaState(newLocalNode);
                        gatewayMetaState.start(Settings.EMPTY, nodeEnvironment, xContentRegistry());
                        delegate = gatewayMetaState.getPersistedState();
                    } else {
                        nodeEnvironment = null;
                        BytesStreamOutput outStream = new BytesStreamOutput();
                        outStream.setVersion(Version.CURRENT);

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
                                newLastAcceptedVersion = randomLongBetween(0L,
                                    newLastAcceptedTerm == lastAcceptedTerm ? lastAcceptedVersion - 1 : Long.MAX_VALUE);
                            }
                            final VotingConfiguration newVotingConfiguration
                                = new VotingConfiguration(randomBoolean() ? emptySet() : singleton(randomAlphaOfLength(10)));
                            final long newValue = randomLong();

                            logger.trace("rolling back persisted cluster state on master-ineligible node [{}]: " +
                                "previously currentTerm={}, lastAcceptedTerm={}, lastAcceptedVersion={} " +
                                "but now currentTerm={}, lastAcceptedTerm={}, lastAcceptedVersion={}", newLocalNode,
                                oldState.getCurrentTerm(), lastAcceptedTerm, lastAcceptedVersion,
                                persistedCurrentTerm, newLastAcceptedTerm, newLastAcceptedVersion);

                            clusterState(newLastAcceptedTerm, newLastAcceptedVersion, newLocalNode, newVotingConfiguration,
                                newVotingConfiguration, newValue).writeTo(outStream);
                        } else {
                            persistedCurrentTerm = oldState.getCurrentTerm();
                            final MetaData updatedMetaData = adaptGlobalMetaData.apply(oldState.getLastAcceptedState().metaData());
                            if (updatedMetaData != oldState.getLastAcceptedState().metaData()) {
                                ClusterState.builder(oldState.getLastAcceptedState()).metaData(updatedMetaData).build().writeTo(outStream);
                            } else {
                                oldState.getLastAcceptedState().writeTo(outStream);
                            }
                        }

                        StreamInput inStream = new NamedWriteableAwareStreamInput(outStream.bytes().streamInput(),
                            new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
                        // adapt cluster state to new localNode instance and add blocks
                        delegate = new InMemoryPersistedState(adaptCurrentTerm.apply(persistedCurrentTerm),
                            ClusterStateUpdaters.addStateNotRecoveredBlock(ClusterState.readFrom(inStream, newLocalNode)));
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException("Unable to create MockPersistedState", e);
                }
            }

            private void possiblyFail(String description) {
                if (disruptStorage && rarely()) {
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

        class ClusterNode {
            private final Logger logger = LogManager.getLogger(ClusterNode.class);

            private final int nodeIndex;
            Coordinator coordinator;
            private final DiscoveryNode localNode;
            final MockPersistedState persistedState;
            final Settings nodeSettings;
            private AckedFakeThreadPoolMasterService masterService;
            private DisruptableClusterApplierService clusterApplierService;
            private ClusterService clusterService;
            TransportService transportService;
            private DisruptableMockTransport mockTransport;
            List<BiConsumer<DiscoveryNode, ClusterState>> extraJoinValidators = new ArrayList<>();

            ClusterNode(int nodeIndex, boolean masterEligible, Settings nodeSettings) {
                this(nodeIndex, createDiscoveryNode(nodeIndex, masterEligible), defaultPersistedStateSupplier, nodeSettings);
            }

            ClusterNode(int nodeIndex, DiscoveryNode localNode, Function<DiscoveryNode, MockPersistedState> persistedStateSupplier,
                        Settings nodeSettings) {
                this.nodeIndex = nodeIndex;
                this.localNode = localNode;
                this.nodeSettings = nodeSettings;
                persistedState = persistedStateSupplier.apply(localNode);
                assertTrue("must use a fresh PersistedState", openPersistedStates.add(persistedState));
                boolean success = false;
                try {
                    onNodeLog(localNode, this::setUp).run();
                    success = true;
                } finally {
                    if (success == false) {
                        persistedState.close(); // removes it from openPersistedStates
                    }
                }
            }

            private void setUp() {
                mockTransport = new DisruptableMockTransport(localNode, logger) {
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
                        return clusterNodes.stream().map(cn -> cn.mockTransport)
                            .filter(transport -> transport.getLocalNode().getAddress().equals(address)).findAny();
                    }
                };

                final Settings settings = nodeSettings.hasValue(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey()) ?
                    nodeSettings : Settings.builder().put(nodeSettings)
                    .putList(ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.getKey(),
                        ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING.get(Settings.EMPTY)).build(); // suppress auto-bootstrap
                transportService = mockTransport.createTransportService(
                    settings, deterministicTaskQueue.getThreadPool(this::onNode),
                    getTransportInterceptor(localNode, deterministicTaskQueue.getThreadPool(this::onNode)),
                    a -> localNode, null, emptySet());
                masterService = new AckedFakeThreadPoolMasterService(localNode.getId(), "test",
                    runnable -> deterministicTaskQueue.scheduleNow(onNode(runnable)));
                final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                clusterApplierService = new DisruptableClusterApplierService(localNode.getId(), settings, clusterSettings,
                    deterministicTaskQueue, this::onNode);
                clusterService = new ClusterService(settings, clusterSettings, masterService, clusterApplierService);
                clusterService.setNodeConnectionsService(
                    new NodeConnectionsService(clusterService.getSettings(), deterministicTaskQueue.getThreadPool(this::onNode),
                        transportService));
                final Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators =
                    Collections.singletonList((dn, cs) -> extraJoinValidators.forEach(validator -> validator.accept(dn, cs)));
                final AllocationService allocationService = ESAllocationTestCase.createAllocationService(Settings.EMPTY);
                coordinator = new Coordinator("test_node", settings, clusterSettings, transportService, writableRegistry(),
                    allocationService, masterService, this::getPersistedState,
                    Cluster.this::provideSeedHosts, clusterApplierService, onJoinValidators, Randomness.get(), (s, p, r) -> {},
                    getElectionStrategy());
                masterService.setClusterStatePublisher(coordinator);
                final GatewayService gatewayService = new GatewayService(settings, allocationService, clusterService,
                    deterministicTaskQueue.getThreadPool(this::onNode), coordinator, null);

                logger.trace("starting up [{}]", localNode);
                transportService.start();
                transportService.acceptIncomingRequests();
                coordinator.start();
                gatewayService.start();
                clusterService.start();
                coordinator.startInitialJoin();
            }

            void close() {
                assertThat("must add nodes to a cluster before closing them", clusterNodes, hasItem(ClusterNode.this));
                onNode(() -> {
                    logger.trace("closing");
                    coordinator.stop();
                    clusterService.stop();
                    //transportService.stop(); // does blocking stuff :/
                    clusterService.close();
                    coordinator.close();
                    //transportService.close(); // does blocking stuff :/
                }).run();
            }

            ClusterNode restartedNode() {
                return restartedNode(Function.identity(), Function.identity(), nodeSettings);
            }

            ClusterNode restartedNode(Function<MetaData, MetaData> adaptGlobalMetaData, Function<Long, Long> adaptCurrentTerm,
                                      Settings nodeSettings) {
                final TransportAddress address = randomBoolean() ? buildNewFakeTransportAddress() : localNode.getAddress();
                final DiscoveryNode newLocalNode = new DiscoveryNode(localNode.getName(), localNode.getId(),
                    UUIDs.randomBase64UUID(random()), // generated deterministically for repeatable tests
                    address.address().getHostString(), address.getAddress(), address, Collections.emptyMap(),
                    localNode.isMasterNode() && Node.NODE_MASTER_SETTING.get(nodeSettings)
                        ? DiscoveryNodeRole.BUILT_IN_ROLES : emptySet(), Version.CURRENT);
                return new ClusterNode(nodeIndex, newLocalNode,
                    node -> new MockPersistedState(newLocalNode, persistedState, adaptGlobalMetaData, adaptCurrentTerm), nodeSettings);
            }

            private CoordinationState.PersistedState getPersistedState() {
                return persistedState;
            }

            String getId() {
                return localNode.getId();
            }

            DiscoveryNode getLocalNode() {
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
                final Runnable wrapped = onNodeLog(localNode, runnable);
                return new Runnable() {
                    @Override
                    public void run() {
                        if (clusterNodes.contains(ClusterNode.this) == false) {
                            logger.trace("ignoring runnable {} from node {} as node has been removed from cluster", runnable, localNode);
                            return;
                        }
                        wrapped.run();
                    }

                    @Override
                    public String toString() {
                        return wrapped.toString();
                    }
                };
            }

            void submitSetAutoShrinkVotingConfiguration(final boolean autoShrinkVotingConfiguration) {
                submitUpdateTask("set master nodes failure tolerance [" + autoShrinkVotingConfiguration + "]", cs ->
                    ClusterState.builder(cs).metaData(
                        MetaData.builder(cs.metaData())
                            .persistentSettings(Settings.builder()
                                .put(cs.metaData().persistentSettings())
                                .put(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.getKey(), autoShrinkVotingConfiguration)
                                .build())
                            .build())
                        .build(), (source, e) -> {});
            }

            AckCollector submitValue(final long value) {
                return submitValue(0, value);
            }

            AckCollector submitValue(final int key, final long value) {
                final int eventId = history.invoke(new Tuple<>(key, value));
                return submitUpdateTask("new value [" + value + "]", cs -> setValue(cs, key, value), new ClusterStateTaskListener() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        history.respond(eventId, value(oldState, key));
                    }

                    @Override
                    public void onNoLongerMaster(String source) {
                        // in this case, we know for sure that event was not processed by the system and will not change history
                        // remove event to help avoid bloated history and state space explosion in linearizability checker
                        history.remove(eventId);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        // do not remove event from history, the write might still take place
                        // instead, complete history when checking for linearizability
                    }
                });
            }

            void readValue(int key) {
                final int eventId = history.invoke(new Tuple<>(key, null));
                submitUpdateTask("read value", cs -> ClusterState.builder(cs).build(), new ClusterStateTaskListener() {
                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        history.respond(eventId, value(newState, key));
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        // reads do not change state
                        // remove event to help avoid bloated history and state space explosion in linearizability checker
                        history.remove(eventId);
                    }
                });
            }

            AckCollector submitUpdateTask(String source, UnaryOperator<ClusterState> clusterStateUpdate,
                                          ClusterStateTaskListener taskListener) {
                final AckCollector ackCollector = new AckCollector();
                onNode(() -> {
                    logger.trace("[{}] submitUpdateTask: enqueueing [{}]", localNode.getId(), source);
                    final long submittedTerm = coordinator.getCurrentTerm();
                    masterService.submitStateUpdateTask(source,
                        new ClusterStateUpdateTask() {
                            @Override
                            public ClusterState execute(ClusterState currentState) {
                                assertThat(currentState.term(), greaterThanOrEqualTo(submittedTerm));
                                masterService.nextAckCollector = ackCollector;
                                return clusterStateUpdate.apply(currentState);
                            }

                            @Override
                            public void onFailure(String source, Exception e) {
                                logger.debug(() -> new ParameterizedMessage("failed to publish: [{}]", source), e);
                                taskListener.onFailure(source, e);
                            }

                            @Override
                            public void onNoLongerMaster(String source) {
                                logger.trace("no longer master: [{}]", source);
                                taskListener.onNoLongerMaster(source);
                            }

                            @Override
                            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                updateCommittedStates();
                                ClusterState state = committedStatesByVersion.get(newState.version());
                                assertNotNull("State not committed : " + newState.toString(), state);
                                assertStateEquals(state, newState);
                                logger.trace("successfully published: [{}]", newState);
                                taskListener.clusterStateProcessed(source, oldState, newState);
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

            void applyInitialConfiguration() {
                onNode(() -> {
                    final Set<String> nodeIdsWithPlaceholders = new HashSet<>(initialConfiguration.getNodeIds());
                    Stream.generate(() -> BOOTSTRAP_PLACEHOLDER_PREFIX + UUIDs.randomBase64UUID(random()))
                        .limit((Math.max(initialConfiguration.getNodeIds().size(), 2) - 1) / 2)
                        .forEach(nodeIdsWithPlaceholders::add);
                    final Set<String> nodeIds = new HashSet<>(
                        randomSubsetOf(initialConfiguration.getNodeIds().size(), nodeIdsWithPlaceholders));
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
                        logger.info(new ParameterizedMessage("failed to set initial configuration to {}",
                            configurationWithPlaceholders), e);
                    }
                }).run();
            }

            private boolean isNotUsefullyBootstrapped() {
                return getLocalNode().isMasterNode() == false || coordinator.isInitialConfigurationSet() == false;
            }

            void allowClusterStateApplicationFailure() {
                clusterApplierService.allowClusterStateApplicationFailure();
            }
        }

        private List<TransportAddress> provideSeedHosts(SeedHostsProvider.HostsResolver ignored) {
            return seedHostsList != null ? seedHostsList
                : clusterNodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getAddress).collect(Collectors.toList());
        }
    }

    protected TransportInterceptor getTransportInterceptor(DiscoveryNode localNode, ThreadPool threadPool) {
        return NOOP_TRANSPORT_INTERCEPTOR;
    }

    protected ElectionStrategy getElectionStrategy() {
        return ElectionStrategy.DEFAULT_INSTANCE;
    }

    public static final String NODE_ID_LOG_CONTEXT_KEY = "nodeId";

    protected static String getNodeIdForLogContext(DiscoveryNode node) {
        return "{" + node.getId() + "}{" + node.getEphemeralId() + "}";
    }

    public static Runnable onNodeLog(DiscoveryNode node, Runnable runnable) {
        final String nodeId = getNodeIdForLogContext(node);
        return new Runnable() {
            @Override
            public void run() {
                try (CloseableThreadContext.Instance ignored = CloseableThreadContext.put(NODE_ID_LOG_CONTEXT_KEY, nodeId)) {
                    runnable.run();
                }
            }

            @Override
            public String toString() {
                return nodeId + ": " + runnable.toString();
            }
        };
    }

    static class AckCollector implements ClusterStatePublisher.AckListener {

        private final Set<DiscoveryNode> ackedNodes = new HashSet<>();
        private final List<DiscoveryNode> successfulNodes = new ArrayList<>();
        private final List<DiscoveryNode> unsuccessfulNodes = new ArrayList<>();

        @Override
        public void onCommit(TimeValue commitTime) {
            // TODO we only currently care about per-node acks
        }

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

        AckedFakeThreadPoolMasterService(String nodeName, String serviceName, Consumer<Runnable> onTaskAvailableToRun) {
            super(nodeName, serviceName, onTaskAvailableToRun);
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
    }

    static class DisruptableClusterApplierService extends ClusterApplierService {
        private final String nodeName;
        private final DeterministicTaskQueue deterministicTaskQueue;
        ClusterStateApplyResponse clusterStateApplyResponse = ClusterStateApplyResponse.SUCCEED;
        private boolean applicationMayFail;

        DisruptableClusterApplierService(String nodeName, Settings settings, ClusterSettings clusterSettings,
                                         DeterministicTaskQueue deterministicTaskQueue, Function<Runnable, Runnable> runnableWrapper) {
            super(nodeName, settings, clusterSettings, deterministicTaskQueue.getThreadPool(runnableWrapper));
            this.nodeName = nodeName;
            this.deterministicTaskQueue = deterministicTaskQueue;
            addStateApplier(event -> {
                switch (clusterStateApplyResponse) {
                    case SUCCEED:
                    case HANG:
                        final ClusterState oldClusterState = event.previousState();
                        final ClusterState newClusterState = event.state();
                        assert oldClusterState.version() <= newClusterState.version() : "updating cluster state from version "
                            + oldClusterState.version() + " to stale version " + newClusterState.version();
                        break;
                    case FAIL:
                        throw new ElasticsearchException("simulated cluster state applier failure");
                }
            });
        }

        @Override
        protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
            return new MockSinglePrioritizingExecutor(nodeName, deterministicTaskQueue);
        }

        @Override
        public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
            if (clusterStateApplyResponse == ClusterStateApplyResponse.HANG) {
                if (randomBoolean()) {
                    // apply cluster state, but don't notify listener
                    super.onNewClusterState(source, clusterStateSupplier, (source1, e) -> {
                        // ignore result
                    });
                }
            } else {
                super.onNewClusterState(source, clusterStateSupplier, listener);
            }
        }

        @Override
        protected void connectToNodesAndWait(ClusterState newClusterState) {
            // don't do anything, and don't block
        }

        @Override
        protected boolean applicationMayFail() {
            return this.applicationMayFail;
        }

        void allowClusterStateApplicationFailure() {
            this.applicationMayFail = true;
        }
    }

    protected DiscoveryNode createDiscoveryNode(int nodeIndex, boolean masterEligible) {
        final TransportAddress address = buildNewFakeTransportAddress();
        return new DiscoveryNode("", "node" + nodeIndex,
            UUIDs.randomBase64UUID(random()), // generated deterministically for repeatable tests
            address.address().getHostString(), address.getAddress(), address, Collections.emptyMap(),
            masterEligible ? DiscoveryNodeRole.BUILT_IN_ROLES : emptySet(), Version.CURRENT);
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
        return ClusterState.builder(clusterState).metaData(
            MetaData.builder(clusterState.metaData())
                .persistentSettings(Settings.builder()
                    .put(clusterState.metaData().persistentSettings())
                    .put("value_" + key, value)
                    .build())
                .build())
            .build();
    }

    public long value(ClusterState clusterState) {
        return value(clusterState, 0);
    }

    public long value(ClusterState clusterState, int key) {
        return clusterState.metaData().persistentSettings().getAsLong("value_" + key, 0L);
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
        return clusterState.metaData().persistentSettings().keySet().stream()
            .filter(s -> s.startsWith("value_")).map(s -> Integer.valueOf(s.substring("value_".length()))).collect(Collectors.toSet());
    }

    /**
     * Simple register model. Writes are modeled by providing an integer input. Reads are modeled by providing null as input.
     * Responses that time out are modeled by returning null. Successful writes return the previous value of the register.
     */
    private final SequentialSpec spec = new LinearizabilityChecker.KeyedSpec() {
        @Override
        public Object getKey(Object value) {
            return ((Tuple) value).v1();
        }

        @Override
        public Object getValue(Object value) {
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
                return Optional.empty();
            } else {
                if (output == null || currentState.equals(output)) {
                    // history is completed with null, simulating timeout, which assumes that write went through
                    return Optional.of(input);
                }
                return Optional.empty();
            }
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

}
