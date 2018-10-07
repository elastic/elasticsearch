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

import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.VotingConfiguration;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.CoordinationStateTests.InMemoryPersistedState;
import org.elasticsearch.cluster.coordination.CoordinatorTests.Cluster.ClusterNode;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterApplier;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.discovery.zen.UnicastHostsProvider.HostsResolver;
import org.elasticsearch.indices.cluster.FakeThreadPoolMasterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.test.disruption.DisruptableMockTransport.ConnectionStatus;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.setValue;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.value;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.FOLLOWER;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.LEADER;
import static org.elasticsearch.cluster.coordination.Coordinator.PUBLISH_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.CoordinatorTests.Cluster.DEFAULT_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.HANDSHAKE_ACTION_NAME;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

@TestLogging("org.elasticsearch.cluster.coordination:TRACE,org.elasticsearch.discovery:TRACE")
public class CoordinatorTests extends ESTestCase {

    @Before
    public void resetPortCounterBeforeEachTest() {
        resetPortCounter();
    }

    public void testCanUpdateClusterStateAfterStabilisation() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        long finalValue = randomLong();

        logger.info("--> submitting value [{}] to [{}]", finalValue, leader);
        leader.submitValue(finalValue);
        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String nodeId = clusterNode.getId();
            final ClusterState appliedState = clusterNode.getLastAppliedClusterState();
            assertThat(nodeId + " has the applied value", value(appliedState), is(finalValue));
        }
    }

    public void testNodesJoinAfterStableCluster() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();

        final long currentTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
        final int newNodesCount = randomIntBetween(1, 2);
        cluster.addNodes(newNodesCount);
        cluster.stabilise(
            // The first pinging discovers the master
            defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING)
                // One message delay to send a join
                + DEFAULT_DELAY_VARIABILITY
                // Commit a new cluster state with the new node(s). Might be split into multiple commits
                + newNodesCount * DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

        final long newTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
        assertEquals(currentTerm, newTerm);
    }

    public void testLeaderDisconnectionDetectedQuickly() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();

        final ClusterNode originalLeader = cluster.getAnyLeader();
        logger.info("--> disconnecting leader {}", originalLeader);
        originalLeader.disconnect();

        cluster.stabilise(Math.max(
            // Each follower may have just sent a leader check, which receives no response
            // TODO not necessary if notified of disconnection
            defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)
                // then wait for the follower to check the leader
                + defaultMillis(LEADER_CHECK_INTERVAL_SETTING)
                // then wait for the exception response
                + DEFAULT_DELAY_VARIABILITY
                // then wait for a new election
                + DEFAULT_ELECTION_DELAY
                // then wait for the old leader's removal to be committed
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

            // ALSO the leader may have just sent a follower check, which receives no response
            // TODO unnecessary if notified of disconnection
            defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)
                // wait for the leader to check its followers
                + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                // then wait for the exception response
                + DEFAULT_DELAY_VARIABILITY
                // then wait for the removal to be committed
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
        ));

        assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
    }

    public void testUnresponsiveLeaderDetectedEventually() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();

        final ClusterNode originalLeader = cluster.getAnyLeader();
        logger.info("--> blackholing leader {}", originalLeader);
        originalLeader.blackhole();

        // This stabilisation time bound is undesirably long. TODO try and reduce it.
        cluster.stabilise(Math.max(
            // first wait for all the followers to notice the leader has gone
            (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING))
                * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
                // then wait for a follower to be promoted to leader
                + DEFAULT_ELECTION_DELAY
                // and the first publication times out because of the unresponsive node
                + defaultMillis(PUBLISH_TIMEOUT_SETTING)
                // there might be a term bump causing another election
                + DEFAULT_ELECTION_DELAY

                // then wait for both of:
                + Math.max(
                // 1. the term bumping publication to time out
                defaultMillis(PUBLISH_TIMEOUT_SETTING),
                // 2. the new leader to notice that the old leader is unresponsive
                (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                    * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING))

                // then wait for the new leader to commit a state without the old leader
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

            // ALSO wait for the leader to notice that its followers are unresponsive
            (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
                // then wait for the leader to try and commit a state removing them, causing it to stand down
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
        ));

        assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
    }

    public void testFollowerDisconnectionDetectedQuickly() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower = cluster.getAnyNodeExcept(leader);
        logger.info("--> disconnecting follower {}", follower);
        follower.disconnect();

        cluster.stabilise(Math.max(
            // the leader may have just sent a follower check, which receives no response
            // TODO unnecessary if notified of disconnection
            defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)
                // wait for the leader to check the follower
                + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                // then wait for the exception response
                + DEFAULT_DELAY_VARIABILITY
                // then wait for the removal to be committed
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

            // ALSO the follower may have just sent a leader check, which receives no response
            // TODO not necessary if notified of disconnection
            defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)
                // then wait for the follower to check the leader
                + defaultMillis(LEADER_CHECK_INTERVAL_SETTING)
                // then wait for the exception response, causing the follower to become a candidate
                + DEFAULT_DELAY_VARIABILITY
        ));
        assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
    }

    public void testUnresponsiveFollowerDetectedEventually() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower = cluster.getAnyNodeExcept(leader);
        logger.info("--> blackholing follower {}", follower);
        follower.blackhole();

        cluster.stabilise(Math.max(
            // wait for the leader to notice that the follower is unresponsive
            (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
                // then wait for the leader to commit a state without the follower
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

            // ALSO wait for the follower to notice the leader is unresponsive
            (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING))
                * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
        ));
        assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
    }

    public void testAckListenerReceivesAcksFromAllNodes() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        AckCollector ackCollector = leader.submitValue(randomLong());
        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertTrue("expected ack from " + clusterNode, ackCollector.hasAckedSuccessfully(clusterNode));
        }
        assertThat("leader should be last to ack", ackCollector.getSuccessfulAckIndex(leader), equalTo(cluster.clusterNodes.size() - 1));
    }

    public void testAckListenerReceivesNackFromFollower() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.FAIL);
        AckCollector ackCollector = leader.submitValue(randomLong());
        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
        assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
        assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
        assertThat("leader should be last to ack", ackCollector.getSuccessfulAckIndex(leader), equalTo(1));
    }

    public void testAckListenerReceivesNackFromLeader() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);
        final long startingTerm = leader.coordinator.getCurrentTerm();

        leader.setClusterStateApplyResponse(ClusterStateApplyResponse.FAIL);
        AckCollector ackCollector = leader.submitValue(randomLong());
        cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing value");
        assertTrue(leader.coordinator.getMode() != Coordinator.Mode.LEADER || leader.coordinator.getCurrentTerm() > startingTerm);
        leader.setClusterStateApplyResponse(ClusterStateApplyResponse.SUCCEED);
        cluster.stabilise();
        assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
        assertTrue("expected ack from " + follower0, ackCollector.hasAckedSuccessfully(follower0));
        assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
        assertTrue(leader.coordinator.getMode() != Coordinator.Mode.LEADER || leader.coordinator.getCurrentTerm() > startingTerm);
    }

    public void testAckListenerReceivesNoAckFromHangingFollower() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.HANG);
        AckCollector ackCollector = leader.submitValue(randomLong());
        cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing value");
        assertTrue("expected immediate ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
        assertFalse("expected no ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        cluster.stabilise();
        assertTrue("expected eventual ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        assertFalse("expected no ack from " + follower0, ackCollector.hasAcked(follower0));
    }

    public void testAckListenerReceivesNacksIfPublicationTimesOut() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        follower0.blackhole();
        follower1.blackhole();
        AckCollector ackCollector = leader.submitValue(randomLong());
        cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing value");
        assertFalse("expected no immediate ack from " + leader, ackCollector.hasAcked(leader));
        assertFalse("expected no immediate ack from " + follower0, ackCollector.hasAcked(follower0));
        assertFalse("expected no immediate ack from " + follower1, ackCollector.hasAcked(follower1));
        follower0.heal();
        follower1.heal();
        cluster.stabilise();
        assertTrue("expected eventual nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
        assertTrue("expected eventual nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
        assertTrue("expected eventual nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
    }

    public void testAckListenerReceivesNacksIfLeaderStandsDown() {
        // TODO: needs support for handling disconnects
//        final Cluster cluster = new Cluster(3);
//        cluster.runRandomly();
//        cluster.stabilise();
//        final ClusterNode leader = cluster.getAnyLeader();
//        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
//        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);
//
//        leader.partition();
//        follower0.coordinator.handleDisconnectedNode(leader.localNode);
//        follower1.coordinator.handleDisconnectedNode(leader.localNode);
//        cluster.runUntil(cluster.getCurrentTimeMillis() + cluster.DEFAULT_ELECTION_TIME);
//        AckCollector ackCollector = leader.submitRandomValue();
//        cluster.runUntil(cluster.currentTimeMillis + Cluster.DEFAULT_DELAY_VARIABILITY);
//        leader.connectionStatus = ConnectionStatus.CONNECTED;
//        cluster.stabilise(cluster.DEFAULT_STABILISATION_TIME, 0L);
//        assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
//        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
//        assertTrue("expected nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
    }

    public void testAckListenerReceivesNacksFromFollowerInHigherTerm() {
        // TODO: needs proper term bumping
//        final Cluster cluster = new Cluster(3);
//        cluster.runRandomly();
//        cluster.stabilise();
//        final ClusterNode leader = cluster.getAnyLeader();
//        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
//        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);
//
//        follower0.coordinator.joinLeaderInTerm(new StartJoinRequest(follower0.localNode, follower0.coordinator.getCurrentTerm() + 1));
//        AckCollector ackCollector = leader.submitValue(randomLong());
//        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
//        assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
//        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
//        assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
    }

    public void testSettingInitialConfigurationTriggersElection() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runFor(defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2 + randomLongBetween(0, 60000), "initial discovery phase");
        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String nodeId = clusterNode.getId();
            assertThat(nodeId + " is CANDIDATE", clusterNode.coordinator.getMode(), is(CANDIDATE));
            assertThat(nodeId + " is in term 0", clusterNode.coordinator.getCurrentTerm(), is(0L));
            assertThat(nodeId + " last accepted in term 0", clusterNode.coordinator.getLastAcceptedState().term(), is(0L));
            assertThat(nodeId + " last accepted version 0", clusterNode.coordinator.getLastAcceptedState().version(), is(0L));
            assertTrue(nodeId + " has an empty last-accepted configuration",
                clusterNode.coordinator.getLastAcceptedState().getLastAcceptedConfiguration().isEmpty());
            assertTrue(nodeId + " has an empty last-committed configuration",
                clusterNode.coordinator.getLastAcceptedState().getLastCommittedConfiguration().isEmpty());
        }

        cluster.getAnyNode().applyInitialConfiguration();
        cluster.stabilise(defaultMillis(
            // the first election should succeed
            ELECTION_INITIAL_TIMEOUT_SETTING) // TODO this wait is unnecessary, we could trigger the election immediately
            // Allow two round-trip for pre-voting and voting
            + 4 * DEFAULT_DELAY_VARIABILITY
            // Then a commit of the new leader's first cluster state
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
    }

    public void testCannotSetInitialConfigurationTwice() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.setInitialConfigurationIfRequired();
        cluster.stabilise();

        final Coordinator coordinator = cluster.getAnyNode().coordinator;
        final CoordinationStateRejectedException exception = expectThrows(CoordinationStateRejectedException.class,
            () -> coordinator.setInitialConfiguration(coordinator.getLastAcceptedState().getLastCommittedConfiguration()));

        assertThat(exception.getMessage(), is("Cannot set initial configuration: configuration has already been set"));
    }

    public void testCannotSetInitialConfigurationWithoutQuorum() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        final Coordinator coordinator = cluster.getAnyNode().coordinator;
        final VotingConfiguration unknownNodeConfiguration = new VotingConfiguration(Collections.singleton("unknown-node"));
        final CoordinationStateRejectedException exception = expectThrows(CoordinationStateRejectedException.class,
            () -> coordinator.setInitialConfiguration(unknownNodeConfiguration));
        assertThat(exception.getMessage(), is("Cannot set initial configuration: no quorum found yet"));

        // This is VERY BAD: setting a _different_ initial configuration. Yet it works if the first attempt will never be a quorum.
        coordinator.setInitialConfiguration(new VotingConfiguration(Collections.singleton(coordinator.getLocalNode().getId())));
        cluster.stabilise();
    }

    private static long defaultMillis(Setting<TimeValue> setting) {
        return setting.get(Settings.EMPTY).millis() + Cluster.DEFAULT_DELAY_VARIABILITY;
    }

    private static int defaultInt(Setting<Integer> setting) {
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
    private static final long DEFAULT_CLUSTER_STATE_UPDATE_DELAY = 7 * DEFAULT_DELAY_VARIABILITY;

    private static final int ELECTION_RETRIES = 10;

    // The time it takes to complete an election
    private static final long DEFAULT_ELECTION_DELAY
        // Pinging all peers twice should be enough to discover all nodes
        = defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2
        // Then wait for an election to be scheduled; we allow enough time for retries to allow for collisions
        + defaultMillis(ELECTION_INITIAL_TIMEOUT_SETTING) * ELECTION_RETRIES
        + defaultMillis(ELECTION_BACK_OFF_TIME_SETTING) * ELECTION_RETRIES * (ELECTION_RETRIES - 1) / 2
        // Allow two round-trip for pre-voting and voting
        + 4 * DEFAULT_DELAY_VARIABILITY
        // Then a commit of the new leader's first cluster state
        + DEFAULT_CLUSTER_STATE_UPDATE_DELAY;

    private static final long DEFAULT_STABILISATION_TIME =
        // If leader just blackholed, need to wait for this to be detected
        (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING))
            * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
            // then wait for a follower to be promoted to leader
            + DEFAULT_ELECTION_DELAY
            // then wait for the new leader to notice that the old leader is unresponsive
            + (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
            * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
            // then wait for the new leader to commit a state without the old leader
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY;

    private static String nodeIdFromIndex(int nodeIndex) {
        return "node" + nodeIndex;
    }

    class Cluster {

        static final long EXTREME_DELAY_VARIABILITY = 10000L;
        static final long DEFAULT_DELAY_VARIABILITY = 100L;

        final List<ClusterNode> clusterNodes;
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            // TODO does ThreadPool need a node name any more?
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "deterministic-task-queue").build(), random());
        private final VotingConfiguration initialConfiguration;

        private final Set<String> disconnectedNodes = new HashSet<>();
        private final Set<String> blackholedNodes = new HashSet<>();
        private final Map<Long, ClusterState> committedStatesByVersion = new HashMap<>();

        Cluster(int initialNodeCount) {
            deterministicTaskQueue.setExecutionDelayVariabilityMillis(DEFAULT_DELAY_VARIABILITY);

            logger.info("--> creating cluster of {} nodes", initialNodeCount);

            Set<String> initialNodeIds = new HashSet<>(initialNodeCount);
            for (int i = 0; i < initialNodeCount; i++) {
                initialNodeIds.add(nodeIdFromIndex(i));
            }
            initialConfiguration = new VotingConfiguration(initialNodeIds);

            clusterNodes = new ArrayList<>(initialNodeCount);
            for (int i = 0; i < initialNodeCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(i);
                clusterNodes.add(clusterNode);
            }
        }

        void addNodes(int newNodesCount) {
            logger.info("--> adding {} nodes", newNodesCount);

            final int nodeSizeAtStart = clusterNodes.size();
            for (int i = 0; i < newNodesCount; i++) {
                final ClusterNode clusterNode = new ClusterNode(nodeSizeAtStart + i);
                clusterNodes.add(clusterNode);
            }
        }

        void setInitialConfigurationIfRequired() {
            if (clusterNodes.stream().allMatch(n -> n.coordinator.getLastAcceptedState().getLastAcceptedConfiguration().isEmpty())) {
                assertThat("setting initial configuration may fail with disconnected nodes", disconnectedNodes, empty());
                assertThat("setting initial configuration may fail with blackholed nodes", blackholedNodes, empty());
                runFor(defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2, "discovery prior to setting initial configuration");
                final ClusterNode bootstrapNode = getAnyNode();
                bootstrapNode.applyInitialConfiguration();
            } else {
                logger.info("--> setting initial configuration not required");
            }
        }

        void runRandomly() {

            // TODO supporting (preserving?) existing disruptions needs implementing if needed, for now we just forbid it
            assertThat("may reconnect disconnected nodes, probably unexpected", disconnectedNodes, empty());
            assertThat("may reconnect blackholed nodes, probably unexpected", blackholedNodes, empty());

            final int randomSteps = scaledRandomIntBetween(10, 10000);
            logger.info("--> start of safety phase of at least [{}] steps", randomSteps);

            deterministicTaskQueue.setExecutionDelayVariabilityMillis(EXTREME_DELAY_VARIABILITY);
            int step = 0;
            long finishTime = -1;

            while (finishTime == -1 || deterministicTaskQueue.getCurrentTimeMillis() <= finishTime) {
                step++;
                final int thisStep = step; // for lambdas

                if (randomSteps <= step && finishTime == -1) {
                    finishTime = deterministicTaskQueue.getLatestDeferredExecutionTime();
                    deterministicTaskQueue.setExecutionDelayVariabilityMillis(DEFAULT_DELAY_VARIABILITY);
                    logger.debug("----> [runRandomly {}] reducing delay variability and running until [{}]", step, finishTime);
                }

                try {
                    if (rarely()) {
                        final ClusterNode clusterNode = getAnyNodePreferringLeaders();
                        final int newValue = randomInt();
                        onNode(clusterNode.getLocalNode(), () -> {
                            logger.debug("----> [runRandomly {}] proposing new value [{}] to [{}]",
                                thisStep, newValue, clusterNode.getId());
                            clusterNode.submitValue(newValue);
                        }).run();
                    } else if (rarely()) {
                        final ClusterNode clusterNode = getAnyNode();
                        onNode(clusterNode.getLocalNode(), () -> {
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
                        onNode(clusterNode.getLocalNode(),
                            () -> {
                                logger.debug("----> [runRandomly {}] applying initial configuration {} to {}",
                                    thisStep, initialConfiguration, clusterNode.getId());
                                clusterNode.coordinator.setInitialConfiguration(initialConfiguration);
                            }).run();
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

                } catch (CoordinationStateRejectedException ignored) {
                    // This is ok: it just means a message couldn't currently be handled.
                }

                assertConsistentStates();
            }

            disconnectedNodes.clear();
            blackholedNodes.clear();
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
            runFor(stabilisationDurationMillis, "stabilising");
            fixLag();
            assertUniqueLeaderAndExpectedModes();
        }

        // TODO remove this when lag detection is implemented
        void fixLag() {
            final ClusterNode leader = getAnyLeader();
            final long leaderVersion = leader.coordinator.getLastAcceptedState().version();
            final long minVersion = clusterNodes.stream()
                .filter(n -> isConnectedPair(n, leader))
                .map(n -> n.coordinator.getLastAcceptedState().version()).min(Long::compare).orElse(Long.MIN_VALUE);

            assert minVersion >= 0;
            if (minVersion < leaderVersion) {
                logger.info("--> publishing a value to fix lag, leaderVersion={}, minVersion={}", leaderVersion, minVersion);
                onNode(leader.getLocalNode(), () -> {
                    synchronized (leader.coordinator.mutex) {
                        leader.submitValue(randomLong());
                    }
                }).run();
            }
            runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "re-stabilising after lag-fixing publication");
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

        private void assertUniqueLeaderAndExpectedModes() {
            final ClusterNode leader = getAnyLeader();
            final long leaderTerm = leader.coordinator.getCurrentTerm();
            Matcher<Long> isPresentAndEqualToLeaderVersion
                = equalTo(leader.coordinator.getLastAcceptedState().getVersion());

            assertTrue(leader.getLastAppliedClusterState().getNodes().nodeExists(leader.getId()));
            assertThat(leader.getLastAppliedClusterState().getVersion(), isPresentAndEqualToLeaderVersion);

            for (final ClusterNode clusterNode : clusterNodes) {
                final String nodeId = clusterNode.getId();
                assertFalse(nodeId + " should not have an active publication", clusterNode.coordinator.publicationInProgress());

                if (clusterNode == leader) {
                    continue;
                }

                if (isConnectedPair(leader, clusterNode)) {
                    assertThat(nodeId + " is a follower", clusterNode.coordinator.getMode(), is(FOLLOWER));
                    assertThat(nodeId + " has the same term as the leader", clusterNode.coordinator.getCurrentTerm(), is(leaderTerm));
                    assertTrue(nodeId + " has voted for the leader", leader.coordinator.hasJoinVoteFrom(clusterNode.getLocalNode()));
                    // TODO assert that this node's accepted and committed states are the same as the leader's

                    assertTrue(nodeId + " is in the leader's applied state",
                        leader.getLastAppliedClusterState().getNodes().nodeExists(nodeId));
                } else {
                    assertThat(nodeId + " is a candidate", clusterNode.coordinator.getMode(), is(CANDIDATE));
                    assertFalse(nodeId + " is not in the leader's applied state",
                        leader.getLastAppliedClusterState().getNodes().nodeExists(nodeId));
                }
            }

            int connectedNodeCount = Math.toIntExact(clusterNodes.stream().filter(n -> isConnectedPair(leader, n)).count());
            assertThat(leader.getLastAppliedClusterState().getNodes().getSize(), equalTo(connectedNodeCount));
        }

        ClusterNode getAnyLeader() {
            List<ClusterNode> allLeaders = clusterNodes.stream().filter(ClusterNode::isLeader).collect(Collectors.toList());
            assertThat("leaders", allLeaders, not(empty()));
            return randomFrom(allLeaders);
        }

        private ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination) {
            ConnectionStatus connectionStatus;
            if (blackholedNodes.contains(sender.getId()) || blackholedNodes.contains(destination.getId())) {
                connectionStatus = ConnectionStatus.BLACK_HOLE;
            } else if (disconnectedNodes.contains(sender.getId()) || disconnectedNodes.contains(destination.getId())) {
                connectionStatus = ConnectionStatus.DISCONNECTED;
            } else {
                connectionStatus = ConnectionStatus.CONNECTED;
            }
            return connectionStatus;
        }

        ClusterNode getAnyNode() {
            return getAnyNodeExcept();
        }

        ClusterNode getAnyNodeExcept(ClusterNode... clusterNodes) {
            Set<String> forbiddenIds = Arrays.stream(clusterNodes).map(ClusterNode::getId).collect(Collectors.toSet());
            List<ClusterNode> acceptableNodes
                = this.clusterNodes.stream().filter(n -> forbiddenIds.contains(n.getId()) == false).collect(Collectors.toList());
            assert acceptableNodes.isEmpty() == false;
            return randomFrom(acceptableNodes);
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

        class ClusterNode extends AbstractComponent {
            private final int nodeIndex;
            private Coordinator coordinator;
            private DiscoveryNode localNode;
            private final PersistedState persistedState;
            private FakeClusterApplier clusterApplier;
            private AckedFakeThreadPoolMasterService masterService;
            private TransportService transportService;
            private DisruptableMockTransport mockTransport;
            private ClusterStateApplyResponse clusterStateApplyResponse = ClusterStateApplyResponse.SUCCEED;

            ClusterNode(int nodeIndex) {
                super(Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeIdFromIndex(nodeIndex)).build());
                this.nodeIndex = nodeIndex;
                localNode = createDiscoveryNode();
                persistedState = new InMemoryPersistedState(0L,
                    clusterState(0L, 0L, localNode, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 0L));
                onNode(localNode, this::setUp).run();
            }

            private DiscoveryNode createDiscoveryNode() {
                return CoordinationStateTests.createNode(nodeIdFromIndex(nodeIndex));
            }

            private void setUp() {
                mockTransport = new DisruptableMockTransport(logger) {
                    @Override
                    protected DiscoveryNode getLocalNode() {
                        return localNode;
                    }

                    @Override
                    protected ConnectionStatus getConnectionStatus(DiscoveryNode sender, DiscoveryNode destination) {
                        return Cluster.this.getConnectionStatus(sender, destination);
                    }

                    @Override
                    protected Optional<DisruptableMockTransport> getDisruptedCapturingTransport(DiscoveryNode node, String action) {
                        final Predicate<ClusterNode> matchesDestination;
                        if (action.equals(HANDSHAKE_ACTION_NAME)) {
                            matchesDestination = n -> n.getLocalNode().getAddress().equals(node.getAddress());
                        } else {
                            matchesDestination = n -> n.getLocalNode().equals(node);
                        }
                        return clusterNodes.stream().filter(matchesDestination).findAny().map(cn -> cn.mockTransport);
                    }

                    @Override
                    protected void handle(DiscoveryNode sender, DiscoveryNode destination, String action, Runnable doDelivery) {
                        // handshake needs to run inline as the caller blockingly waits on the result
                        if (action.equals(HANDSHAKE_ACTION_NAME)) {
                            onNode(destination, doDelivery).run();
                        } else {
                            deterministicTaskQueue.scheduleNow(onNode(destination, doDelivery));
                        }
                    }

                    @Override
                    protected void onBlackholedDuringSend(long requestId, String action, DiscoveryNode destination) {
                        if (action.equals(HANDSHAKE_ACTION_NAME)) {
                            logger.trace("ignoring blackhole and delivering {}", getRequestDescription(requestId, action, destination));
                            // handshakes always have a timeout, and are sent in a blocking fashion, so we must respond with an exception.
                            sendFromTo(destination, getLocalNode(), action, getDisconnectException(requestId, action, destination));
                        } else {
                            super.onBlackholedDuringSend(requestId, action, destination);
                        }
                    }
                };

                final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
                clusterApplier = new FakeClusterApplier(settings, clusterSettings);
                masterService = new AckedFakeThreadPoolMasterService("test",
                    runnable -> deterministicTaskQueue.scheduleNow(onNode(localNode, runnable)));
                transportService = mockTransport.createTransportService(
                    settings, deterministicTaskQueue.getThreadPool(runnable -> onNode(localNode, runnable)), NOOP_TRANSPORT_INTERCEPTOR,
                    a -> localNode, null, emptySet());
                coordinator = new Coordinator(settings, transportService, ESAllocationTestCase.createAllocationService(Settings.EMPTY),
                    masterService, this::getPersistedState, Cluster.this::provideUnicastHosts, clusterApplier, Randomness.get());
                masterService.setClusterStatePublisher(coordinator);

                transportService.start();
                transportService.acceptIncomingRequests();
                masterService.start();
                coordinator.start();
                coordinator.startInitialJoin();
            }

            private PersistedState getPersistedState() {
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

            void setClusterStateApplyResponse(ClusterStateApplyResponse clusterStateApplyResponse) {
                this.clusterStateApplyResponse = clusterStateApplyResponse;
            }

            AckCollector submitValue(final long value) {
                return submitUpdateTask("new value [" + value + "]", cs -> setValue(cs, value));
            }

            AckCollector submitUpdateTask(String source, UnaryOperator<ClusterState> clusterStateUpdate) {
                final AckCollector ackCollector = new AckCollector();
                onNode(localNode, () -> {
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
                            }

                            @Override
                            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                updateCommittedStates();
                                ClusterState state = committedStatesByVersion.get(newState.version());
                                assertNotNull("State not committed : " + newState.toString(), state);
                                assertEquals(value(state), value(newState));
                                logger.trace("successfully published: [{}]", newState);
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

            ClusterState getLastAppliedClusterState() {
                return clusterApplier.lastAppliedClusterState;
            }

            void applyInitialConfiguration() {
                onNode(localNode, () -> {
                    try {
                        coordinator.setInitialConfiguration(initialConfiguration);
                        logger.info("successfully set initial configuration to {}", initialConfiguration);
                    } catch (CoordinationStateRejectedException e) {
                        logger.info(new ParameterizedMessage("failed to set initial configuration to {}", initialConfiguration), e);
                    }
                }).run();
            }

            private class FakeClusterApplier implements ClusterApplier {

                final ClusterName clusterName;
                private final ClusterSettings clusterSettings;
                ClusterState lastAppliedClusterState;

                private FakeClusterApplier(Settings settings, ClusterSettings clusterSettings) {
                    clusterName = ClusterName.CLUSTER_NAME_SETTING.get(settings);
                    this.clusterSettings = clusterSettings;
                }

                @Override
                public void setInitialState(ClusterState initialState) {
                    assert lastAppliedClusterState == null;
                    assert initialState != null;
                    lastAppliedClusterState = initialState;
                }

                @Override
                public void onNewClusterState(String source, Supplier<ClusterState> clusterStateSupplier, ClusterApplyListener listener) {
                    switch (clusterStateApplyResponse) {
                        case SUCCEED:
                            deterministicTaskQueue.scheduleNow(onNode(localNode, new Runnable() {
                                @Override
                                public void run() {
                                    final ClusterState oldClusterState = clusterApplier.lastAppliedClusterState;
                                    final ClusterState newClusterState = clusterStateSupplier.get();
                                    assert oldClusterState.version() <= newClusterState.version() : "updating cluster state from version "
                                        + oldClusterState.version() + " to stale version " + newClusterState.version();
                                    clusterApplier.lastAppliedClusterState = newClusterState;
                                    final Settings incomingSettings = newClusterState.metaData().settings();
                                    clusterSettings.applySettings(incomingSettings); // TODO validation might throw exceptions here.
                                    listener.onSuccess(source);
                                }

                                @Override
                                public String toString() {
                                    return "apply cluster state from [" + source + "]";
                                }
                            }));
                            break;
                        case FAIL:
                            deterministicTaskQueue.scheduleNow(onNode(localNode, new Runnable() {
                                @Override
                                public void run() {
                                    listener.onFailure(source, new ElasticsearchException("cluster state application failed"));
                                }

                                @Override
                                public String toString() {
                                    return "fail to apply cluster state from [" + source + "]";
                                }
                            }));
                            break;
                        case HANG:
                            if (randomBoolean()) {
                                deterministicTaskQueue.scheduleNow(onNode(localNode, new Runnable() {
                                    @Override
                                    public void run() {
                                        final ClusterState oldClusterState = clusterApplier.lastAppliedClusterState;
                                        final ClusterState newClusterState = clusterStateSupplier.get();
                                        assert oldClusterState.version() <= newClusterState.version() :
                                            "updating cluster state from version "
                                                + oldClusterState.version() + " to stale version " + newClusterState.version();
                                        clusterApplier.lastAppliedClusterState = newClusterState;
                                    }

                                    @Override
                                    public String toString() {
                                        return "apply cluster state from [" + source + "] without ack";
                                    }
                                }));
                            }
                            break;
                    }
                }
            }
        }

        private List<TransportAddress> provideUnicastHosts(HostsResolver ignored) {
            return clusterNodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getAddress).collect(Collectors.toList());
        }
    }

    private static Runnable onNode(DiscoveryNode node, Runnable runnable) {
        final String nodeId = "{" + node.getId() + "}{" + node.getEphemeralId() + "}";
        return new Runnable() {
            @Override
            public void run() {
                try (CloseableThreadContext.Instance ignored = CloseableThreadContext.put("nodeId", nodeId)) {
                    runnable.run();
                }
            }

            @Override
            public String toString() {
                return nodeId + ": " + runnable.toString();
            }
        };
    }

    static class AckCollector implements AckListener {

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

        AckedFakeThreadPoolMasterService(String serviceName, Consumer<Runnable> onTaskAvailableToRun) {
            super(serviceName, onTaskAvailableToRun);
        }

        @Override
        protected AckListener wrapAckListener(AckListener ackListener) {
            final AckCollector ackCollector = nextAckCollector;
            nextAckCollector = new AckCollector();
            return new AckListener() {
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

}
