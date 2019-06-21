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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractDiffable;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.NodeConnectionsService;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher.AckListener;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.PersistedState;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.coordination.CoordinatorTests.Cluster.ClusterNode;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.History;
import org.elasticsearch.cluster.coordination.LinearizabilityChecker.SequentialSpec;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.discovery.SeedHostsProvider.HostsResolver;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.gateway.ClusterStateUpdaters;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.gateway.MetaStateService;
import org.elasticsearch.gateway.MockGatewayMetaState;
import org.elasticsearch.indices.cluster.FakeThreadPoolMasterService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.disruption.DisruptableMockTransport;
import org.elasticsearch.test.disruption.DisruptableMockTransport.ConnectionStatus;
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
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.BOOTSTRAP_PLACEHOLDER_PREFIX;
import static org.elasticsearch.cluster.coordination.CoordinationStateTests.clusterState;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.FOLLOWER;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.LEADER;
import static org.elasticsearch.cluster.coordination.Coordinator.PUBLISH_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.CoordinatorTests.Cluster.DEFAULT_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_BACK_OFF_TIME_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_DURATION_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ALL;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ID;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_WRITES;
import static org.elasticsearch.cluster.coordination.Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.transport.TransportService.NOOP_TRANSPORT_INTERCEPTOR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class CoordinatorTests extends ESTestCase {

    private final List<NodeEnvironment> nodeEnvironments = new ArrayList<>();

    private final AtomicInteger nextNodeIndex = new AtomicInteger();

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

    @Before
    public void resetPortCounterBeforeEachTest() {
        resetPortCounter();
    }

    // check that runRandomly leads to reproducible results
    public void testRepeatableTests() throws Exception {
        final Callable<Long> test = () -> {
            resetNodeIndexBeforeEachTest();
            final Cluster cluster = new Cluster(randomIntBetween(1, 5));
            cluster.runRandomly();
            final long afterRunRandomly = value(cluster.getAnyNode().getLastAppliedClusterState());
            cluster.stabilise();
            final long afterStabilisation = value(cluster.getAnyNode().getLastAppliedClusterState());
            return afterRunRandomly ^ afterStabilisation;
        };
        final long seed = randomLong();
        logger.info("First run with seed [{}]", seed);
        final long result1 = RandomizedContext.current().runWithPrivateRandomness(seed, test);
        logger.info("Second run with seed [{}]", seed);
        final long result2 = RandomizedContext.current().runWithPrivateRandomness(seed, test);
        assertEquals(result1, result2);
    }

    /**
     * This test was added to verify that state recovery is properly reset on a node after it has become master and successfully
     * recovered a state (see {@link GatewayService}). The situation which triggers this with a decent likelihood is as follows:
     * 3 master-eligible nodes (leader, follower1, follower2), the followers are shut down (leader remains), when followers come back
     * one of them becomes leader and publishes first state (with STATE_NOT_RECOVERED_BLOCK) to old leader, which accepts it.
     * Old leader is initiating an election at the same time, and wins election. It becomes leader again, but as it previously
     * successfully completed state recovery, is never reset to a state where state recovery can be retried.
     */
    public void testStateRecoveryResetAfterPreviousLeadership() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower2 = cluster.getAnyNodeExcept(leader, follower1);

        // restart follower1 and follower2
        for (ClusterNode clusterNode : Arrays.asList(follower1, follower2)) {
            clusterNode.close();
            cluster.clusterNodes.forEach(
                cn -> cluster.deterministicTaskQueue.scheduleNow(cn.onNode(
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
            cluster.clusterNodes.replaceAll(cn -> cn == clusterNode ? cn.restartedNode() : cn);
        }

        cluster.stabilise();
    }

    public void testCanUpdateClusterStateAfterStabilisation() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
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

    public void testDoesNotElectNonMasterNode() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5), false, Settings.EMPTY);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        assertTrue(leader.localNode.isMasterNode());
    }

    public void testNodesJoinAfterStableCluster() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final long currentTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
        cluster.addNodesAndStabilise(randomIntBetween(1, 2));

        final long newTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
        assertEquals(currentTerm, newTerm);
    }

    public void testExpandsConfigurationWhenGrowingFromOneNodeToThreeButDoesNotShrink() {
        final Cluster cluster = new Cluster(1);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();

        cluster.addNodesAndStabilise(2);

        {
            assertThat(leader.coordinator.getMode(), is(Mode.LEADER));
            final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
            assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
        }

        final ClusterNode disconnect1 = cluster.getAnyNode();
        logger.info("--> disconnecting {}", disconnect1);
        disconnect1.disconnect();
        cluster.stabilise();

        {
            final ClusterNode newLeader = cluster.getAnyLeader();
            final VotingConfiguration lastCommittedConfiguration = newLeader.getLastAppliedClusterState().getLastCommittedConfiguration();
            assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
        }
    }

    public void testExpandsConfigurationWhenGrowingFromThreeToFiveNodesAndShrinksBackToThreeOnFailure() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();

        logger.info("setting auto-shrink reconfiguration to true");
        leader.submitSetAutoShrinkVotingConfiguration(true);
        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
        assertTrue(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metaData().settings()));

        cluster.addNodesAndStabilise(2);

        {
            assertThat(leader.coordinator.getMode(), is(Mode.LEADER));
            final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
            assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
        }

        final ClusterNode disconnect1 = cluster.getAnyNode();
        final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);
        logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
        disconnect1.disconnect();
        disconnect2.disconnect();
        cluster.stabilise();

        {
            final ClusterNode newLeader = cluster.getAnyLeader();
            final VotingConfiguration lastCommittedConfiguration = newLeader.getLastAppliedClusterState().getLastCommittedConfiguration();
            assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
            assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
            assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
        }

        // we still tolerate the loss of one more node here

        final ClusterNode disconnect3 = cluster.getAnyNodeExcept(disconnect1, disconnect2);
        logger.info("--> disconnecting {}", disconnect3);
        disconnect3.disconnect();
        cluster.stabilise();

        {
            final ClusterNode newLeader = cluster.getAnyLeader();
            final VotingConfiguration lastCommittedConfiguration = newLeader.getLastAppliedClusterState().getLastCommittedConfiguration();
            assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
            assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
            assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
            assertTrue(lastCommittedConfiguration.getNodeIds().contains(disconnect3.getId()));
        }

        // however we do not tolerate the loss of yet another one

        final ClusterNode disconnect4 = cluster.getAnyNodeExcept(disconnect1, disconnect2, disconnect3);
        logger.info("--> disconnecting {}", disconnect4);
        disconnect4.disconnect();
        cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
        }

        // moreover we are still stuck even if two other nodes heal
        logger.info("--> healing {} and {}", disconnect1, disconnect2);
        disconnect1.heal();
        disconnect2.heal();
        cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
        }

        // we require another node to heal to recover
        final ClusterNode toHeal = randomBoolean() ? disconnect3 : disconnect4;
        logger.info("--> healing {}", toHeal);
        toHeal.heal();
        cluster.stabilise();
    }

    public void testCanShrinkFromFiveNodesToThree() {
        final Cluster cluster = new Cluster(5);
        cluster.runRandomly();
        cluster.stabilise();

        {
            final ClusterNode leader = cluster.getAnyLeader();
            logger.info("setting auto-shrink reconfiguration to false");
            leader.submitSetAutoShrinkVotingConfiguration(false);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertFalse(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metaData().settings()));
        }

        final ClusterNode disconnect1 = cluster.getAnyNode();
        final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);

        logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
        disconnect1.disconnect();
        disconnect2.disconnect();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();

        {
            final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
            assertThat(lastCommittedConfiguration + " should be all nodes", lastCommittedConfiguration.getNodeIds(),
                equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet())));
        }

        logger.info("setting auto-shrink reconfiguration to true");
        leader.submitSetAutoShrinkVotingConfiguration(true);
        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY * 2); // allow for a reconfiguration
        assertTrue(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metaData().settings()));

        {
            final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
            assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
            assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
            assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
        }
    }

    public void testDoesNotShrinkConfigurationBelowThreeNodes() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode disconnect1 = cluster.getAnyNode();

        logger.info("--> disconnecting {}", disconnect1);
        disconnect1.disconnect();
        cluster.stabilise();

        final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);
        logger.info("--> disconnecting {}", disconnect2);
        disconnect2.disconnect();
        cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
        }

        disconnect1.heal();
        cluster.stabilise(); // would not work if disconnect1 were removed from the configuration
    }

    public void testDoesNotShrinkConfigurationBelowFiveNodesIfAutoShrinkDisabled() {
        final Cluster cluster = new Cluster(5);
        cluster.runRandomly();
        cluster.stabilise();

        cluster.getAnyLeader().submitSetAutoShrinkVotingConfiguration(false);
        cluster.stabilise(DEFAULT_ELECTION_DELAY);

        final ClusterNode disconnect1 = cluster.getAnyNode();
        final ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);

        logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
        disconnect1.disconnect();
        disconnect2.disconnect();
        cluster.stabilise();

        final ClusterNode disconnect3 = cluster.getAnyNodeExcept(disconnect1, disconnect2);
        logger.info("--> disconnecting {}", disconnect3);
        disconnect3.disconnect();
        cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            assertThat(clusterNode.getId() + " should be a candidate", clusterNode.coordinator.getMode(), equalTo(Mode.CANDIDATE));
        }

        disconnect1.heal();
        cluster.stabilise(); // would not work if disconnect1 were removed from the configuration
    }

    public void testLeaderDisconnectionWithDisconnectEventDetectedQuickly() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode originalLeader = cluster.getAnyLeader();
        logger.info("--> disconnecting leader {}", originalLeader);
        originalLeader.disconnect();
        logger.info("--> followers get disconnect event for leader {} ", originalLeader);
        cluster.getAllNodesExcept(originalLeader).forEach(cn -> cn.onDisconnectEventFrom(originalLeader));
        // turn leader into candidate, which stabilisation asserts at the end
        cluster.getAllNodesExcept(originalLeader).forEach(cn -> originalLeader.onDisconnectEventFrom(cn));
        cluster.stabilise(DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
            // then wait for a new election
            + DEFAULT_ELECTION_DELAY
            // wait for the removal to be committed
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            // then wait for the followup reconfiguration
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
        assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
    }

    public void testLeaderDisconnectionWithoutDisconnectEventDetectedQuickly() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode originalLeader = cluster.getAnyLeader();
        logger.info("--> disconnecting leader {}", originalLeader);
        originalLeader.disconnect();

        cluster.stabilise(Math.max(
            // Each follower may have just sent a leader check, which receives no response
            defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)
                // then wait for the follower to check the leader
                + defaultMillis(LEADER_CHECK_INTERVAL_SETTING)
                // then wait for the exception response
                + DEFAULT_DELAY_VARIABILITY
                // then wait for a new election
                + DEFAULT_ELECTION_DELAY,

            // ALSO the leader may have just sent a follower check, which receives no response
            defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)
                // wait for the leader to check its followers
                + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                // then wait for the exception response
                + DEFAULT_DELAY_VARIABILITY)

            // FINALLY:

            // wait for the removal to be committed
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            // then wait for the followup reconfiguration
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

        assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
    }

    public void testUnresponsiveLeaderDetectedEventually() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
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
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // then wait for the followup reconfiguration
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
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower = cluster.getAnyNodeExcept(leader);
        logger.info("--> disconnecting follower {}", follower);
        follower.disconnect();
        logger.info("--> leader {} and follower {} get disconnect event", leader, follower);
        leader.onDisconnectEventFrom(follower);
        follower.onDisconnectEventFrom(leader); // to turn follower into candidate, which stabilisation asserts at the end
        cluster.stabilise(DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            // then wait for the followup reconfiguration
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
        assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
    }

    public void testFollowerDisconnectionWithoutDisconnectEventDetectedQuickly() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower = cluster.getAnyNodeExcept(leader);
        logger.info("--> disconnecting follower {}", follower);
        follower.disconnect();
        cluster.stabilise(Math.max(
            // the leader may have just sent a follower check, which receives no response
            defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)
                // wait for the leader to check the follower
                + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                // then wait for the exception response
                + DEFAULT_DELAY_VARIABILITY
                // then wait for the removal to be committed
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // then wait for the followup reconfiguration
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

            // ALSO the follower may have just sent a leader check, which receives no response
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
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // then wait for the followup reconfiguration
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
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        logger.info("--> blocking cluster state application on {}", follower0);
        follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.HANG);

        logger.info("--> publishing another value");
        AckCollector ackCollector = leader.submitValue(randomLong());
        cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing value");

        assertTrue("expected immediate ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
        assertFalse("expected no ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        cluster.stabilise(defaultMillis(PUBLISH_TIMEOUT_SETTING));
        assertTrue("expected eventual ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
        assertFalse("expected no ack from " + follower0, ackCollector.hasAcked(follower0));
    }

    public void testAckListenerReceivesNacksIfPublicationTimesOut() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
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
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.stabilise();
        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
        final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

        leader.blackhole();
        follower0.onDisconnectEventFrom(leader);
        follower1.onDisconnectEventFrom(leader);
        // let followers elect a leader among themselves before healing the leader and running the publication
        cluster.runFor(DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
            + DEFAULT_ELECTION_DELAY, "elect new leader");
        // cluster has two nodes in mode LEADER, in different terms ofc, and the one in the lower term won’t be able to publish anything
        leader.heal();
        AckCollector ackCollector = leader.submitValue(randomLong());
        cluster.stabilise(); // TODO: check if can find a better bound here
        assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
        assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
        assertTrue("expected nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
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
            assertFalse(nodeId + " has not received an initial configuration", clusterNode.coordinator.isInitialConfigurationSet());
            assertTrue(nodeId + " has an empty last-accepted configuration",
                clusterNode.coordinator.getLastAcceptedState().getLastAcceptedConfiguration().isEmpty());
            assertTrue(nodeId + " has an empty last-committed configuration",
                clusterNode.coordinator.getLastAcceptedState().getLastCommittedConfiguration().isEmpty());

            final Set<DiscoveryNode> foundPeers = new HashSet<>();
            clusterNode.coordinator.getFoundPeers().forEach(foundPeers::add);
            assertTrue(nodeId + " should not have discovered itself", foundPeers.add(clusterNode.getLocalNode()));
            assertThat(nodeId + " should have found all peers", foundPeers, hasSize(cluster.size()));
        }

        final ClusterNode bootstrapNode = cluster.getAnyBootstrappableNode();
        bootstrapNode.applyInitialConfiguration();
        assertTrue(bootstrapNode.getId() + " has been bootstrapped", bootstrapNode.coordinator.isInitialConfigurationSet());

        cluster.stabilise(
            // the first election should succeed, because only one node knows of the initial configuration and therefore can win a
            // pre-voting round and proceed to an election, so there cannot be any collisions
            defaultMillis(ELECTION_INITIAL_TIMEOUT_SETTING) // TODO this wait is unnecessary, we could trigger the election immediately
                // Allow two round-trip for pre-voting and voting
                + 4 * DEFAULT_DELAY_VARIABILITY
                // Then a commit of the new leader's first cluster state
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                // Then allow time for all the other nodes to join, each of which might cause a reconfiguration
                + (cluster.size() - 1) * 2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            // TODO Investigate whether 4 publications is sufficient due to batching? A bound linear in the number of nodes isn't great.
        );
    }

    public void testCannotSetInitialConfigurationTwice() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final Coordinator coordinator = cluster.getAnyNode().coordinator;
        assertFalse(coordinator.setInitialConfiguration(coordinator.getLastAcceptedState().getLastCommittedConfiguration()));
    }

    public void testCannotSetInitialConfigurationWithoutQuorum() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        final Coordinator coordinator = cluster.getAnyNode().coordinator;
        final VotingConfiguration unknownNodeConfiguration = new VotingConfiguration(
            Sets.newHashSet(coordinator.getLocalNode().getId(), "unknown-node"));
        final String exceptionMessage = expectThrows(CoordinationStateRejectedException.class,
            () -> coordinator.setInitialConfiguration(unknownNodeConfiguration)).getMessage();
        assertThat(exceptionMessage,
            startsWith("not enough nodes discovered to form a quorum in the initial configuration [knownNodes=["));
        assertThat(exceptionMessage, containsString("unknown-node"));
        assertThat(exceptionMessage, containsString(coordinator.getLocalNode().toString()));

        // This is VERY BAD: setting a _different_ initial configuration. Yet it works if the first attempt will never be a quorum.
        assertTrue(coordinator.setInitialConfiguration(new VotingConfiguration(Collections.singleton(coordinator.getLocalNode().getId()))));
        cluster.stabilise();
    }

    public void testCannotSetInitialConfigurationWithoutLocalNode() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        final Coordinator coordinator = cluster.getAnyNode().coordinator;
        final VotingConfiguration unknownNodeConfiguration = new VotingConfiguration(Sets.newHashSet("unknown-node"));
        final String exceptionMessage = expectThrows(CoordinationStateRejectedException.class,
            () -> coordinator.setInitialConfiguration(unknownNodeConfiguration)).getMessage();
        assertThat(exceptionMessage,
            equalTo("local node is not part of initial configuration"));
    }

    public void testDiffBasedPublishing() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final long finalValue = randomLong();
        final Map<ClusterNode, PublishClusterStateStats> prePublishStats = cluster.clusterNodes.stream().collect(
            Collectors.toMap(Function.identity(), cn -> cn.coordinator.stats().getPublishStats()));
        logger.info("--> submitting value [{}] to [{}]", finalValue, leader);
        leader.submitValue(finalValue);
        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
        final Map<ClusterNode, PublishClusterStateStats> postPublishStats = cluster.clusterNodes.stream().collect(
            Collectors.toMap(Function.identity(), cn -> cn.coordinator.stats().getPublishStats()));

        for (ClusterNode cn : cluster.clusterNodes) {
            assertThat(value(cn.getLastAppliedClusterState()), is(finalValue));
            if (cn == leader) {
                // leader does not update publish stats as it's not using the serialized state
                assertEquals(cn.toString(), prePublishStats.get(cn).getFullClusterStateReceivedCount(),
                    postPublishStats.get(cn).getFullClusterStateReceivedCount());
                assertEquals(cn.toString(), prePublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount(),
                    postPublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount());
                assertEquals(cn.toString(), prePublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount(),
                    postPublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount());
            } else {
                // followers receive a diff
                assertEquals(cn.toString(), prePublishStats.get(cn).getFullClusterStateReceivedCount(),
                    postPublishStats.get(cn).getFullClusterStateReceivedCount());
                assertEquals(cn.toString(), prePublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount() + 1,
                    postPublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount());
                assertEquals(cn.toString(), prePublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount(),
                    postPublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount());
            }
        }
    }

    public void testJoiningNodeReceivesFullState() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        cluster.runRandomly();
        cluster.stabilise();

        cluster.addNodesAndStabilise(1);
        final ClusterNode newNode = cluster.clusterNodes.get(cluster.clusterNodes.size() - 1);
        final PublishClusterStateStats newNodePublishStats = newNode.coordinator.stats().getPublishStats();
        // initial cluster state send when joining
        assertEquals(1L, newNodePublishStats.getFullClusterStateReceivedCount());
        // possible follow-up reconfiguration was published as a diff
        assertEquals(cluster.size() % 2, newNodePublishStats.getCompatibleClusterStateDiffReceivedCount());
        assertEquals(0L, newNodePublishStats.getIncompatibleClusterStateDiffReceivedCount());
    }

    public void testIncompatibleDiffResendsFullState() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode follower = cluster.getAnyNodeExcept(leader);
        logger.info("--> blackholing {}", follower);
        follower.blackhole();
        final PublishClusterStateStats prePublishStats = follower.coordinator.stats().getPublishStats();
        logger.info("--> submitting first value to {}", leader);
        leader.submitValue(randomLong());
        cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "publish first state");
        logger.info("--> healing {}", follower);
        follower.heal();
        logger.info("--> submitting second value to {}", leader);
        leader.submitValue(randomLong());
        cluster.stabilise();
        final PublishClusterStateStats postPublishStats = follower.coordinator.stats().getPublishStats();
        assertEquals(prePublishStats.getFullClusterStateReceivedCount() + 1,
            postPublishStats.getFullClusterStateReceivedCount());
        assertEquals(prePublishStats.getCompatibleClusterStateDiffReceivedCount(),
            postPublishStats.getCompatibleClusterStateDiffReceivedCount());
        assertEquals(prePublishStats.getIncompatibleClusterStateDiffReceivedCount() + 1,
            postPublishStats.getIncompatibleClusterStateDiffReceivedCount());
    }

    /**
     * Simulates a situation where a follower becomes disconnected from the leader, but only for such a short time where
     * it becomes candidate and puts up a NO_MASTER_BLOCK, but then receives a follower check from the leader. If the leader
     * does not notice the node disconnecting, it is important for the node not to be turned back into a follower but try
     * and join the leader again.
     */
    public void testStayCandidateAfterReceivingFollowerCheckFromKnownMaster() {
        final Cluster cluster = new Cluster(2, false, Settings.EMPTY);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode nonLeader = cluster.getAnyNodeExcept(leader);
        nonLeader.onNode(() -> {
            logger.debug("forcing {} to become candidate", nonLeader.getId());
            synchronized (nonLeader.coordinator.mutex) {
                nonLeader.coordinator.becomeCandidate("forced");
            }
            logger.debug("simulate follower check coming through from {} to {}", leader.getId(), nonLeader.getId());
            expectThrows(CoordinationStateRejectedException.class, () -> nonLeader.coordinator.onFollowerCheckRequest(
                new FollowersChecker.FollowerCheckRequest(leader.coordinator.getCurrentTerm(), leader.getLocalNode())));
            assertThat(nonLeader.coordinator.getMode(), equalTo(CANDIDATE));
        }).run();
        cluster.stabilise();
    }

    public void testAppliesNoMasterBlockWritesByDefault() {
        testAppliesNoMasterBlock(null, NO_MASTER_BLOCK_WRITES);
    }

    public void testAppliesNoMasterBlockWritesIfConfigured() {
        testAppliesNoMasterBlock("write", NO_MASTER_BLOCK_WRITES);
    }

    public void testAppliesNoMasterBlockAllIfConfigured() {
        testAppliesNoMasterBlock("all", NO_MASTER_BLOCK_ALL);
    }

    private void testAppliesNoMasterBlock(String noMasterBlockSetting, ClusterBlock expectedBlock) {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        leader.submitUpdateTask("update NO_MASTER_BLOCK_SETTING", cs -> {
            final Builder settingsBuilder = Settings.builder().put(cs.metaData().persistentSettings());
            settingsBuilder.put(NO_MASTER_BLOCK_SETTING.getKey(), noMasterBlockSetting);
            return ClusterState.builder(cs).metaData(MetaData.builder(cs.metaData()).persistentSettings(settingsBuilder.build())).build();
        }, (source, e) -> {});
        cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing setting update");

        leader.disconnect();
        cluster.runFor(defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING) + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "detecting disconnection");

        assertThat(leader.getLastAppliedClusterState().blocks().global(), hasItem(expectedBlock));

        // TODO reboot the leader and verify that the same block is applied when it restarts
    }

    public void testNodeCannotJoinIfJoinValidationFailsOnMaster() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 3));
        cluster.runRandomly();
        cluster.stabilise();

        // check that if node join validation fails on master, the nodes can't join
        List<ClusterNode> addedNodes = cluster.addNodes(randomIntBetween(1, 2));
        final Set<DiscoveryNode> validatedNodes = new HashSet<>();
        cluster.getAnyLeader().extraJoinValidators.add((discoveryNode, clusterState) -> {
            validatedNodes.add(discoveryNode);
            throw new IllegalArgumentException("join validation failed");
        });
        final long previousClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
        cluster.runFor(10000, "failing join validation");
        assertEquals(validatedNodes, addedNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet()));
        assertTrue(addedNodes.stream().allMatch(ClusterNode::isCandidate));
        final long newClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
        assertEquals(previousClusterStateVersion, newClusterStateVersion);

        cluster.getAnyLeader().extraJoinValidators.clear();
        cluster.stabilise();
    }

    public void testNodeCannotJoinIfJoinValidationFailsOnJoiningNode() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 3));
        cluster.runRandomly();
        cluster.stabilise();

        // check that if node join validation fails on joining node, the nodes can't join
        List<ClusterNode> addedNodes = cluster.addNodes(randomIntBetween(1, 2));
        final Set<DiscoveryNode> validatedNodes = new HashSet<>();
        addedNodes.stream().forEach(cn -> cn.extraJoinValidators.add((discoveryNode, clusterState) -> {
            validatedNodes.add(discoveryNode);
            throw new IllegalArgumentException("join validation failed");
        }));
        final long previousClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
        cluster.runFor(10000, "failing join validation");
        assertEquals(validatedNodes, addedNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet()));
        assertTrue(addedNodes.stream().allMatch(ClusterNode::isCandidate));
        final long newClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
        assertEquals(previousClusterStateVersion, newClusterStateVersion);

        addedNodes.stream().forEach(cn -> cn.extraJoinValidators.clear());
        cluster.stabilise();
    }

    public void testClusterCannotFormWithFailingJoinValidation() {
        final Cluster cluster = new Cluster(randomIntBetween(1, 5));
        // fail join validation on a majority of nodes in the initial configuration
        randomValueOtherThanMany(nodes ->
            cluster.initialConfiguration.hasQuorum(
                nodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getId).collect(Collectors.toSet())) == false,
            () -> randomSubsetOf(cluster.clusterNodes))
        .forEach(cn -> cn.extraJoinValidators.add((discoveryNode, clusterState) -> {
            throw new IllegalArgumentException("join validation failed");
        }));
        cluster.bootstrapIfNecessary();
        cluster.runFor(10000, "failing join validation");
        assertTrue(cluster.clusterNodes.stream().allMatch(cn -> cn.getLastAppliedClusterState().version() == 0));
    }

    public void testCannotJoinClusterWithDifferentUUID() throws IllegalAccessException {
        final Cluster cluster1 = new Cluster(randomIntBetween(1, 3));
        cluster1.runRandomly();
        cluster1.stabilise();

        final Cluster cluster2 = new Cluster(3);
        cluster2.runRandomly();
        cluster2.stabilise();

        final ClusterNode shiftedNode = randomFrom(cluster2.clusterNodes).restartedNode();
        final ClusterNode newNode = cluster1.new ClusterNode(nextNodeIndex.getAndIncrement(),
            shiftedNode.getLocalNode(), n -> shiftedNode.persistedState, shiftedNode.nodeSettings);
        cluster1.clusterNodes.add(newNode);

        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1",
                JoinHelper.class.getCanonicalName(),
                Level.INFO,
                "*failed to join*"));
        Logger joinLogger = LogManager.getLogger(JoinHelper.class);
        Loggers.addAppender(joinLogger, mockAppender);
        cluster1.runFor(DEFAULT_STABILISATION_TIME, "failing join validation");
        try {
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(joinLogger, mockAppender);
            mockAppender.stop();
        }
        assertEquals(0, newNode.getLastAppliedClusterState().version());

        final ClusterNode detachedNode = newNode.restartedNode(
            metaData -> DetachClusterCommand.updateMetaData(metaData),
            term -> DetachClusterCommand.updateCurrentTerm(), newNode.nodeSettings);
        cluster1.clusterNodes.replaceAll(cn -> cn == newNode ? detachedNode : cn);
        cluster1.stabilise();
    }

    public void testDiscoveryUsesNodesFromLastClusterState() {
        final Cluster cluster = new Cluster(randomIntBetween(3, 5));
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode partitionedNode = cluster.getAnyNode();
        if (randomBoolean()) {
            logger.info("--> blackholing {}", partitionedNode);
            partitionedNode.blackhole();
        } else {
            logger.info("--> disconnecting {}", partitionedNode);
            partitionedNode.disconnect();
        }
        cluster.setEmptySeedHostsList();
        cluster.stabilise();

        partitionedNode.heal();
        cluster.runRandomly(false);
        cluster.stabilise();
    }

    public void testFollowerRemovedIfUnableToSendRequestsToMaster() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader = cluster.getAnyLeader();
        final ClusterNode otherNode = cluster.getAnyNodeExcept(leader);

        cluster.blackholeConnectionsFrom(otherNode, leader);

        cluster.runFor(
            (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
                * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
                + (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + DEFAULT_DELAY_VARIABILITY)
                * defaultInt(LEADER_CHECK_RETRY_COUNT_SETTING)
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,
            "awaiting removal of asymmetrically-partitioned node");

        assertThat(leader.getLastAppliedClusterState().nodes().toString(),
            leader.getLastAppliedClusterState().nodes().getSize(), equalTo(2));

        cluster.clearBlackholedConnections();

        cluster.stabilise(
            // time for the disconnected node to find the master again
            defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2
                // time for joining
                + 4 * DEFAULT_DELAY_VARIABILITY
                // Then a commit of the updated cluster state
                + DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
    }

    public void testSingleNodeDiscoveryWithoutQuorum() {
        final Cluster cluster = new Cluster(3);
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode clusterNode = cluster.getAnyNode();
        logger.debug("rebooting [{}]", clusterNode.getId());
        clusterNode.close();
        cluster.clusterNodes.forEach(
            cn -> cluster.deterministicTaskQueue.scheduleNow(cn.onNode(
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
        IllegalStateException ise = expectThrows(IllegalStateException.class,
            () -> cluster.clusterNodes.replaceAll(cn -> cn == clusterNode ?
                cn.restartedNode(Function.identity(), Function.identity(), Settings.builder()
                    .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE).build()) :
                cn));
        assertThat(ise.getMessage(), containsString("cannot start with [discovery.type] set to [single-node] when local node"));
        assertThat(ise.getMessage(), containsString("does not have quorum in voting configuration"));
    }

    public void testSingleNodeDiscoveryWithQuorum() {
        final Cluster cluster = new Cluster(1, randomBoolean(), Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(),
            DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE).build());
        cluster.runRandomly();
        cluster.stabilise();
    }

    private static class BrokenCustom extends AbstractDiffable<ClusterState.Custom> implements ClusterState.Custom {

        static final String EXCEPTION_MESSAGE = "simulated";

        @Override
        public String getWriteableName() {
            return "broken";
        }

        @Override
        public Version getMinimalSupportedVersion() {
            return Version.V_EMPTY;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new ElasticsearchException(EXCEPTION_MESSAGE);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder;
        }
    }

    public void testClusterRecoversAfterExceptionDuringSerialization() {
        final Cluster cluster = new Cluster(randomIntBetween(2, 5)); // 1-node cluster doesn't do any serialization
        cluster.runRandomly();
        cluster.stabilise();

        final ClusterNode leader1 = cluster.getAnyLeader();

        logger.info("--> submitting broken task to [{}]", leader1);

        final AtomicBoolean failed = new AtomicBoolean();
        leader1.submitUpdateTask("broken-task",
            cs -> ClusterState.builder(cs).putCustom("broken", new BrokenCustom()).build(),
            (source, e) -> {
                assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
                assertThat(e.getCause().getMessage(), equalTo(BrokenCustom.EXCEPTION_MESSAGE));
                failed.set(true);
            });
        cluster.runFor(DEFAULT_DELAY_VARIABILITY + 1, "processing broken task");
        assertTrue(failed.get());

        cluster.stabilise();

        final ClusterNode leader2 = cluster.getAnyLeader();
        long finalValue = randomLong();

        logger.info("--> submitting value [{}] to [{}]", finalValue, leader2);
        leader2.submitValue(finalValue);
        cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            final String nodeId = clusterNode.getId();
            final ClusterState appliedState = clusterNode.getLastAppliedClusterState();
            assertThat(nodeId + " has the applied value", value(appliedState), is(finalValue));
        }
    }

    public void testLogsWarningPeriodicallyIfClusterNotFormed() {
        final long warningDelayMillis;
        final Settings settings;
        if (randomBoolean()) {
            settings = Settings.EMPTY;
            warningDelayMillis = ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(settings).millis();
        } else {
            warningDelayMillis = randomLongBetween(1, 100000);
            settings = Settings.builder()
                .put(ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.getKey(), warningDelayMillis + "ms")
                .build();
        }
        logger.info("--> emitting warnings every [{}ms]", warningDelayMillis);

        final Cluster cluster = new Cluster(3, true, settings);
        cluster.runRandomly();
        cluster.stabilise();

        logger.info("--> disconnecting all nodes");

        for (final ClusterNode clusterNode : cluster.clusterNodes) {
            clusterNode.disconnect();
        }

        cluster.runFor(defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING),
            "waiting for leader failure");

        for (int i = scaledRandomIntBetween(1, 10); i >= 0; i--) {
            final MockLogAppender mockLogAppender;
            try {
                mockLogAppender = new MockLogAppender();
            } catch (IllegalAccessException e) {
                throw new AssertionError(e);
            }

            try {
                Loggers.addAppender(LogManager.getLogger(ClusterFormationFailureHelper.class), mockLogAppender);
                mockLogAppender.start();
                mockLogAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
                    final Set<DiscoveryNode> nodesLogged = new HashSet<>();

                    @Override
                    public void match(LogEvent event) {
                        final String message = event.getMessage().getFormattedMessage();
                        assertThat(message,
                            startsWith("master not discovered or elected yet, an election requires at least 2 nodes with ids from ["));

                        final List<ClusterNode> matchingNodes = cluster.clusterNodes.stream()
                            .filter(n -> event.getContextData().<String>getValue(NODE_ID_LOG_CONTEXT_KEY)
                                .equals(getNodeIdForLogContext(n.getLocalNode()))).collect(Collectors.toList());
                        assertThat(matchingNodes, hasSize(1));

                        assertTrue(Regex.simpleMatch("*have discovered *" + matchingNodes.get(0).toString() + "*discovery will continue*",
                            message));

                        nodesLogged.add(matchingNodes.get(0).getLocalNode());
                    }

                    @Override
                    public void assertMatched() {
                        assertThat(nodesLogged + " vs " + cluster.clusterNodes, nodesLogged,
                            equalTo(cluster.clusterNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet())));
                    }
                });
                cluster.runFor(warningDelayMillis, "waiting for warning to be emitted");
                mockLogAppender.assertAllExpectationsMatched();
            } finally {
                mockLogAppender.stop();
                Loggers.removeAppender(LogManager.getLogger(ClusterFormationFailureHelper.class), mockLogAppender);
            }
        }
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
        + defaultMillis(ELECTION_DURATION_SETTING) * ELECTION_RETRIES
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
            // perhaps there is an election collision requiring another publication (which times out) and a term bump
            + defaultMillis(PUBLISH_TIMEOUT_SETTING) + DEFAULT_ELECTION_DELAY
            // then wait for the new leader to notice that the old leader is unresponsive
            + (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING))
            * defaultInt(FOLLOWER_CHECK_RETRY_COUNT_SETTING)
            // then wait for the new leader to commit a state without the old leader
            + DEFAULT_CLUSTER_STATE_UPDATE_DELAY;

    class Cluster {

        static final long EXTREME_DELAY_VARIABILITY = 10000L;
        static final long DEFAULT_DELAY_VARIABILITY = 100L;

        final List<ClusterNode> clusterNodes;
        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue(
            // TODO does ThreadPool need a node name any more?
            Settings.builder().put(NODE_NAME_SETTING.getKey(), "deterministic-task-queue").build(), random());
        private boolean disruptStorage;

        private final VotingConfiguration initialConfiguration;

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
                    assertTrue(nodeId + " has voted for " + leaderId, leader.coordinator.hasJoinVoteFrom(clusterNode.getLocalNode()));
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
            assertTrue("history not linearizable: " + history, linearizabilityChecker.isLinearizable(spec, history, i -> null));
            logger.info("linearizability check completed");
        }

        void bootstrapIfNecessary() {
            if (clusterNodes.stream().allMatch(ClusterNode::isNotUsefullyBootstrapped)) {
                assertThat("setting initial configuration may fail with disconnected nodes", disconnectedNodes, empty());
                assertThat("setting initial configuration may fail with blackholed nodes", blackholedNodes, empty());
                runFor(defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2, "discovery prior to setting initial configuration");
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

        class MockPersistedState implements PersistedState {
            private final PersistedState delegate;
            private final NodeEnvironment nodeEnvironment;

            MockPersistedState(DiscoveryNode localNode) {
                try {
                    if (rarely()) {
                        nodeEnvironment = newNodeEnvironment();
                        nodeEnvironments.add(nodeEnvironment);
                        delegate = new MockGatewayMetaState(Settings.EMPTY, nodeEnvironment, xContentRegistry(), localNode)
                                .getPersistedState(Settings.EMPTY, null);
                    } else {
                        nodeEnvironment = null;
                        delegate = new InMemoryPersistedState(0L,
                            ClusterStateUpdaters.addStateNotRecoveredBlock(
                                clusterState(0L, 0L, localNode, VotingConfiguration.EMPTY_CONFIG, VotingConfiguration.EMPTY_CONFIG, 0L)));
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
                        final MetaStateService metaStateService = new MetaStateService(nodeEnvironment, xContentRegistry());
                        final MetaData updatedMetaData = adaptGlobalMetaData.apply(oldState.getLastAcceptedState().metaData());
                        if (updatedMetaData != oldState.getLastAcceptedState().metaData()) {
                            metaStateService.writeGlobalStateAndUpdateManifest("update global state", updatedMetaData);
                        }
                        final long updatedTerm = adaptCurrentTerm.apply(oldState.getCurrentTerm());
                        if (updatedTerm != oldState.getCurrentTerm()) {
                            final Manifest manifest = metaStateService.loadManifestOrEmpty();
                            metaStateService.writeManifestAndCleanup("update term",
                                new Manifest(updatedTerm, manifest.getClusterStateVersion(), manifest.getGlobalGeneration(),
                                    manifest.getIndexGenerations()));
                        }
                        delegate = new MockGatewayMetaState(Settings.EMPTY, nodeEnvironment, xContentRegistry(), newLocalNode)
                                .getPersistedState(Settings.EMPTY, null);
                    } else {
                        nodeEnvironment = null;
                        BytesStreamOutput outStream = new BytesStreamOutput();
                        outStream.setVersion(Version.CURRENT);
                        final MetaData updatedMetaData = adaptGlobalMetaData.apply(oldState.getLastAcceptedState().metaData());
                        final ClusterState clusterState;
                        if (updatedMetaData != oldState.getLastAcceptedState().metaData()) {
                            clusterState = ClusterState.builder(oldState.getLastAcceptedState()).metaData(updatedMetaData).build();
                        } else {
                            clusterState = oldState.getLastAcceptedState();
                        }
                        clusterState.writeTo(outStream);
                        StreamInput inStream = new NamedWriteableAwareStreamInput(outStream.bytes().streamInput(),
                                new NamedWriteableRegistry(ClusterModule.getNamedWriteables()));
                        // adapt cluster state to new localNode instance and add blocks
                        delegate = new InMemoryPersistedState(adaptCurrentTerm.apply(oldState.getCurrentTerm()),
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
        }

        class ClusterNode {
            private final Logger logger = LogManager.getLogger(ClusterNode.class);

            private final int nodeIndex;
            private Coordinator coordinator;
            private final DiscoveryNode localNode;
            private final MockPersistedState persistedState;
            private final Settings nodeSettings;
            private AckedFakeThreadPoolMasterService masterService;
            private DisruptableClusterApplierService clusterApplierService;
            private ClusterService clusterService;
            private TransportService transportService;
            private DisruptableMockTransport mockTransport;
            private List<BiConsumer<DiscoveryNode, ClusterState>> extraJoinValidators = new ArrayList<>();

            ClusterNode(int nodeIndex, boolean masterEligible, Settings nodeSettings) {
                this(nodeIndex, createDiscoveryNode(nodeIndex, masterEligible), defaultPersistedStateSupplier, nodeSettings);
            }

            ClusterNode(int nodeIndex, DiscoveryNode localNode, Function<DiscoveryNode, MockPersistedState> persistedStateSupplier,
                        Settings nodeSettings) {
                this.nodeIndex = nodeIndex;
                this.localNode = localNode;
                this.nodeSettings = nodeSettings;
                persistedState = persistedStateSupplier.apply(localNode);
                onNodeLog(localNode, this::setUp).run();
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
                    settings, deterministicTaskQueue.getThreadPool(this::onNode), NOOP_TRANSPORT_INTERCEPTOR,
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
                    Cluster.this::provideSeedHosts, clusterApplierService, onJoinValidators, Randomness.get(), s -> {});
                masterService.setClusterStatePublisher(coordinator);
                final GatewayService gatewayService = new GatewayService(settings, allocationService, clusterService,
                    deterministicTaskQueue.getThreadPool(this::onNode), null, coordinator);

                logger.trace("starting up [{}]", localNode);
                transportService.start();
                transportService.acceptIncomingRequests();
                coordinator.start();
                gatewayService.start();
                clusterService.start();
                coordinator.startInitialJoin();
            }

            void close() {
                onNode(() -> {
                    logger.trace("taking down [{}]", localNode);
                    coordinator.stop();
                    clusterService.stop();
                    //transportService.stop(); // does blocking stuff :/
                    clusterService.close();
                    coordinator.close();
                    //transportService.close(); // does blocking stuff :/
                });
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
                    localNode.isMasterNode() ? DiscoveryNodeRole.BUILT_IN_ROLES : emptySet(), Version.CURRENT);
                return new ClusterNode(nodeIndex, newLocalNode,
                    node -> new MockPersistedState(newLocalNode, persistedState, adaptGlobalMetaData, adaptCurrentTerm), nodeSettings);
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
        }

        private List<TransportAddress> provideSeedHosts(HostsResolver ignored) {
            return seedHostsList != null ? seedHostsList
                : clusterNodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getAddress).collect(Collectors.toList());
        }
    }

    private static final String NODE_ID_LOG_CONTEXT_KEY = "nodeId";

    private static String getNodeIdForLogContext(DiscoveryNode node) {
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

        AckedFakeThreadPoolMasterService(String nodeName, String serviceName, Consumer<Runnable> onTaskAvailableToRun) {
            super(nodeName, serviceName, onTaskAvailableToRun);
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

    static class DisruptableClusterApplierService extends ClusterApplierService {
        private final String nodeName;
        private final DeterministicTaskQueue deterministicTaskQueue;
        ClusterStateApplyResponse clusterStateApplyResponse = ClusterStateApplyResponse.SUCCEED;

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
    }

    private static DiscoveryNode createDiscoveryNode(int nodeIndex, boolean masterEligible) {
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
