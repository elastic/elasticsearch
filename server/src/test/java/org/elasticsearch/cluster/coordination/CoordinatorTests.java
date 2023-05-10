/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SimpleDiffable;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.ClusterNode;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.Coordinator.Mode;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.service.ClusterStateUpdateStats;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.Settings.Builder;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.monitor.NodeHealthService;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.DEFAULT_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;
import static org.elasticsearch.cluster.coordination.Coordinator.PUBLISH_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.FollowersChecker.FOLLOWER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_INTERVAL_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_RETRY_COUNT_SETTING;
import static org.elasticsearch.cluster.coordination.LeaderChecker.LEADER_CHECK_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_ALL;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_METADATA_WRITES;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_SETTING;
import static org.elasticsearch.cluster.coordination.NoMasterBlockService.NO_MASTER_BLOCK_WRITES;
import static org.elasticsearch.cluster.coordination.Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class CoordinatorTests extends AbstractCoordinatorTestCase {

    public void testCanUpdateClusterStateAfterStabilisation() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
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
    }

    public void testDoesNotElectNonMasterNode() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5), false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            assertTrue(leader.getLocalNode().isMasterNode());
        }
    }

    public void testUnhealthyNodesGetsRemoved() {
        testUnhealthyNodesGetsRemoved(true);
    }

    protected void testUnhealthyNodesGetsRemoved(boolean verifyVotingConfiguration) {
        AtomicReference<StatusInfo> healthStatusInfo = new AtomicReference<>(new StatusInfo(HEALTHY, "healthy-info"));
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            logger.info("--> adding two new healthy nodes");
            ClusterNode newNode1 = cluster.new ClusterNode(
                nextNodeIndex.getAndIncrement(), true, leader.nodeSettings, healthStatusInfo::get
            );
            ClusterNode newNode2 = cluster.new ClusterNode(
                nextNodeIndex.getAndIncrement(), true, leader.nodeSettings, healthStatusInfo::get
            );
            cluster.clusterNodes.add(newNode1);
            cluster.clusterNodes.add(newNode2);
            cluster.stabilise(
                // The first pinging discovers the master
                defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING)
                    // One message delay to send a join
                    + DEFAULT_DELAY_VARIABILITY
                    // Commit a new cluster state with the new node(s). Might be split into multiple commits, and each might need a
                    // followup reconfiguration
                    + 2 * 2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );

            if (verifyVotingConfiguration) {
                assertThat(leader.coordinator.getMode(), is(Mode.LEADER));
                final VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState().getLastCommittedConfiguration();
                assertThat(
                    lastCommittedConfiguration + " should be all nodes",
                    lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(ClusterNode::getId).collect(Collectors.toSet()))
                );
            }

            logger.info("setting auto-shrink reconfiguration to true");
            leader.submitSetAutoShrinkVotingConfiguration(true);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertTrue(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metadata().settings()));

            logger.info("--> changing health of newly added nodes to unhealthy");
            healthStatusInfo.getAndSet(new StatusInfo(UNHEALTHY, "unhealthy-info"));

            cluster.stabilise(
                Math.max(
                    // Each follower may have just sent a leader check, which receives no response
                    defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)
                        // then wait for the follower to check the leader
                        + defaultMillis(LEADER_CHECK_INTERVAL_SETTING)
                        // then wait for the exception response
                        + DEFAULT_DELAY_VARIABILITY,

                    // ALSO the leader may have just sent a follower check, which receives no response
                    defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)
                        // wait for the leader to check its followers
                        + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING)
                        // then wait for the exception response
                        + DEFAULT_DELAY_VARIABILITY
                )

                    // FINALLY:

                    // wait for the removal to be committed
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );

            if (verifyVotingConfiguration) {
                final ClusterNode newLeader = cluster.getAnyLeader();
                final VotingConfiguration lastCommittedConfiguration = newLeader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(newNode1.getId()));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(newNode2.getId()));
            }
        }
    }

    public void testNodesJoinAfterStableCluster() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final long currentTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
            cluster.addNodesAndStabilise(randomIntBetween(1, 2));

            final long newTerm = cluster.getAnyLeader().coordinator.getCurrentTerm();
            assertEquals(currentTerm, newTerm);
        }
    }

    public void testLeaderDisconnectionWithDisconnectEventDetectedQuickly() {
        testLeaderDisconnectionWithDisconnectEventDetectedQuickly(Settings.EMPTY, TimeValue.ZERO);
    }

    protected void testLeaderDisconnectionWithDisconnectEventDetectedQuickly(Settings settings, TimeValue extraStabilisationTime) {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5), true, settings)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode originalLeader = cluster.getAnyLeader();
            logger.info("--> disconnecting leader {}", originalLeader);
            originalLeader.disconnect();
            logger.info("--> followers get disconnect event for leader {} ", originalLeader);
            cluster.getAllNodesExcept(originalLeader).forEach(cn -> cn.onDisconnectEventFrom(originalLeader));
            // turn leader into candidate, which stabilisation asserts at the end
            cluster.getAllNodesExcept(originalLeader).forEach(originalLeader::onDisconnectEventFrom);
            cluster.stabilise(
                DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
                    // then wait for a new election
                    + DEFAULT_ELECTION_DELAY
                    // wait for the removal to be committed
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // plus possibly some extra time according to the config
                    + extraStabilisationTime.millis()
            );
            assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
        }
    }

    public void testLeaderDisconnectionWithoutDisconnectEventDetectedQuickly() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode originalLeader = cluster.getAnyLeader();
            logger.info("--> disconnecting leader {}", originalLeader);
            originalLeader.disconnect();

            cluster.stabilise(
                Math.max(
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
                        + DEFAULT_DELAY_VARIABILITY
                )

                    // FINALLY:

                    // wait for the removal to be committed
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );

            assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
        }
    }

    public void testUnresponsiveLeaderDetectedEventually() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode originalLeader = cluster.getAnyLeader();
            logger.info("--> blackholing leader {}", originalLeader);
            originalLeader.blackhole();

            cluster.stabilise(
                Math.max(
                    // first wait for all the followers to notice the leader has gone
                    (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)) * defaultInt(
                        LEADER_CHECK_RETRY_COUNT_SETTING
                    )
                        // then wait for a follower to be promoted to leader
                        + DEFAULT_ELECTION_DELAY
                        // and the first publication times out because of the unresponsive node
                        + defaultMillis(PUBLISH_TIMEOUT_SETTING)
                        // there might be a term bump causing another election
                        + DEFAULT_ELECTION_DELAY
                        // in clusters with 5 nodes the chances of concurrent elections
                        // increase, meaning that it takes longer to get a leader elected
                        // so we should take into account those cases to ensure that the
                        // cluster stabilises over time. See #63918 for a really messy scenario.
                        + DEFAULT_ELECTION_DELAY
                        // additionally take into account that publications might take longer
                        // until the new leader detects that the old leader is unresponsive
                        + defaultMillis(PUBLISH_TIMEOUT_SETTING)

                        // then wait for both of:
                        + Math.max(
                            // 1. the term bumping publication to time out
                            defaultMillis(PUBLISH_TIMEOUT_SETTING),
                            // 2. the new leader to notice that the old leader is unresponsive
                            (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)) * defaultInt(
                                FOLLOWER_CHECK_RETRY_COUNT_SETTING
                            )
                        )

                        // then wait for the new leader to commit a state without the old leader
                        + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                        // then wait for the followup reconfiguration
                        + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

                    // ALSO wait for the leader to notice that its followers are unresponsive
                    (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)) * defaultInt(
                        FOLLOWER_CHECK_RETRY_COUNT_SETTING
                    )
                        // then wait for the leader to try and commit a state removing them, causing it to stand down
                        + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                )
            );

            assertThat(cluster.getAnyLeader().getId(), not(equalTo(originalLeader.getId())));
        }
    }

    public void testUnhealthyLeaderIsReplaced() {
        final AtomicReference<StatusInfo> nodeHealthServiceStatus = new AtomicReference<>(new StatusInfo(HEALTHY, "healthy-info"));
        final int initialClusterSize = between(1, 3);
        try (Cluster cluster = new Cluster(initialClusterSize, true, Settings.EMPTY, nodeHealthServiceStatus::get)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();

            final int newClusterNodes = between(initialClusterSize + 1, 4);
            logger.info("--> adding [{}] new healthy nodes", newClusterNodes);
            final NodeHealthService alwaysHealthy = () -> new StatusInfo(HEALTHY, "healthy-info");
            final Set<String> newNodeIds = Sets.newHashSetWithExpectedSize(newClusterNodes);
            for (int i = 0; i < newClusterNodes; i++) {
                final ClusterNode node = cluster.new ClusterNode(nextNodeIndex.getAndIncrement(), true, leader.nodeSettings, alwaysHealthy);
                newNodeIds.add(node.getId());
                cluster.clusterNodes.add(node);
            }

            cluster.stabilise(
                // The first pinging discovers the master
                defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING)
                    // One message delay to send a join
                    + DEFAULT_DELAY_VARIABILITY
                    // Commit a new cluster state with the new node(s). Might be split into multiple commits, and each might need a
                    // followup reconfiguration
                    + 3 * 2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );

            logger.info("--> change initial nodes to report as unhealthy");
            nodeHealthServiceStatus.getAndSet(new StatusInfo(UNHEALTHY, "unhealthy-info"));

            cluster.stabilise(
                // first wait for all the followers to notice the leader has gone
                (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING))
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
                    )

                    // then wait for the new leader to commit a state without the old leader
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );

            final String leaderId = cluster.getAnyLeader().getId();
            assertTrue(leaderId + " should be one of " + newNodeIds, newNodeIds.contains(leaderId));
        }
    }

    public void testFollowerDisconnectionDetectedQuickly() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower = cluster.getAnyNodeExcept(leader);
            logger.info("--> disconnecting follower {}", follower);
            follower.disconnect();
            logger.info("--> leader {} and follower {} get disconnect event", leader, follower);
            leader.onDisconnectEventFrom(follower);
            follower.onDisconnectEventFrom(leader); // to turn follower into candidate, which stabilisation asserts at the end
            cluster.stabilise(
                DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // then wait for the followup reconfiguration
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );
            assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
        }
    }

    public void testFollowerDisconnectionWithoutDisconnectEventDetectedQuickly() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower = cluster.getAnyNodeExcept(leader);
            logger.info("--> disconnecting follower {}", follower);
            follower.disconnect();
            cluster.stabilise(
                Math.max(
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
                )
            );
            assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
        }
    }

    public void testUnresponsiveFollowerDetectedEventually() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower = cluster.getAnyNodeExcept(leader);
            logger.info("--> blackholing follower {}", follower);
            follower.blackhole();

            cluster.stabilise(
                Math.max(
                    // wait for the leader to notice that the follower is unresponsive
                    (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)) * defaultInt(
                        FOLLOWER_CHECK_RETRY_COUNT_SETTING
                    )
                        // then wait for the leader to commit a state without the follower
                        + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                        // then wait for the followup reconfiguration
                        + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,

                    // ALSO wait for the follower to notice the leader is unresponsive
                    (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + defaultMillis(LEADER_CHECK_TIMEOUT_SETTING)) * defaultInt(
                        LEADER_CHECK_RETRY_COUNT_SETTING
                    )
                )
            );
            assertThat(cluster.getAnyLeader().getId(), equalTo(leader.getId()));
        }
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
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower2 = cluster.getAnyNodeExcept(leader, follower1);

            // restart follower1 and follower2
            for (ClusterNode clusterNode : List.of(follower1, follower2)) {
                clusterNode.close();
                cluster.clusterNodes.forEach(cn -> cluster.deterministicTaskQueue.scheduleNow(cn.onNode(new Runnable() {
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
    }

    public void testAckListenerReceivesAcksFromAllNodes() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertTrue("expected ack from " + clusterNode, ackCollector.hasAckedSuccessfully(clusterNode));
            }
            assertThat(
                "leader should be last to ack",
                ackCollector.getSuccessfulAckIndex(leader),
                equalTo(cluster.clusterNodes.size() - 1)
            );
        }
    }

    public void testAckListenerReceivesNackFromFollower() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

            follower0.allowClusterStateApplicationFailure();
            follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.FAIL);
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
            assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
            assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));
            assertThat("leader should be last to ack", ackCollector.getSuccessfulAckIndex(leader), equalTo(1));
        }
    }

    public void testAckListenerReceivesNackFromLeader() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);
            final long startingTerm = leader.coordinator.getCurrentTerm();

            leader.allowClusterStateApplicationFailure();
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
    }

    public void testAckListenerReceivesNoAckFromHangingFollower() {
        try (
            Cluster cluster = new Cluster(
                3,
                true,
                Settings.builder().put(LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING.getKey(), "1000d").build()
            )
        ) {
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

            logger.info("--> publishing final value to resynchronize nodes");
            follower0.setClusterStateApplyResponse(ClusterStateApplyResponse.SUCCEED);
            ackCollector = leader.submitValue(randomLong());
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertTrue(ackCollector.hasAckedSuccessfully(leader));
            assertTrue(ackCollector.hasAckedSuccessfully(follower0));
            assertTrue(ackCollector.hasAckedSuccessfully(follower1));
        }
    }

    public void testAckListenerReceivesNacksIfPublicationTimesOut() {
        try (Cluster cluster = new Cluster(3)) {
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
    }

    public void testAckListenerReceivesNacksIfLeaderStandsDown() {
        testAckListenerReceivesNacksIfLeaderStandsDown(Settings.EMPTY, TimeValue.ZERO);
    }

    protected void testAckListenerReceivesNacksIfLeaderStandsDown(Settings settings, TimeValue extraStabilisationTime) {
        try (Cluster cluster = new Cluster(3, true, settings)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

            leader.blackhole();
            follower0.onDisconnectEventFrom(leader);
            follower1.onDisconnectEventFrom(leader);
            // let followers elect a leader among themselves before healing the leader and running the publication
            cluster.runFor(
                DEFAULT_DELAY_VARIABILITY // disconnect is scheduled
                    + DEFAULT_ELECTION_DELAY + extraStabilisationTime.millis(),
                "elect new leader"
            );
            // cluster has two nodes in mode LEADER, in different terms ofc, and the one in the lower term won’t be able to publish anything
            leader.heal();
            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.stabilise();
            assertTrue("expected nack from " + leader, ackCollector.hasAckedUnsuccessfully(leader));
            assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
            assertTrue("expected nack from " + follower1, ackCollector.hasAckedUnsuccessfully(follower1));
        }
    }

    public void testAckListenerReceivesNacksFromFollowerInHigherTerm() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode follower0 = cluster.getAnyNodeExcept(leader);
            final ClusterNode follower1 = cluster.getAnyNodeExcept(leader, follower0);

            final long originalTerm = leader.coordinator.getCurrentTerm();
            follower0.coordinator.onFollowerCheckRequest(
                new FollowersChecker.FollowerCheckRequest(originalTerm + 1, leader.coordinator.getLocalNode())
            );

            AckCollector ackCollector = leader.submitValue(randomLong());
            cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "cluster state update");
            assertTrue("expected ack from " + leader, ackCollector.hasAckedSuccessfully(leader));
            assertTrue("expected nack from " + follower0, ackCollector.hasAckedUnsuccessfully(follower0));
            assertTrue("expected ack from " + follower1, ackCollector.hasAckedSuccessfully(follower1));

            cluster.stabilise();
            assertThat(cluster.getAnyLeader().coordinator.getCurrentTerm(), greaterThanOrEqualTo(originalTerm + 1));
        }
    }

    public void testCannotSetInitialConfigurationTwice() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final Coordinator coordinator = cluster.getAnyNode().coordinator;
            assertFalse(coordinator.setInitialConfiguration(coordinator.getLastAcceptedState().getLastCommittedConfiguration()));
        }
    }

    public void testDiffBasedPublishing() {
        try (Cluster cluster = new Cluster(randomIntBetween(2, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final long finalValue = randomLong();
            final Map<ClusterNode, PublishClusterStateStats> prePublishStats = cluster.clusterNodes.stream()
                .collect(Collectors.toMap(Function.identity(), cn -> cn.coordinator.stats().getPublishStats()));

            if (cluster.clusterNodes.size() > 1) {
                // at least one node must have published at least one full cluster state when other nodes joined
                final Optional<PublishClusterStateStats> optionalStats = prePublishStats.values()
                    .stream()
                    .filter(stats -> stats.getClusterStateSerializationStats().getFullStateCount() > 0)
                    .findAny();
                assertTrue(optionalStats.isPresent());
                final ClusterStateSerializationStats serializationStats = optionalStats.get().getClusterStateSerializationStats();
                assertThat(serializationStats.toString(), serializationStats.getTotalUncompressedFullStateBytes(), greaterThan(0L));
                assertThat(serializationStats.toString(), serializationStats.getTotalCompressedFullStateBytes(), greaterThan(4L));
            }

            logger.info("--> submitting value [{}] to [{}]", finalValue, leader);
            leader.submitValue(finalValue);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            final Map<ClusterNode, PublishClusterStateStats> postPublishStats = cluster.clusterNodes.stream()
                .collect(Collectors.toMap(Function.identity(), cn -> cn.coordinator.stats().getPublishStats()));

            for (ClusterNode cn : cluster.clusterNodes) {
                assertThat(value(cn.getLastAppliedClusterState()), is(finalValue));
                assertEquals(
                    cn.toString(),
                    prePublishStats.get(cn).getFullClusterStateReceivedCount(),
                    postPublishStats.get(cn).getFullClusterStateReceivedCount()
                );
                assertEquals(
                    cn.toString(),
                    prePublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount() + (cn == leader ? 0 : 1),
                    postPublishStats.get(cn).getCompatibleClusterStateDiffReceivedCount()
                );
                assertEquals(
                    cn.toString(),
                    prePublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount(),
                    postPublishStats.get(cn).getIncompatibleClusterStateDiffReceivedCount()
                );

                if (cn != leader) {
                    assertEquals(
                        cn.toString(),
                        Strings.toString(prePublishStats.get(cn).getClusterStateSerializationStats()),
                        Strings.toString(postPublishStats.get(cn).getClusterStateSerializationStats())
                    );
                }
            }

            final ClusterStateSerializationStats serializationStats0 = prePublishStats.get(leader).getClusterStateSerializationStats();
            final ClusterStateSerializationStats serializationStats1 = postPublishStats.get(leader).getClusterStateSerializationStats();

            assertThat(serializationStats1.getDiffCount(), equalTo(serializationStats0.getDiffCount() + 1));
            assertThat(serializationStats1.getTotalUncompressedDiffBytes(), greaterThan(serializationStats0.getDiffCount()));
            assertThat(
                serializationStats1.getTotalCompressedDiffBytes(),
                greaterThan(serializationStats0.getDiffCount() + 4 /* compressed data starts with 4 byte header */)
            );

            assertThat(serializationStats1.getFullStateCount(), equalTo(serializationStats0.getFullStateCount()));
            assertThat(
                serializationStats1.getTotalUncompressedFullStateBytes(),
                equalTo(serializationStats0.getTotalUncompressedFullStateBytes())
            );
            assertThat(
                serializationStats1.getTotalCompressedFullStateBytes(),
                equalTo(serializationStats0.getTotalCompressedFullStateBytes())
            );
        }
    }

    private static class TimeAdvancer {

        public static final int MAX_ADVANCE_MILLIS = 2000;

        private final DeterministicTaskQueue deterministicTaskQueue;
        private long elapsedTime;

        TimeAdvancer(DeterministicTaskQueue deterministicTaskQueue) {
            this.deterministicTaskQueue = deterministicTaskQueue;
        }

        void advanceTime() {
            final long startTime = deterministicTaskQueue.getCurrentTimeMillis();
            deterministicTaskQueue.scheduleAt(startTime + between(1000, MAX_ADVANCE_MILLIS), new Runnable() {
                @Override
                public void run() {}

                @Override
                public String toString() {
                    return "no-op task to advance time";
                }
            });
            deterministicTaskQueue.advanceTime();
            elapsedTime += deterministicTaskQueue.getCurrentTimeMillis() - startTime;
        }

        long getElapsedTime() {
            return elapsedTime;
        }
    }

    public void testMasterStatsOnNoOpUpdate() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5), false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();

            for (ClusterNode clusterNode : cluster.getAllNodesExcept()) {
                assertThat(
                    clusterNode.coordinator.stats().getClusterStateUpdateStats() != null,
                    equalTo(clusterNode.getLocalNode().isMasterNode())
                );
            }

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterStateUpdateStats stats0 = leader.coordinator.stats().getClusterStateUpdateStats();

            final TimeAdvancer computeAdvancer = new TimeAdvancer(cluster.deterministicTaskQueue);
            final TimeAdvancer notifyAdvancer = new TimeAdvancer(cluster.deterministicTaskQueue);
            leader.submitUpdateTask("unchanged", cs -> {
                computeAdvancer.advanceTime();
                return cs;
            }, new CoordinatorTestClusterStateUpdateTask() {
                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    notifyAdvancer.advanceTime();
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : e;
                }
            });

            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY + TimeAdvancer.MAX_ADVANCE_MILLIS * 2);

            final ClusterStateUpdateStats stats1 = leader.coordinator.stats().getClusterStateUpdateStats();

            final String description = Strings.toString(stats1) + " vs " + Strings.toString(stats0);
            assertThat(description, stats1.getUnchangedTaskCount() - stats0.getUnchangedTaskCount(), greaterThanOrEqualTo(1L));
            assertThat(
                description,
                stats1.getUnchangedComputationElapsedMillis() - stats0.getUnchangedComputationElapsedMillis(),
                greaterThanOrEqualTo(computeAdvancer.getElapsedTime())
            );
            assertThat(
                description,
                stats1.getUnchangedNotificationElapsedMillis() - stats0.getUnchangedNotificationElapsedMillis(),
                greaterThanOrEqualTo(notifyAdvancer.getElapsedTime())
            );
        }
    }

    public void testMasterStatsOnSuccessfulUpdate() {

        final String customName = "delayed";

        class DelayedCustom extends AbstractNamedDiffable<ClusterState.Custom> implements ClusterState.Custom {
            @Nullable
            private final TimeAdvancer timeAdvancer;

            DelayedCustom(TimeAdvancer timeAdvancer) {
                super();
                this.timeAdvancer = timeAdvancer;
            }

            @Override
            public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
                return Collections.emptyIterator();
            }

            @Override
            public String getWriteableName() {
                return customName;
            }

            @Override
            public TransportVersion getMinimalSupportedVersion() {
                return TransportVersion.CURRENT;
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                if (timeAdvancer != null) {
                    timeAdvancer.advanceTime();
                }
            }
        }

        try (Cluster cluster = new Cluster(randomIntBetween(1, 5)) {
            @Override
            protected List<NamedWriteableRegistry.Entry> extraNamedWriteables() {
                return List.of(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, customName, in -> new DelayedCustom(null)));
            }
        }) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterStateUpdateStats stats0 = leader.coordinator.stats().getClusterStateUpdateStats();

            final TimeAdvancer computeAdvancer = new TimeAdvancer(cluster.deterministicTaskQueue);
            final TimeAdvancer notifyAdvancer = new TimeAdvancer(cluster.deterministicTaskQueue);
            final TimeAdvancer contextAdvancer = new TimeAdvancer(cluster.deterministicTaskQueue);
            leader.submitUpdateTask("update", cs -> {
                computeAdvancer.advanceTime();
                return ClusterState.builder(cs).putCustom(customName, new DelayedCustom(contextAdvancer)).build();
            }, new CoordinatorTestClusterStateUpdateTask() {
                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    notifyAdvancer.advanceTime();
                }

                @Override
                public void onFailure(Exception e) {
                    assert false : e;
                }
            });

            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY + TimeAdvancer.MAX_ADVANCE_MILLIS * 3);

            final ClusterStateUpdateStats stats1 = leader.coordinator.stats().getClusterStateUpdateStats();

            final String description = Strings.toString(stats1) + " vs " + Strings.toString(stats0);
            assertThat(description, stats1.getPublicationSuccessCount() - stats0.getPublicationSuccessCount(), greaterThanOrEqualTo(1L));
            assertThat(
                description,
                stats1.getSuccessfulComputationElapsedMillis() - stats0.getSuccessfulComputationElapsedMillis(),
                greaterThanOrEqualTo(computeAdvancer.getElapsedTime())
            );
            assertThat(
                description,
                stats1.getSuccessfulNotificationElapsedMillis() - stats0.getSuccessfulNotificationElapsedMillis(),
                greaterThanOrEqualTo(notifyAdvancer.getElapsedTime())
            );
            assertThat(
                description,
                stats1.getSuccessfulPublicationElapsedMillis() - stats0.getSuccessfulPublicationElapsedMillis(),
                greaterThanOrEqualTo(notifyAdvancer.getElapsedTime())
            );
            assertThat(
                description,
                stats1.getSuccessfulContextConstructionElapsedMillis() - stats0.getSuccessfulContextConstructionElapsedMillis(),
                greaterThanOrEqualTo(contextAdvancer.getElapsedTime())
            );

            // this is atomic up to some scheduling delay
            assertThat(
                description,
                stats1.getSuccessfulMasterApplyElapsedMillis() - stats0.getSuccessfulMasterApplyElapsedMillis(),
                lessThanOrEqualTo(DEFAULT_DELAY_VARIABILITY)
            );
        }
    }

    public void testMasterStatsOnFailedUpdate() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterStateUpdateStats stats0 = leader.coordinator.stats().getClusterStateUpdateStats();

            final TimeAdvancer computeAdvancer = new TimeAdvancer(cluster.deterministicTaskQueue);
            final TimeAdvancer notifyAdvancer = new TimeAdvancer(cluster.deterministicTaskQueue);
            leader.submitUpdateTask("update", cs -> {
                computeAdvancer.advanceTime();
                return ClusterState.builder(cs).build();
            }, new CoordinatorTestClusterStateUpdateTask() {
                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    fail("shouldn't have processed cluster state");
                }

                @Override
                public void onFailure(Exception e) {
                    notifyAdvancer.advanceTime();
                }
            });

            leader.blackhole();
            cluster.stabilise(DEFAULT_STABILISATION_TIME + TimeAdvancer.MAX_ADVANCE_MILLIS * 2);

            final ClusterStateUpdateStats stats1 = leader.coordinator.stats().getClusterStateUpdateStats();

            final String description = Strings.toString(stats1) + " vs " + Strings.toString(stats0);
            assertThat(description, stats1.getPublicationFailureCount() - stats0.getPublicationFailureCount(), greaterThanOrEqualTo(1L));
            assertThat(
                description,
                stats1.getFailedComputationElapsedMillis() - stats0.getFailedComputationElapsedMillis(),
                greaterThanOrEqualTo(computeAdvancer.getElapsedTime())
            );
            assertThat(
                description,
                stats1.getFailedNotificationElapsedMillis() - stats0.getFailedNotificationElapsedMillis(),
                greaterThanOrEqualTo(notifyAdvancer.getElapsedTime())
            );
            assertThat(
                description,
                stats1.getFailedPublicationElapsedMillis() - stats0.getFailedPublicationElapsedMillis(),
                greaterThanOrEqualTo(notifyAdvancer.getElapsedTime())
            );

            // this action is atomic, no simulated time can elapse
            assertThat(description, stats0.getFailedContextConstructionElapsedMillis(), equalTo(0L));
            assertThat(description, stats1.getFailedContextConstructionElapsedMillis(), equalTo(0L));

            // no state should have been applied
            assertThat(description, stats1.getFailedMasterApplyElapsedMillis() - stats0.getFailedMasterApplyElapsedMillis(), equalTo(0L));
        }
    }

    public void testJoiningNodeReceivesFullState() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
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
    }

    public void testIncompatibleDiffResendsFullState() {
        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
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
            assertEquals(prePublishStats.getFullClusterStateReceivedCount() + 1, postPublishStats.getFullClusterStateReceivedCount());
            assertEquals(
                prePublishStats.getCompatibleClusterStateDiffReceivedCount(),
                postPublishStats.getCompatibleClusterStateDiffReceivedCount()
            );
            assertEquals(
                prePublishStats.getIncompatibleClusterStateDiffReceivedCount() + 1,
                postPublishStats.getIncompatibleClusterStateDiffReceivedCount()
            );
        }
    }

    /**
     * Simulates a situation where a follower becomes disconnected from the leader, but only for such a short time where
     * it becomes candidate and puts up a NO_MASTER_BLOCK, but then receives a follower check from the leader. If the leader
     * does not notice the node disconnecting, it is important for the node not to be turned back into a follower but try
     * and join the leader again.
     */
    public void testStayCandidateAfterReceivingFollowerCheckFromKnownMaster() {
        try (Cluster cluster = new Cluster(2, false, Settings.EMPTY)) {
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
                expectThrows(
                    CoordinationStateRejectedException.class,
                    () -> nonLeader.coordinator.onFollowerCheckRequest(
                        new FollowersChecker.FollowerCheckRequest(leader.coordinator.getCurrentTerm(), leader.getLocalNode())
                    )
                );
                assertThat(nonLeader.coordinator.getMode(), equalTo(CANDIDATE));
            }).run();
            cluster.stabilise();
        }
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

    public void testAppliesNoMasterBlockMetadataWritesIfConfigured() {
        testAppliesNoMasterBlock("metadata_write", NO_MASTER_BLOCK_METADATA_WRITES);
    }

    private void testAppliesNoMasterBlock(String noMasterBlockSetting, ClusterBlock expectedBlock) {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            leader.submitUpdateTask("update NO_MASTER_BLOCK_SETTING", cs -> {
                final Builder settingsBuilder = Settings.builder().put(cs.metadata().persistentSettings());
                settingsBuilder.put(NO_MASTER_BLOCK_SETTING.getKey(), noMasterBlockSetting);
                return ClusterState.builder(cs)
                    .metadata(Metadata.builder(cs.metadata()).persistentSettings(settingsBuilder.build()))
                    .build();
            }, (e) -> {});
            cluster.runFor(DEFAULT_CLUSTER_STATE_UPDATE_DELAY, "committing setting update");

            final ClusterNode removedNode = cluster.getAnyNode();

            removedNode.disconnect();
            cluster.runFor(
                Math.max(
                    defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING) + defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING),
                    defaultMillis(LEADER_CHECK_TIMEOUT_SETTING) + defaultMillis(LEADER_CHECK_INTERVAL_SETTING)
                ) + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,
                "detecting disconnection"
            );

            assertThat(removedNode.getLastAppliedClusterState().blocks().global(), hasItem(expectedBlock));

            removedNode.close();
            final ClusterNode restartedNode = removedNode.restartedNode();
            cluster.clusterNodes.replaceAll(cn -> cn == removedNode ? restartedNode : cn);
            restartedNode.disconnect();

            cluster.stabilise();
            assertThat(restartedNode.getLastAppliedClusterState().blocks().global(), hasItem(expectedBlock));
        }
    }

    public void testNodeCannotJoinIfJoinValidationFailsOnMaster() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 3))) {
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
    }

    @TestLogging(
        reason = "test includes assertions about Coordinator and JoinHelper logging",
        value = "org.elasticsearch.cluster.coordination.Coordinator:WARN,org.elasticsearch.cluster.coordination.JoinHelper:INFO"
    )
    public void testNodeCannotJoinIfJoinPingValidationFailsOnMaster() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 3))) {
            cluster.runRandomly();
            cluster.stabilise();

            cluster.getAnyLeader().addActionBlock(JoinHelper.JOIN_PING_ACTION_NAME);

            // check that if node ping join validation fails on master, the nodes can't join
            List<ClusterNode> addedNodes = cluster.addNodes(randomIntBetween(1, 2));
            final long previousClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();

            MockLogAppender mockAppender = new MockLogAppender();
            try (var ignored = mockAppender.capturing(JoinHelper.class, Coordinator.class)) {
                mockAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "failed to join",
                        JoinHelper.class.getCanonicalName(),
                        Level.INFO,
                        "*failed to join*"
                    )
                );
                mockAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "failed to ping",
                        Coordinator.class.getCanonicalName(),
                        Level.WARN,
                        "*failed to ping joining node*"
                    )
                );
                cluster.runFor(10000, "failing joins");
                mockAppender.assertAllExpectationsMatched();
            }

            assertTrue(addedNodes.stream().allMatch(ClusterNode::isCandidate));
            final long newClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
            assertEquals(previousClusterStateVersion, newClusterStateVersion);

            cluster.getAnyLeader().clearActionBlocks();
            cluster.stabilise();
        }
    }

    public void testNodeCannotJoinIfJoinValidationFailsOnJoiningNode() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 3))) {
            cluster.runRandomly();
            cluster.stabilise();

            // check that if node join validation fails on joining node, the nodes can't join
            List<ClusterNode> addedNodes = cluster.addNodes(randomIntBetween(1, 2));
            final Set<DiscoveryNode> validatedNodes = new HashSet<>();
            addedNodes.forEach(cn -> cn.extraJoinValidators.add((discoveryNode, clusterState) -> {
                validatedNodes.add(discoveryNode);
                throw new IllegalArgumentException("join validation failed");
            }));
            final long previousClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
            cluster.runFor(10000, "failing join validation");
            assertEquals(validatedNodes, addedNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet()));
            assertTrue(addedNodes.stream().allMatch(ClusterNode::isCandidate));
            final long newClusterStateVersion = cluster.getAnyLeader().getLastAppliedClusterState().version();
            assertEquals(previousClusterStateVersion, newClusterStateVersion);

            addedNodes.forEach(cn -> cn.extraJoinValidators.clear());
            cluster.stabilise();
        }
    }

    public void testClusterCannotFormWithFailingJoinValidation() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            // fail join validation on a majority of nodes in the initial configuration
            randomValueOtherThanMany(
                nodes -> cluster.initialConfiguration.hasQuorum(
                    nodes.stream().map(ClusterNode::getLocalNode).map(DiscoveryNode::getId).collect(Collectors.toSet())
                ) == false,
                () -> randomSubsetOf(cluster.clusterNodes)
            ).forEach(cn -> cn.extraJoinValidators.add((discoveryNode, clusterState) -> {
                throw new IllegalArgumentException("join validation failed");
            }));
            cluster.bootstrapIfNecessary();
            cluster.runFor(10000, "failing join validation");
            assertTrue(cluster.clusterNodes.stream().allMatch(cn -> cn.getLastAppliedClusterState().version() == 0));

            for (ClusterNode clusterNode : cluster.clusterNodes) {
                clusterNode.extraJoinValidators.clear();
            }

            cluster.stabilise();
        }
    }

    @TestLogging(
        reason = "test includes assertions about JoinHelper logging",
        value = "org.elasticsearch.cluster.coordination.JoinHelper:INFO"
    )
    public void testCannotJoinClusterWithDifferentUUID() {
        try (Cluster cluster1 = new Cluster(randomIntBetween(1, 3))) {
            cluster1.runRandomly();
            cluster1.stabilise();

            final ClusterNode nodeInOtherCluster;
            try (Cluster cluster2 = new Cluster(3)) {
                cluster2.runRandomly();
                cluster2.stabilise();

                nodeInOtherCluster = randomFrom(cluster2.clusterNodes);
            }

            final ClusterNode newNode = cluster1.new ClusterNode(
                nextNodeIndex.getAndIncrement(), nodeInOtherCluster.getLocalNode(), (node, threadPool) -> cluster1
                    .createPersistedStateFromExistingState(
                        node,
                        nodeInOtherCluster.persistedState,
                        Function.identity(),
                        Function.identity(),
                        threadPool
                    ), nodeInOtherCluster.nodeSettings, () -> new StatusInfo(StatusInfo.Status.HEALTHY, "healthy-info")
            );

            cluster1.clusterNodes.add(newNode);

            MockLogAppender mockAppender = new MockLogAppender();
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation("test1", JoinHelper.class.getCanonicalName(), Level.INFO, "*failed to join*")
            );
            try (var ignored = mockAppender.capturing(JoinHelper.class)) {
                cluster1.runFor(DEFAULT_STABILISATION_TIME, "failing join validation");
                mockAppender.assertAllExpectationsMatched();
            }
            assertEquals(0, newNode.getLastAppliedClusterState().version());

            newNode.close();
            final ClusterNode detachedNode = newNode.restartedNode(
                DetachClusterCommand::updateMetadata,
                term -> DetachClusterCommand.updateCurrentTerm(),
                newNode.nodeSettings
            );
            cluster1.clusterNodes.replaceAll(cn -> cn == newNode ? detachedNode : cn);
            cluster1.stabilise();
        }
    }

    @TestLogging(
        reason = "test includes assertions about logging",
        value = "org.elasticsearch.cluster.coordination.Coordinator:WARN,org.elasticsearch.cluster.coordination.JoinHelper:INFO"
    )
    public void testReportsConnectBackProblemsDuringJoining() {
        try (var cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final var partitionedNode = cluster.getAnyNode();
            partitionedNode.disconnect();
            cluster.stabilise();

            logger.info("--> healing [{}] but blocking handshakes", partitionedNode);
            partitionedNode.heal();
            final var leader = cluster.getAnyLeader();
            leader.addActionBlock(TransportService.HANDSHAKE_ACTION_NAME);

            final var mockAppender = new MockLogAppender();
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "connect-back failure",
                    Coordinator.class.getCanonicalName(),
                    Level.WARN,
                    "*received join request from ["
                        + partitionedNode.getLocalNode().descriptionWithoutAttributes()
                        + "] but could not connect back to the joining node"
                )
            );
            mockAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
                boolean matched = false;

                @Override
                public void match(LogEvent event) {
                    if (event.getLevel() != Level.INFO) {
                        return;
                    }
                    if (event.getLoggerName().equals(JoinHelper.class.getCanonicalName()) == false) {
                        return;
                    }

                    var cause = event.getThrown();
                    if (cause == null) {
                        return;
                    }
                    cause = cause.getCause();
                    if (cause == null) {
                        return;
                    }
                    if (Regex.simpleMatch(
                        "* failure when opening connection back from ["
                            + leader.getLocalNode().descriptionWithoutAttributes()
                            + "] to ["
                            + partitionedNode.getLocalNode().descriptionWithoutAttributes()
                            + "]",
                        cause.getMessage()
                    ) == false) {
                        return;
                    }
                    if (cause.getStackTrace() != null && cause.getStackTrace().length != 0) {
                        return;
                    }
                    matched = true;
                }

                @Override
                public void assertMatched() {
                    assertTrue(matched);
                }
            });
            try (var ignored = mockAppender.capturing(Coordinator.class, JoinHelper.class)) {
                cluster.runFor(
                    // This expects 8 tasks to be executed after PeerFinder handling wakeup:
                    //
                    // * connectToRemoteMasterNode[0.0.0.0:11]
                    // * [internal:transport/handshake] from {node1} to {node2}
                    // * response to [internal:transport/handshake] from {node1} to {node2}
                    // * [internal:discovery/request_peers] from {node1} to
                    // * response to [internal:discovery/request_peers] from {node1} to {node2}
                    // * [internal:cluster/coordination/join] from {node1} to {node2}
                    // * [internal:transport/handshake] from {node2} to {node1} (rejected due to action block)
                    // * error response to [internal:cluster/coordination/join] from {node1} to {node2}
                    //
                    defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) + 8 * DEFAULT_DELAY_VARIABILITY,
                    "allowing time for join attempt"
                );
                mockAppender.assertAllExpectationsMatched();
            }

            leader.clearActionBlocks();
        }
    }

    public void testFollowerRemovedIfUnableToSendRequestsToMaster() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final ClusterNode otherNode = cluster.getAnyNodeExcept(leader);

            cluster.blackholeConnectionsFrom(otherNode, leader);

            cluster.runFor(
                (defaultMillis(FOLLOWER_CHECK_INTERVAL_SETTING) + defaultMillis(FOLLOWER_CHECK_TIMEOUT_SETTING)) * defaultInt(
                    FOLLOWER_CHECK_RETRY_COUNT_SETTING
                ) + (defaultMillis(LEADER_CHECK_INTERVAL_SETTING) + DEFAULT_DELAY_VARIABILITY) * defaultInt(
                    LEADER_CHECK_RETRY_COUNT_SETTING
                ) + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,
                "awaiting removal of asymmetrically-partitioned node"
            );

            assertThat(
                leader.getLastAppliedClusterState().nodes().toString(),
                leader.getLastAppliedClusterState().nodes().getSize(),
                equalTo(2)
            );

            cluster.clearBlackholedConnections();

            cluster.stabilise(
                // time for the disconnected node to find the master again
                defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2
                    // time for joining
                    + 4 * DEFAULT_DELAY_VARIABILITY
                    // Then a commit of the updated cluster state
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );
        }
    }

    public void testSingleNodeDiscoveryWithQuorum() {
        try (
            Cluster cluster = new Cluster(
                1,
                randomBoolean(),
                Settings.builder().put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE).build()
            )
        ) {
            cluster.runRandomly();
            cluster.stabilise();
        }
    }

    public void testSingleNodeDiscoveryStabilisesEvenWhenDisrupted() {
        // A cluster using single-node discovery should not apply any timeouts to joining or cluster state publication. There are no
        // other options, so there's no point in failing and retrying from scratch no matter how badly disrupted we are and we may as
        // well just wait.

        // larger variability is are good for checking that we don't time out, but smaller variability also tightens up the time bound
        // within which we expect to converge, so use a mix of both
        final long delayVariabilityMillis = randomLongBetween(DEFAULT_DELAY_VARIABILITY, TimeValue.timeValueMinutes(10).millis());

        Settings.Builder settings = Settings.builder()
            .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE);

        // If the delay variability is high, set election duration accordingly, to avoid the possible endless repetition of voting rounds.
        // Note that elections could take even longer than the delay variability, but this seems to be long enough to avoid bad collisions.
        if (ElectionSchedulerFactory.ELECTION_DURATION_SETTING.getDefault(Settings.EMPTY).getMillis() < delayVariabilityMillis) {
            settings = settings.put(
                ElectionSchedulerFactory.ELECTION_DURATION_SETTING.getKey(),
                TimeValue.timeValueMillis(delayVariabilityMillis)
            );
        }

        try (Cluster cluster = new Cluster(1, randomBoolean(), settings.build())) {
            if (randomBoolean()) {
                cluster.runRandomly(true, false, delayVariabilityMillis);
            }

            cluster.deterministicTaskQueue.setExecutionDelayVariabilityMillis(delayVariabilityMillis);

            final ClusterNode clusterNode = cluster.getAnyNode();

            final long clusterStateUpdateDelay = CLUSTER_STATE_UPDATE_NUMBER_OF_DELAYS * delayVariabilityMillis;

            // cf. DEFAULT_STABILISATION_TIME, but stabilisation is quicker when there's a single node - there's no meaningful fault
            // detection and ongoing publications do not time out
            cluster.runFor(ELECTION_INITIAL_TIMEOUT_SETTING.get(Settings.EMPTY).millis() + delayVariabilityMillis
            // two round trips for pre-voting and voting
                + 4 * delayVariabilityMillis
                // and then the election update
                + clusterStateUpdateDelay, "stabilising");

            assertThat(cluster.getAnyLeader(), sameInstance(clusterNode));

            final int pendingTaskCount = clusterNode.getPendingTaskCount();
            cluster.runFor((pendingTaskCount + 1) * clusterStateUpdateDelay, "draining task queue");

            assertFalse(clusterNode.coordinator.publicationInProgress());
            assertThat(
                clusterNode.coordinator.getLastAcceptedState().version(),
                equalTo(clusterNode.getLastAppliedClusterState().version())
            );
            cluster.deterministicTaskQueue.setExecutionDelayVariabilityMillis(DEFAULT_DELAY_VARIABILITY);
        }
    }

    private static class BrokenCustom implements SimpleDiffable<ClusterState.Custom>, ClusterState.Custom {

        static final String EXCEPTION_MESSAGE = "simulated";

        @Override
        public String getWriteableName() {
            return "broken";
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.ZERO;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            throw new ElasticsearchException(EXCEPTION_MESSAGE);
        }

        @Override
        public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
            return Collections.emptyIterator();
        }

    }

    public void testClusterRecoversAfterExceptionDuringSerialization() {
        try (Cluster cluster = new Cluster(randomIntBetween(2, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader1 = cluster.getAnyLeader();

            logger.info("--> submitting broken task to [{}]", leader1);

            final AtomicBoolean failed = new AtomicBoolean();
            leader1.submitUpdateTask("broken-task", cs -> ClusterState.builder(cs).putCustom("broken", new BrokenCustom()).build(), (e) -> {
                assertThat(e.getCause(), instanceOf(ElasticsearchException.class));
                assertThat(e.getCause().getMessage(), equalTo(BrokenCustom.EXCEPTION_MESSAGE));
                failed.set(true);
            });
            // allow for forking 3 times:
            // - once onto the master-service thread
            // - once to fork the publication in FakeThreadPoolMasterService
            // - once to fork the publication listener back onto the master-service thread
            cluster.runFor(3 * DEFAULT_DELAY_VARIABILITY + 1, "processing broken task");
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
    }

    @TestLogging(
        reason = "testing ClusterFormationFailureHelper logging",
        value = "org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper:WARN"
    )
    public void testLogsWarningPeriodicallyIfClusterNotFormed() {
        testLogsWarningPeriodicallyIfClusterNotFormed(
            "master not discovered or elected yet, an election requires at least 2 nodes with ids from [",
            nodeId -> "*have only discovered non-quorum *" + nodeId + "*discovery will continue*"
        );
    }

    protected void testLogsWarningPeriodicallyIfClusterNotFormed(String expectedMessageStart, UnaryOperator<String> discoveryMessageFn) {
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

        try (Cluster cluster = new Cluster(3, true, settings)) {
            cluster.runRandomly();
            cluster.stabilise();

            logger.info("--> disconnecting all nodes");

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                clusterNode.disconnect();
            }

            cluster.runFor(
                defaultMillis(LEADER_CHECK_TIMEOUT_SETTING) // to wait for any in-flight check to time out
                    + defaultMillis(LEADER_CHECK_INTERVAL_SETTING) // to wait for the next check to be sent
                    + 2 * DEFAULT_DELAY_VARIABILITY, // to send the failing check and receive the disconnection response
                "waiting for leader failure"
            );

            for (final ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(clusterNode.getId() + " is CANDIDATE", clusterNode.coordinator.getMode(), is(CANDIDATE));
            }

            for (int i = scaledRandomIntBetween(1, 10); i >= 0; i--) {
                final MockLogAppender mockLogAppender = new MockLogAppender();
                try (var ignored = mockLogAppender.capturing(ClusterFormationFailureHelper.class)) {
                    mockLogAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
                        final Set<DiscoveryNode> nodesLogged = new HashSet<>();

                        @Override
                        public void match(LogEvent event) {
                            final String message = event.getMessage().getFormattedMessage();
                            assertThat(message, startsWith(expectedMessageStart));

                            final List<ClusterNode> matchingNodes = cluster.clusterNodes.stream()
                                .filter(
                                    n -> event.getContextData()
                                        .<String>getValue(DeterministicTaskQueue.NODE_ID_LOG_CONTEXT_KEY)
                                        .equals(DeterministicTaskQueue.getNodeIdForLogContext(n.getLocalNode()))
                                )
                                .toList();
                            assertThat(matchingNodes, hasSize(1));

                            assertTrue(message, Regex.simpleMatch(discoveryMessageFn.apply(matchingNodes.get(0).toString()), message));

                            nodesLogged.add(matchingNodes.get(0).getLocalNode());
                        }

                        @Override
                        public void assertMatched() {
                            assertThat(
                                nodesLogged + " vs " + cluster.clusterNodes,
                                nodesLogged,
                                equalTo(cluster.clusterNodes.stream().map(ClusterNode::getLocalNode).collect(Collectors.toSet()))
                            );
                        }
                    });
                    cluster.runFor(warningDelayMillis + DEFAULT_DELAY_VARIABILITY, "waiting for warning to be emitted");
                    mockLogAppender.assertAllExpectationsMatched();
                }
            }

            for (ClusterNode clusterNode : cluster.clusterNodes) {
                clusterNode.heal();
            }
        }
    }

    @TestLogging(
        reason = "testing warning of a single-node cluster having discovery seed hosts",
        value = "org.elasticsearch.cluster.coordination.Coordinator:WARN"
    )
    public void testLogsWarningPeriodicallyIfSingleNodeClusterHasSeedHosts() {
        final long warningDelayMillis;
        final Settings settings;
        final String fakeSeedHost = buildNewFakeTransportAddress().toString();
        if (randomBoolean()) {
            settings = Settings.builder().putList(DISCOVERY_SEED_HOSTS_SETTING.getKey(), fakeSeedHost).build();
            warningDelayMillis = Coordinator.SINGLE_NODE_CLUSTER_SEED_HOSTS_CHECK_INTERVAL_SETTING.get(settings).millis();
        } else {
            warningDelayMillis = randomLongBetween(1, 100000);
            settings = Settings.builder()
                .put(ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.getKey(), warningDelayMillis + "ms")
                .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey(), fakeSeedHost)
                .build();
        }
        logger.info("--> emitting warnings every [{}ms]", warningDelayMillis);

        try (Cluster cluster = new Cluster(1, true, settings)) {
            cluster.runRandomly();
            cluster.stabilise();

            for (int i = scaledRandomIntBetween(1, 10); i >= 0; i--) {
                final MockLogAppender mockLogAppender = new MockLogAppender();
                try (var ignored = mockLogAppender.capturing(Coordinator.class)) {
                    mockLogAppender.addExpectation(new MockLogAppender.LoggingExpectation() {
                        String loggedClusterUuid;

                        @Override
                        public void match(LogEvent event) {
                            final String message = event.getMessage().getFormattedMessage();
                            assertThat(message, startsWith("This node is a fully-formed single-node cluster with cluster UUID"));
                            loggedClusterUuid = (String) event.getMessage().getParameters()[0];
                        }

                        @Override
                        public void assertMatched() {
                            final String clusterUuid = cluster.getAnyNode().getLastAppliedClusterState().metadata().clusterUUID();
                            assertThat(loggedClusterUuid + " vs " + clusterUuid, clusterUuid, equalTo(clusterUuid));
                        }
                    });
                    cluster.runFor(warningDelayMillis + DEFAULT_DELAY_VARIABILITY, "waiting for warning to be emitted");
                    mockLogAppender.assertAllExpectationsMatched();
                }
            }
        }
    }

    public void testInvariantWhenTwoNodeClusterBecomesSingleNodeCluster() {
        try (Cluster cluster = new Cluster(2)) {
            cluster.stabilise();
            assertTrue(cluster.getAnyNodeExcept(cluster.getAnyLeader()).disconnect()); // Remove non-leader node
            cluster.stabilise();
        }
    }

    @TestLogging(
        reason = "testing LagDetector and CoordinatorPublication logging",
        value = "org.elasticsearch.cluster.coordination.LagDetector:DEBUG,"
            + "org.elasticsearch.cluster.coordination.Coordinator.CoordinatorPublication:INFO"
    )
    public void testLogsMessagesIfPublicationDelayed() {
        try (Cluster cluster = new Cluster(between(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();
            final ClusterNode brokenNode = cluster.getAnyNodeExcept(cluster.getAnyLeader());

            final MockLogAppender mockLogAppender = new MockLogAppender();
            try (var ignored = mockLogAppender.capturing(Coordinator.CoordinatorPublication.class, LagDetector.class)) {

                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "publication info message",
                        Coordinator.CoordinatorPublication.class.getCanonicalName(),
                        Level.INFO,
                        "after [*] publication of cluster state version [*] is still waiting for "
                            + brokenNode.getLocalNode()
                            + " ["
                            + Publication.PublicationTargetState.SENT_PUBLISH_REQUEST
                            + ']'
                    )
                );

                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "publication warning",
                        Coordinator.CoordinatorPublication.class.getCanonicalName(),
                        Level.WARN,
                        "after [*] publication of cluster state version [*] is still waiting for "
                            + brokenNode.getLocalNode()
                            + " ["
                            + Publication.PublicationTargetState.SENT_PUBLISH_REQUEST
                            + ']'
                    )
                );

                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "lag warning",
                        LagDetector.class.getCanonicalName(),
                        Level.WARN,
                        "node ["
                            + brokenNode
                            + "] is lagging at cluster state version [*], "
                            + "although publication of cluster state version [*] completed [*] ago"
                    )
                );

                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "hot threads from lagging node",
                        LagDetector.class.getCanonicalName(),
                        Level.DEBUG,
                        "hot threads from node ["
                            + brokenNode.getLocalNode().descriptionWithoutAttributes()
                            + "] lagging at version [*] despite commit of cluster state version [*]*"
                    )
                );

                // drop the publication messages to one node, but then restore connectivity so it remains in the cluster and does not fail
                // health checks
                brokenNode.blackhole();
                cluster.deterministicTaskQueue.scheduleAt(
                    cluster.deterministicTaskQueue.getCurrentTimeMillis() + DEFAULT_CLUSTER_STATE_UPDATE_DELAY,
                    new Runnable() {
                        @Override
                        public void run() {
                            brokenNode.heal();
                        }

                        @Override
                        public String toString() {
                            return "healing " + brokenNode;
                        }
                    }
                );
                cluster.getAnyLeader().submitValue(randomLong());
                cluster.runFor(
                    defaultMillis(PUBLISH_TIMEOUT_SETTING) + 2 * DEFAULT_DELAY_VARIABILITY + defaultMillis(
                        LagDetector.CLUSTER_FOLLOWER_LAG_TIMEOUT_SETTING
                    ) + DEFAULT_DELAY_VARIABILITY + 2 * DEFAULT_DELAY_VARIABILITY,
                    "waiting for messages to be emitted"
                );

                mockLogAppender.assertAllExpectationsMatched();
            }
        }
    }

    public void testDoesNotPerformElectionWhenRestartingFollower() {
        try (Cluster cluster = new Cluster(randomIntBetween(2, 5), false, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();

            final ClusterNode leader = cluster.getAnyLeader();
            final long expectedTerm = leader.coordinator.getCurrentTerm();

            if (cluster.clusterNodes.stream().filter(n -> n.getLocalNode().isMasterNode()).count() == 2) {
                // in the 2-node case, auto-shrinking the voting configuration is required to reduce the voting configuration down to just
                // the leader, otherwise restarting the other master-eligible node triggers an election
                leader.submitSetAutoShrinkVotingConfiguration(true);
                cluster.stabilise(2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY); // 1st delay for the setting update, 2nd for the reconfiguration
            }

            for (final ClusterNode clusterNode : cluster.getAllNodesExcept(leader)) {
                logger.info("--> restarting {}", clusterNode);
                clusterNode.close();
                cluster.clusterNodes.replaceAll(
                    cn -> cn == clusterNode ? cn.restartedNode(Function.identity(), Function.identity(), Settings.EMPTY) : cn
                );
                cluster.stabilise();
                assertThat("term should not change", cluster.getAnyNode().coordinator.getCurrentTerm(), is(expectedTerm));
            }
        }
    }

    public void testImproveConfigurationPerformsVotingConfigExclusionStateCheck() {
        try (Cluster cluster = new Cluster(1)) {
            cluster.runRandomly();
            cluster.stabilise();

            final Coordinator coordinator = cluster.getAnyLeader().coordinator;
            final ClusterState currentState = coordinator.getLastAcceptedState();

            Set<CoordinationMetadata.VotingConfigExclusion> newVotingConfigExclusion1 = new HashSet<>() {
                {
                    add(
                        new CoordinationMetadata.VotingConfigExclusion(
                            "resolvableNodeId",
                            CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER
                        )
                    );
                }
            };

            ClusterState newState1 = buildNewClusterStateWithVotingConfigExclusion(currentState, newVotingConfigExclusion1);

            assertFalse(Coordinator.validVotingConfigExclusionState(newState1));

            Set<CoordinationMetadata.VotingConfigExclusion> newVotingConfigExclusion2 = new HashSet<>() {
                {
                    add(
                        new CoordinationMetadata.VotingConfigExclusion(
                            CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER,
                            "resolvableNodeName"
                        )
                    );
                }
            };

            ClusterState newState2 = buildNewClusterStateWithVotingConfigExclusion(currentState, newVotingConfigExclusion2);

            assertFalse(Coordinator.validVotingConfigExclusionState(newState2));
        }
    }

    public void testPeerFinderListener() throws Exception {
        try (Cluster cluster = new Cluster(3, true, Settings.EMPTY)) {
            cluster.runRandomly();
            cluster.stabilise();
            ClusterNode leader = cluster.getAnyLeader();
            ClusterNode nodeWithListener = cluster.getAnyNodeExcept(leader);
            AtomicBoolean listenerCalled = new AtomicBoolean(false);
            nodeWithListener.coordinator.addPeerFinderListener(() -> listenerCalled.set(true));
            assertFalse(listenerCalled.get());
            leader.disconnect();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "Letting disconnect take effect");
            cluster.stabilise();
            assertTrue(cluster.clusterNodes.contains(nodeWithListener));
            assertBusy(() -> assertTrue(listenerCalled.get()));
        }
    }

    private ClusterState buildNewClusterStateWithVotingConfigExclusion(
        ClusterState currentState,
        Set<CoordinationMetadata.VotingConfigExclusion> newVotingConfigExclusion
    ) {
        DiscoveryNodes newNodes = DiscoveryNodes.builder(currentState.nodes())
            .add(
                TestDiscoveryNode.create(
                    "resolvableNodeName",
                    "resolvableNodeId",
                    buildNewFakeTransportAddress(),
                    emptyMap(),
                    Set.of(DiscoveryNodeRole.MASTER_ROLE)
                )
            )
            .build();

        CoordinationMetadata.Builder coordMetadataBuilder = CoordinationMetadata.builder(currentState.coordinationMetadata());
        newVotingConfigExclusion.forEach(coordMetadataBuilder::addVotingConfigExclusion);
        Metadata newMetadata = Metadata.builder(currentState.metadata()).coordinationMetadata(coordMetadataBuilder.build()).build();

        return ClusterState.builder(currentState).nodes(newNodes).metadata(newMetadata).build();
    }

}
