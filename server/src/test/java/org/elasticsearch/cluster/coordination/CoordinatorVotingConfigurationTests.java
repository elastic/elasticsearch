/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.discovery.DiscoveryModule;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.DEFAULT_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.AbstractCoordinatorTestCase.Cluster.EXTREME_DELAY_VARIABILITY;
import static org.elasticsearch.cluster.coordination.Coordinator.Mode.CANDIDATE;
import static org.elasticsearch.cluster.coordination.ElectionSchedulerFactory.ELECTION_INITIAL_TIMEOUT_SETTING;
import static org.elasticsearch.cluster.coordination.Reconfigurator.CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION;
import static org.elasticsearch.discovery.PeerFinder.DISCOVERY_FIND_PEERS_INTERVAL_SETTING;
import static org.elasticsearch.test.NodeRoles.nonMasterNode;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

@TestLogging(reason = "these tests do a lot of log-worthy things but we usually don't care", value = "org.elasticsearch:FATAL")
public class CoordinatorVotingConfigurationTests extends AbstractCoordinatorTestCase {
    public void testExpandsConfigurationWhenGrowingFromThreeToFiveNodesAndShrinksBackToThreeOnFailure() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final Cluster.ClusterNode leader = cluster.getAnyLeader();

            logger.info("setting auto-shrink reconfiguration to true");
            leader.submitSetAutoShrinkVotingConfiguration(true);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
            assertTrue(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metadata().settings()));

            cluster.addNodesAndStabilise(2);

            {
                assertThat(leader.coordinator.getMode(), is(Coordinator.Mode.LEADER));
                final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(
                    lastCommittedConfiguration + " should be all nodes",
                    lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(Cluster.ClusterNode::getId).collect(Collectors.toSet()))
                );
            }

            final Cluster.ClusterNode disconnect1 = cluster.getAnyNode();
            final Cluster.ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);
            logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
            disconnect1.disconnect();
            disconnect2.disconnect();
            cluster.stabilise();

            {
                final Cluster.ClusterNode newLeader = cluster.getAnyLeader();
                final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration = newLeader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
            }

            // we still tolerate the loss of one more node here

            final Cluster.ClusterNode disconnect3 = cluster.getAnyNodeExcept(disconnect1, disconnect2);
            logger.info("--> disconnecting {}", disconnect3);
            disconnect3.disconnect();
            cluster.stabilise();

            {
                final Cluster.ClusterNode newLeader = cluster.getAnyLeader();
                final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration = newLeader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
                assertTrue(lastCommittedConfiguration.getNodeIds().contains(disconnect3.getId()));
            }

            // however we do not tolerate the loss of yet another one

            final Cluster.ClusterNode disconnect4 = cluster.getAnyNodeExcept(disconnect1, disconnect2, disconnect3);
            logger.info("--> disconnecting {}", disconnect4);
            disconnect4.disconnect();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final Cluster.ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(
                    clusterNode.getId() + " should be a candidate",
                    clusterNode.coordinator.getMode(),
                    equalTo(Coordinator.Mode.CANDIDATE)
                );
            }

            // moreover we are still stuck even if two other nodes heal
            logger.info("--> healing {} and {}", disconnect1, disconnect2);
            disconnect1.heal();
            disconnect2.heal();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final Cluster.ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(
                    clusterNode.getId() + " should be a candidate",
                    clusterNode.coordinator.getMode(),
                    equalTo(Coordinator.Mode.CANDIDATE)
                );
            }

            // we require another node to heal to recover
            final Cluster.ClusterNode toHeal = randomBoolean() ? disconnect3 : disconnect4;
            logger.info("--> healing {}", toHeal);
            toHeal.heal();
            cluster.stabilise();
        }
    }

    public void testDoesNotShrinkConfigurationBelowThreeNodes() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final Cluster.ClusterNode disconnect1 = cluster.getAnyNode();

            logger.info("--> disconnecting {}", disconnect1);
            disconnect1.disconnect();
            cluster.stabilise();

            final Cluster.ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);
            logger.info("--> disconnecting {}", disconnect2);
            disconnect2.disconnect();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final Cluster.ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(
                    clusterNode.getId() + " should be a candidate",
                    clusterNode.coordinator.getMode(),
                    equalTo(Coordinator.Mode.CANDIDATE)
                );
            }

            disconnect1.heal();
            cluster.stabilise(); // would not work if disconnect1 were removed from the configuration
        }
    }

    public void testCanShrinkFromFiveNodesToThree() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.runRandomly();
            cluster.stabilise();

            {
                final Cluster.ClusterNode leader = cluster.getAnyLeader();
                logger.info("setting auto-shrink reconfiguration to false");
                leader.submitSetAutoShrinkVotingConfiguration(false);
                cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY);
                assertFalse(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metadata().settings()));
            }

            final Cluster.ClusterNode disconnect1 = cluster.getAnyNode();
            final Cluster.ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);

            logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
            disconnect1.disconnect();
            disconnect2.disconnect();
            cluster.stabilise();

            final Cluster.ClusterNode leader = cluster.getAnyLeader();

            {
                final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(
                    lastCommittedConfiguration + " should be all nodes",
                    lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(Cluster.ClusterNode::getId).collect(Collectors.toSet()))
                );
            }

            logger.info("setting auto-shrink reconfiguration to true");
            leader.submitSetAutoShrinkVotingConfiguration(true);
            cluster.stabilise(DEFAULT_CLUSTER_STATE_UPDATE_DELAY * 2); // allow for a reconfiguration
            assertTrue(CLUSTER_AUTO_SHRINK_VOTING_CONFIGURATION.get(leader.getLastAppliedClusterState().metadata().settings()));

            {
                final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(lastCommittedConfiguration + " should be 3 nodes", lastCommittedConfiguration.getNodeIds().size(), equalTo(3));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect1.getId()));
                assertFalse(lastCommittedConfiguration.getNodeIds().contains(disconnect2.getId()));
            }
        }
    }

    public void testDoesNotShrinkConfigurationBelowFiveNodesIfAutoShrinkDisabled() {
        try (Cluster cluster = new Cluster(5)) {
            cluster.runRandomly();
            cluster.stabilise();

            cluster.getAnyLeader().submitSetAutoShrinkVotingConfiguration(false);
            cluster.stabilise(DEFAULT_ELECTION_DELAY);

            final Cluster.ClusterNode disconnect1 = cluster.getAnyNode();
            final Cluster.ClusterNode disconnect2 = cluster.getAnyNodeExcept(disconnect1);

            logger.info("--> disconnecting {} and {}", disconnect1, disconnect2);
            disconnect1.disconnect();
            disconnect2.disconnect();
            cluster.stabilise();

            final Cluster.ClusterNode disconnect3 = cluster.getAnyNodeExcept(disconnect1, disconnect2);
            logger.info("--> disconnecting {}", disconnect3);
            disconnect3.disconnect();
            cluster.runFor(DEFAULT_STABILISATION_TIME, "allowing time for fault detection");

            for (final Cluster.ClusterNode clusterNode : cluster.clusterNodes) {
                assertThat(
                    clusterNode.getId() + " should be a candidate",
                    clusterNode.coordinator.getMode(),
                    equalTo(Coordinator.Mode.CANDIDATE)
                );
            }

            disconnect1.heal();
            cluster.stabilise(); // would not work if disconnect1 were removed from the configuration
        }
    }

    public void testExpandsConfigurationWhenGrowingFromOneNodeToThreeButDoesNotShrink() {
        try (Cluster cluster = new Cluster(1)) {
            cluster.runRandomly();
            cluster.stabilise();

            final Cluster.ClusterNode leader = cluster.getAnyLeader();

            cluster.addNodesAndStabilise(2);

            {
                assertThat(leader.coordinator.getMode(), is(Coordinator.Mode.LEADER));
                final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration = leader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(
                    lastCommittedConfiguration + " should be all nodes",
                    lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(Cluster.ClusterNode::getId).collect(Collectors.toSet()))
                );
            }

            final Cluster.ClusterNode disconnect1 = cluster.getAnyNode();
            logger.info("--> disconnecting {}", disconnect1);
            disconnect1.disconnect();
            cluster.stabilise();

            {
                final Cluster.ClusterNode newLeader = cluster.getAnyLeader();
                final CoordinationMetadata.VotingConfiguration lastCommittedConfiguration = newLeader.getLastAppliedClusterState()
                    .getLastCommittedConfiguration();
                assertThat(
                    lastCommittedConfiguration + " should be all nodes",
                    lastCommittedConfiguration.getNodeIds(),
                    equalTo(cluster.clusterNodes.stream().map(Cluster.ClusterNode::getId).collect(Collectors.toSet()))
                );
            }
        }
    }

    public void testSingleNodeDiscoveryWithoutQuorum() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final Cluster.ClusterNode clusterNode = cluster.getAnyNode();
            logger.debug("rebooting [{}]", clusterNode.getId());
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
            IllegalStateException ise = expectThrows(
                IllegalStateException.class,
                () -> cluster.clusterNodes.replaceAll(
                    cn -> cn == clusterNode
                        ? cn.restartedNode(
                            Function.identity(),
                            Function.identity(),
                            Settings.builder()
                                .put(DiscoveryModule.DISCOVERY_TYPE_SETTING.getKey(), DiscoveryModule.SINGLE_NODE_DISCOVERY_TYPE)
                                .build()
                        )
                        : cn
                )
            );
            assertThat(
                ise.getMessage(),
                allOf(
                    containsString("cannot start with [discovery.type] set to [single-node] when local node"),
                    containsString("does not have quorum in voting configuration")
                )
            );

            cluster.clusterNodes.remove(clusterNode); // to avoid closing it twice
        }
    }

    public void testReconfiguresToExcludeMasterIneligibleNodesInVotingConfig() {
        try (Cluster cluster = new Cluster(3)) {
            cluster.runRandomly();
            cluster.stabilise();

            final Cluster.ClusterNode chosenNode = cluster.getAnyNode();

            assertThat(
                cluster.getAnyLeader().getLastAppliedClusterState().getLastCommittedConfiguration().getNodeIds(),
                hasItem(chosenNode.getId())
            );
            assertThat(
                cluster.getAnyLeader().getLastAppliedClusterState().getLastAcceptedConfiguration().getNodeIds(),
                hasItem(chosenNode.getId())
            );

            final boolean chosenNodeIsLeader = chosenNode == cluster.getAnyLeader();
            final long termBeforeRestart = cluster.getAnyNode().coordinator.getCurrentTerm();

            logger.info("--> restarting [{}] as a master-ineligible node", chosenNode);

            chosenNode.close();
            cluster.clusterNodes.replaceAll(
                cn -> cn == chosenNode ? cn.restartedNode(Function.identity(), Function.identity(), nonMasterNode()) : cn
            );
            cluster.stabilise();

            if (chosenNodeIsLeader == false) {
                assertThat("term did not change", cluster.getAnyNode().coordinator.getCurrentTerm(), is(termBeforeRestart));
            }

            assertThat(
                cluster.getAnyLeader().getLastAppliedClusterState().getLastCommittedConfiguration().getNodeIds(),
                not(hasItem(chosenNode.getId()))
            );
            assertThat(
                cluster.getAnyLeader().getLastAppliedClusterState().getLastAcceptedConfiguration().getNodeIds(),
                not(hasItem(chosenNode.getId()))
            );
        }
    }

    public void testCannotSetInitialConfigurationWithoutLocalNode() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            final Coordinator coordinator = cluster.getAnyNode().coordinator;
            final CoordinationMetadata.VotingConfiguration unknownNodeConfiguration = new CoordinationMetadata.VotingConfiguration(
                Sets.newHashSet("unknown-node")
            );
            final String exceptionMessage = expectThrows(
                CoordinationStateRejectedException.class,
                () -> coordinator.setInitialConfiguration(unknownNodeConfiguration)
            ).getMessage();
            assertThat(exceptionMessage, equalTo("local node is not part of initial configuration"));
        }
    }

    public void testCannotSetInitialConfigurationWithoutQuorum() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            final Coordinator coordinator = cluster.getAnyNode().coordinator;
            final CoordinationMetadata.VotingConfiguration unknownNodeConfiguration = new CoordinationMetadata.VotingConfiguration(
                Sets.newHashSet(coordinator.getLocalNode().getId(), "unknown-node")
            );
            final String exceptionMessage = expectThrows(
                CoordinationStateRejectedException.class,
                () -> coordinator.setInitialConfiguration(unknownNodeConfiguration)
            ).getMessage();
            assertThat(
                exceptionMessage,
                startsWith("not enough nodes discovered to form a quorum in the initial configuration [knownNodes=[")
            );
            assertThat(exceptionMessage, containsString("unknown-node"));
            assertThat(exceptionMessage, containsString(coordinator.getLocalNode().toString()));

            // This is VERY BAD: setting a _different_ initial configuration. Yet it works if the first attempt will never be a quorum.
            assertTrue(coordinator.setInitialConfiguration(CoordinationMetadata.VotingConfiguration.of(coordinator.getLocalNode())));
            cluster.stabilise();
        }
    }

    public void testSettingInitialConfigurationTriggersElection() {
        try (Cluster cluster = new Cluster(randomIntBetween(1, 5))) {
            cluster.runFor(
                defaultMillis(DISCOVERY_FIND_PEERS_INTERVAL_SETTING) * 2 + randomLongBetween(0, 60000),
                "initial discovery phase"
            );
            for (final Cluster.ClusterNode clusterNode : cluster.clusterNodes) {
                final String nodeId = clusterNode.getId();
                assertThat(nodeId + " is CANDIDATE", clusterNode.coordinator.getMode(), is(CANDIDATE));
                assertThat(nodeId + " is in term 0", clusterNode.coordinator.getCurrentTerm(), is(0L));
                assertThat(nodeId + " last accepted in term 0", clusterNode.coordinator.getLastAcceptedState().term(), is(0L));
                assertThat(nodeId + " last accepted version 0", clusterNode.coordinator.getLastAcceptedState().version(), is(0L));
                assertFalse(nodeId + " has not received an initial configuration", clusterNode.coordinator.isInitialConfigurationSet());
                assertTrue(
                    nodeId + " has an empty last-accepted configuration",
                    clusterNode.coordinator.getLastAcceptedState().getLastAcceptedConfiguration().isEmpty()
                );
                assertTrue(
                    nodeId + " has an empty last-committed configuration",
                    clusterNode.coordinator.getLastAcceptedState().getLastCommittedConfiguration().isEmpty()
                );

                final Set<DiscoveryNode> foundPeers = new HashSet<>();
                clusterNode.coordinator.getFoundPeers().forEach(foundPeers::add);
                assertTrue(nodeId + " should not have discovered itself", foundPeers.add(clusterNode.getLocalNode()));
                assertThat(nodeId + " should have found all peers", foundPeers, hasSize(cluster.size()));
            }

            final Cluster.ClusterNode bootstrapNode = cluster.getAnyBootstrappableNode();
            bootstrapNode.applyInitialConfiguration();
            assertTrue(bootstrapNode.getId() + " has been bootstrapped", bootstrapNode.coordinator.isInitialConfigurationSet());

            cluster.stabilise(
                // the first election should succeed, because only one node knows of the initial configuration and therefore can win a
                // pre-voting round and proceed to an election, so there cannot be any collisions
                defaultMillis(ELECTION_INITIAL_TIMEOUT_SETTING)
                    // Allow two round-trip for pre-voting and voting
                    + 4 * DEFAULT_DELAY_VARIABILITY
                    // Then a commit of the new leader's first cluster state
                    + DEFAULT_CLUSTER_STATE_UPDATE_DELAY
                    // Then allow time for all the other nodes to join, each of which might cause a reconfiguration
                    + (cluster.size() - 1) * 2 * DEFAULT_CLUSTER_STATE_UPDATE_DELAY
            );
        }
    }

    @TestLogging(
        reason = "test includes assertions about ClusterBootstrapService logging",
        value = "org.elasticsearch.cluster.coordination.ClusterBootstrapService:INFO"
    )
    public void testClusterUUIDLogging() {
        final var mockAppender = new MockLogAppender();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "fresh node message",
                ClusterBootstrapService.class.getCanonicalName(),
                Level.INFO,
                "this node has not joined a bootstrapped cluster yet; [cluster.initial_master_nodes] is set to []"
            )
        );
        try (var ignored = mockAppender.capturing(ClusterBootstrapService.class); var cluster = new Cluster(randomIntBetween(1, 3))) {
            cluster.runRandomly();
            cluster.stabilise();
            mockAppender.assertAllExpectationsMatched();

            final var restartingNode = cluster.getAnyNode();
            mockAppender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "restarted node message",
                    ClusterBootstrapService.class.getCanonicalName(),
                    Level.INFO,
                    "this node is locked into cluster UUID ["
                        + restartingNode.getLastAppliedClusterState().metadata().clusterUUID()
                        + "] and will not attempt further cluster bootstrapping"
                )
            );
            restartingNode.close();
            cluster.clusterNodes.replaceAll(cn -> cn == restartingNode ? cn.restartedNode() : cn);
            cluster.stabilise();
            mockAppender.assertAllExpectationsMatched();
        }
    }

    public void testDiscoveryUsesNodesFromLastClusterState() {

        // This test only applies when using proper majority-based voting configurations. If we permit non-overlapping configurations then
        // the isolated node can form a one-node cluster when it is healed, and the other nodes won't know be able to discover it without a
        // proper discovery config since it wasn't in their last cluster states.
        //
        // (if we wanted this to work without a proper discovery config then we could e.g. record the master's publish address in the
        // heartbeat blob, but we can assume that discovery is properly configured when using an atomic-register coordinator, and therefore
        // there's no need for this extra complexity)

        try (Cluster cluster = new Cluster(randomIntBetween(3, 5))) {
            cluster.runRandomly();
            cluster.stabilise();

            final Cluster.ClusterNode partitionedNode = cluster.getAnyNode();
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
            cluster.runRandomly(false, true, EXTREME_DELAY_VARIABILITY);
            cluster.stabilise();
        }
    }
}
