/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.BOOTSTRAP_PLACEHOLDER_PREFIX;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.monitor.StatusInfo.Status.HEALTHY;
import static org.elasticsearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ClusterFormationFailureHelperTests extends ESTestCase {

    private static final ElectionStrategy electionStrategy = ElectionStrategy.DEFAULT_INSTANCE;

    public void testScheduling() {
        final long expectedDelayMillis;
        final Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            expectedDelayMillis
                = ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(Settings.EMPTY).millis();
        } else {
            expectedDelayMillis = randomLongBetween(100, 100000);
            settingsBuilder.put(ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.getKey(),
                expectedDelayMillis + "ms");
        }

        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId())).build();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final AtomicLong warningCount = new AtomicLong();
        final AtomicLong logLastFailedJoinAttemptWarningCount = new AtomicLong();

        final ClusterFormationFailureHelper clusterFormationFailureHelper = new ClusterFormationFailureHelper(settingsBuilder.build(),
            () -> {
                warningCount.incrementAndGet();
                return new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList(), 0L, electionStrategy,
                    new StatusInfo(HEALTHY, "healthy-info"));
            },
            deterministicTaskQueue.getThreadPool(), logLastFailedJoinAttemptWarningCount::incrementAndGet);

        deterministicTaskQueue.runAllTasks();
        assertThat("should not schedule anything yet", warningCount.get(), is(0L));

        final long startTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
        clusterFormationFailureHelper.start();

        while (warningCount.get() == 0) {
            assertTrue(clusterFormationFailureHelper.isRunning());
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(warningCount.get(), is(1L));
        assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, is(expectedDelayMillis));

        while (warningCount.get() < 5) {
            assertTrue(clusterFormationFailureHelper.isRunning());
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, equalTo(5 * expectedDelayMillis));

        clusterFormationFailureHelper.stop();
        assertFalse(clusterFormationFailureHelper.isRunning());
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertThat(warningCount.get(), is(5L));
        assertThat(logLastFailedJoinAttemptWarningCount.get(), is(5L));

        warningCount.set(0);
        logLastFailedJoinAttemptWarningCount.set(0);
        clusterFormationFailureHelper.start();
        clusterFormationFailureHelper.stop();
        clusterFormationFailureHelper.start();
        final long secondStartTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();

        while (warningCount.get() < 5) {
            assertTrue(clusterFormationFailureHelper.isRunning());
            if (deterministicTaskQueue.hasRunnableTasks()) {
                deterministicTaskQueue.runRandomTask();
            } else {
                deterministicTaskQueue.advanceTime();
            }
        }
        assertThat(deterministicTaskQueue.getCurrentTimeMillis() - secondStartTimeMillis, equalTo(5 * expectedDelayMillis));

        clusterFormationFailureHelper.stop();
        assertFalse(clusterFormationFailureHelper.isRunning());
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertThat(warningCount.get(), is(5L));
        assertThat(logLastFailedJoinAttemptWarningCount.get(), is(5L));
    }

    public void testDescriptionOnMasterIneligibleNodes() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L).nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId())).build();

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList(), 15L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"))
                .getDescription(),
            is("master not discovered yet: have discovered []; discovery will continue using [] from hosts providers " +
                "and [] from last-known cluster state; node term 15, last-accepted version 12 in term 0"));

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, singletonList(otherAddress), emptyList(), 16L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"))
                .getDescription(),
            is("master not discovered yet: have discovered []; discovery will continue using [" + otherAddress +
                "] from hosts providers and [] from last-known cluster state; node term 16, last-accepted version 12 in term 0"));

        final DiscoveryNode otherNode = makeDiscoveryNode("other");
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(otherNode), 17L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"))
                .getDescription(),
            is("master not discovered yet: have discovered [" + noAttr(otherNode) + "]; discovery will continue using [] from hosts " +
                "providers and [] from last-known cluster state; node term 17, last-accepted version 12 in term 0"));
    }

    public void testDescriptionOnUnhealthyNodes() {
        final DiscoveryNode dataNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L).nodes(DiscoveryNodes.builder().add(dataNode).localNodeId(dataNode.getId())).build();

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList(), 15L, electionStrategy,
                new StatusInfo(UNHEALTHY, "unhealthy-info"))
                .getDescription(),
            is("this node is unhealthy: unhealthy-info"));

        final DiscoveryNode masterNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE), Version.CURRENT);
        clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L).nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId())).build();

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList(), 15L, electionStrategy,
                new StatusInfo(UNHEALTHY, "unhealthy-info"))
                .getDescription(),
            is("this node is unhealthy: unhealthy-info"));
    }

    public void testDescriptionBeforeBootstrapping() {
        final DiscoveryNode localNode = makeDiscoveryNode("local");
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(7L)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(4L).build()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId())).build();

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList(), 1L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped cluster, and " +
                "[cluster.initial_master_nodes] is empty on this node: have discovered []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 1, last-accepted version 7 in term 4"));

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, singletonList(otherAddress), emptyList(), 2L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"))
                .getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped cluster, and " +
                "[cluster.initial_master_nodes] is empty on this node: have discovered []; " +
                "discovery will continue using [" + otherAddress + "] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 2, last-accepted version 7 in term 4"));

        final DiscoveryNode otherNode = makeDiscoveryNode("other");
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(otherNode), 3L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"))
                .getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped cluster, and " +
                "[cluster.initial_master_nodes] is empty on this node: have discovered [" + noAttr(otherNode) + "]; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 3, last-accepted version 7 in term 4"));

        assertThat(new ClusterFormationState(Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "other").build(),
                clusterState, emptyList(), emptyList(), 4L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped cluster, and " +
                "this node must discover master-eligible nodes [other] to bootstrap a cluster: have discovered []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 4, last-accepted version 7 in term 4"));
    }

    private static VotingConfiguration config(String[] nodeIds) {
        return new VotingConfiguration(Arrays.stream(nodeIds).collect(Collectors.toSet()));
    }

    private static ClusterState state(DiscoveryNode localNode, String... configNodeIds) {
        return state(localNode, configNodeIds, configNodeIds);
    }

    private static ClusterState state(DiscoveryNode localNode, String[] acceptedConfig, String[] committedConfig) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder()
                .lastAcceptedConfiguration(config(acceptedConfig))
                .lastCommittedConfiguration(config(committedConfig)).build())).build();
    }


    public void testDescriptionAfterDetachCluster() {
        final DiscoveryNode localNode = makeDiscoveryNode("local");

        final ClusterState clusterState = state(localNode,
                VotingConfiguration.MUST_JOIN_ELECTED_MASTER.getNodeIds().toArray(new String[0]));

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList(), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
                is("master not discovered yet and this node was detached from its previous cluster, " +
                        "have discovered []; " +
                        "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                        "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, singletonList(otherAddress), emptyList(), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
                is("master not discovered yet and this node was detached from its previous cluster, " +
                        "have discovered []; " +
                        "discovery will continue using [" + otherAddress + "] from hosts providers and [" + noAttr(localNode) +
                        "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        final DiscoveryNode otherNode = makeDiscoveryNode("otherNode");
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(otherNode), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
                is("master not discovered yet and this node was detached from its previous cluster, " +
                        "have discovered [" + noAttr(otherNode) + "]; " +
                        "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                        "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        final DiscoveryNode yetAnotherNode = makeDiscoveryNode("yetAnotherNode");
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(yetAnotherNode), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
                is("master not discovered yet and this node was detached from its previous cluster, " +
                        "have discovered [" + noAttr(yetAnotherNode) + "]; " +
                        "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                        "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

    }

    public void testDescriptionAfterBootstrapping() {
        final DiscoveryNode localNode = makeDiscoveryNode("local");

        final ClusterState clusterState = state(localNode, "otherNode");

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList(), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, singletonList(otherAddress), emptyList(), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [" + otherAddress + "] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        final DiscoveryNode otherNode = makeDiscoveryNode("otherNode");
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(otherNode), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have discovered possible quorum [" + noAttr(otherNode) + "]; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        final DiscoveryNode yetAnotherNode = makeDiscoveryNode("yetAnotherNode");
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(yetAnotherNode), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have only discovered non-quorum [" + noAttr(yetAnotherNode) + "]; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2"), emptyList(), emptyList(), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires two nodes with ids [n1, n2], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3"), emptyList(), emptyList(), 0L,
                electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires at least 2 nodes with ids from [n1, n2, n3], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", BOOTSTRAP_PLACEHOLDER_PREFIX + "n3"),
                emptyList(), emptyList(), 0L, electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires 2 nodes with ids [n1, n2], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3", "n4"), emptyList(), emptyList(), 0L,
                electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3", "n4", "n5"), emptyList(), emptyList(), 0L,
                electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4, n5], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3", "n4", BOOTSTRAP_PLACEHOLDER_PREFIX + "n5"),
                emptyList(), emptyList(), 0L, electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3",
            BOOTSTRAP_PLACEHOLDER_PREFIX + "n4", BOOTSTRAP_PLACEHOLDER_PREFIX + "n5"), emptyList(), emptyList(), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires 3 nodes with ids [n1, n2, n3], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n1"}), emptyList(),
                emptyList(), 0L, electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n2"}), emptyList(),
                emptyList(), 0L, electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1] and a node with id [n2], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n2", "n3"}), emptyList(),
                emptyList(), 0L, electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1] and two nodes with ids [n2, n3], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n2", "n3", "n4"}),
                emptyList(), emptyList(), 0L, electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1] and " +
                "at least 2 nodes with ids from [n2, n3, n4], " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

        final DiscoveryNode otherMasterNode = makeDiscoveryNode("other-master");
        final DiscoveryNode otherNonMasterNode = new DiscoveryNode("other-non-master", buildNewFakeTransportAddress(), emptyMap(),
            new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()).stream()
                .filter(r -> r != DiscoveryNodeRole.MASTER_ROLE).collect(Collectors.toList())),
            Version.CURRENT);

        String[] configNodeIds = new String[]{"n1", "n2"};
        final ClusterState stateWithOtherNodes = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).add(otherMasterNode).add(otherNonMasterNode))
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder()
                .lastAcceptedConfiguration(config(configNodeIds))
                .lastCommittedConfiguration(config(configNodeIds)).build())).build();

        assertThat(
            new ClusterFormationState(Settings.EMPTY, stateWithOtherNodes, emptyList(), emptyList(), 0L, electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info")).getDescription(),

            // nodes from last-known cluster state could be in either order
            is(oneOf(
                "master not discovered or elected yet, an election requires two nodes with ids [n1, n2], " +
                    "have only discovered non-quorum []; " +
                    "discovery will continue using [] from hosts providers and [" + noAttr(localNode) + ", " + noAttr(otherMasterNode) +
                    "] from last-known cluster state; node term 0, last-accepted version 0 in term 0",

                "master not discovered or elected yet, an election requires two nodes with ids [n1, n2], " +
                    "have only discovered non-quorum []; " +
                    "discovery will continue using [] from hosts providers and [" + noAttr(otherMasterNode) + ", " + noAttr(localNode) +
                    "] from last-known cluster state; node term 0, last-accepted version 0 in term 0")));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, GatewayMetaState.STALE_STATE_CONFIG_NODE_ID), emptyList(),
                emptyList(), 0L, electionStrategy, new StatusInfo(HEALTHY, "healthy-info")).getDescription(),
            is("master not discovered or elected yet, an election requires one or more nodes that have already participated as " +
                "master-eligible nodes in the cluster but this node was not master-eligible the last time it joined the cluster, " +
                "have only discovered non-quorum []; " +
                "discovery will continue using [] from hosts providers and [" + noAttr(localNode) +
                "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"));

    }

    private static DiscoveryNode makeDiscoveryNode(String nodeId) {
        final Map<String,String> attributes = new HashMap<>();
        if (randomBoolean()) {
            attributes.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return new DiscoveryNode(nodeId, buildNewFakeTransportAddress(), attributes, DiscoveryNodeRole.roles(), Version.CURRENT);
    }

    private static String noAttr(DiscoveryNode discoveryNode) {
        final StringBuilder stringBuilder = new StringBuilder();
        discoveryNode.appendDescriptionWithoutAttributes(stringBuilder);
        return stringBuilder.toString();
    }
}
