/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayMetaState;
import org.elasticsearch.monitor.StatusInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.MockLogAppender;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
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

    // Hard-coding the class name here because it is also mentioned in the troubleshooting docs, so should not be renamed without care.
    private static final String LOGGER_NAME = "org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper";

    public void testScheduling() {
        final long expectedDelayMillis;
        final Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            expectedDelayMillis = ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.get(Settings.EMPTY)
                .millis();
        } else {
            expectedDelayMillis = randomLongBetween(100, 100000);
            settingsBuilder.put(
                ClusterFormationFailureHelper.DISCOVERY_CLUSTER_FORMATION_WARNING_TIMEOUT_SETTING.getKey(),
                expectedDelayMillis + "ms"
            );
        }

        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local").roles(emptySet()).build();
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final AtomicLong warningCount = new AtomicLong();
        final AtomicLong logLastFailedJoinAttemptWarningCount = new AtomicLong();

        final ClusterFormationFailureHelper clusterFormationFailureHelper = new ClusterFormationFailureHelper(
            settingsBuilder.build(),
            () -> {
                warningCount.incrementAndGet();
                return new ClusterFormationState(
                    Settings.EMPTY,
                    clusterState,
                    emptyList(),
                    emptyList(),
                    0L,
                    electionStrategy,
                    new StatusInfo(HEALTHY, "healthy-info"),
                    emptyList()
                );
            },
            deterministicTaskQueue.getThreadPool(),
            logLastFailedJoinAttemptWarningCount::incrementAndGet
        );

        deterministicTaskQueue.runAllTasks();
        assertThat("should not schedule anything yet", warningCount.get(), is(0L));

        final long startTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
        clusterFormationFailureHelper.start();

        var mockLogAppender = new MockLogAppender();
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation("master not discovered", LOGGER_NAME, Level.WARN, "master not discovered")
        );
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "troubleshooting link",
                LOGGER_NAME,
                Level.WARN,
                "* for troubleshooting guidance, see "
                    + "https://www.elastic.co/guide/en/elasticsearch/reference/*/discovery-troubleshooting.html*"
            )
        );
        try (var ignored = mockLogAppender.capturing(ClusterFormationFailureHelper.class)) {
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
            mockLogAppender.assertAllExpectationsMatched();
        }

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
        final DiscoveryNode localNode = DiscoveryNodeUtils.builder("local").roles(emptySet()).build();
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                15L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet: have discovered []; discovery will continue using [] from hosts providers "
                    + "and [] from last-known cluster state; node term 15, last-accepted version 12 in term 0"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                16L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet: have discovered []; discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and [] from last-known cluster state; node term 16, last-accepted version 12 in term 0"
            )
        );

        final DiscoveryNode otherNode = makeDiscoveryNode("other");
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                17L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet: have discovered ["
                    + noAttr(otherNode)
                    + "]; discovery will continue using [] from hosts "
                    + "providers and [] from last-known cluster state; node term 17, last-accepted version 12 in term 0"
            )
        );
    }

    public void testDescriptionOnUnhealthyNodes() {
        final DiscoveryNode dataNode = DiscoveryNodeUtils.builder("local").roles(emptySet()).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L)
            .nodes(DiscoveryNodes.builder().add(dataNode).localNodeId(dataNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                15L,
                electionStrategy,
                new StatusInfo(UNHEALTHY, "unhealthy-info"),
                emptyList()
            ).getDescription(),
            is("this node is unhealthy: unhealthy-info")
        );

        final DiscoveryNode masterNode = DiscoveryNodeUtils.builder("local").roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(12L)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                15L,
                electionStrategy,
                new StatusInfo(UNHEALTHY, "unhealthy-info"),
                emptyList()
            ).getDescription(),
            is("this node is unhealthy: unhealthy-info")
        );
    }

    public void testDescriptionBeforeBootstrapping() {
        final DiscoveryNode localNode = makeDiscoveryNode("local");
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .version(7L)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(4L).build()))
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                1L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "[cluster.initial_master_nodes] is empty on this node: have discovered []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 1, last-accepted version 7 in term 4"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                2L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "[cluster.initial_master_nodes] is empty on this node: have discovered []; "
                    + "discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 2, last-accepted version 7 in term 4"
            )
        );

        final DiscoveryNode otherNode = makeDiscoveryNode("other");
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                3L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "[cluster.initial_master_nodes] is empty on this node: have discovered ["
                    + noAttr(otherNode)
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 3, last-accepted version 7 in term 4"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "other").build(),
                clusterState,
                emptyList(),
                emptyList(),
                4L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet, this node has not previously joined a bootstrapped cluster, and "
                    + "this node must discover master-eligible nodes [other] to bootstrap a cluster: have discovered []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 4, last-accepted version 7 in term 4"
            )
        );
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
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastAcceptedConfiguration(config(acceptedConfig))
                            .lastCommittedConfiguration(config(committedConfig))
                            .build()
                    )
            )
            .build();
    }

    public void testDescriptionAfterDetachCluster() {
        final DiscoveryNode localNode = makeDiscoveryNode("local");

        final ClusterState clusterState = state(
            localNode,
            VotingConfiguration.MUST_JOIN_ELECTED_MASTER.getNodeIds().toArray(new String[0])
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered []; "
                    + "discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode otherNode = makeDiscoveryNode("otherNode");
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered ["
                    + noAttr(otherNode)
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode yetAnotherNode = makeDiscoveryNode("yetAnotherNode");
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(yetAnotherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered yet and this node was detached from its previous cluster, "
                    + "have discovered ["
                    + noAttr(yetAnotherNode)
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

    }

    public void testDescriptionAfterBootstrapping() {
        final DiscoveryNode localNode = makeDiscoveryNode("local");

        final ClusterState clusterState = state(localNode, "otherNode");

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                singletonList(otherAddress),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using ["
                    + otherAddress
                    + "] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode otherNode = makeDiscoveryNode("otherNode");
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(otherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have discovered possible quorum ["
                    + noAttr(otherNode)
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode yetAnotherNode = makeDiscoveryNode("yetAnotherNode");
        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                singletonList(yetAnotherNode),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [otherNode], "
                    + "have only discovered non-quorum ["
                    + noAttr(yetAnotherNode)
                    + "]; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires two nodes with ids [n1, n2], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires at least 2 nodes with ids from [n1, n2, n3], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", BOOTSTRAP_PLACEHOLDER_PREFIX + "n3"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires 2 nodes with ids [n1, n2], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", "n4"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", "n4", "n5"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4, n5], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", "n4", BOOTSTRAP_PLACEHOLDER_PREFIX + "n5"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, "n1", "n2", "n3", BOOTSTRAP_PLACEHOLDER_PREFIX + "n4", BOOTSTRAP_PLACEHOLDER_PREFIX + "n5"),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires 3 nodes with ids [n1, n2, n3], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n1" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [n1], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n2" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [n1] and a node with id [n2], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n2", "n3" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [n1] and two nodes with ids [n2, n3], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, new String[] { "n1" }, new String[] { "n2", "n3", "n4" }),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires a node with id [n1] and "
                    + "at least 2 nodes with ids from [n2, n3, n4], "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

        final DiscoveryNode otherMasterNode = makeDiscoveryNode("other-master");
        final DiscoveryNode otherNonMasterNode = DiscoveryNodeUtils.builder("other-non-master")
            .roles(
                new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()).stream().filter(r -> r != DiscoveryNodeRole.MASTER_ROLE).toList())
            )
            .build();

        String[] configNodeIds = new String[] { "n1", "n2" };
        final ClusterState stateWithOtherNodes = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()).add(otherMasterNode).add(otherNonMasterNode))
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastAcceptedConfiguration(config(configNodeIds))
                            .lastCommittedConfiguration(config(configNodeIds))
                            .build()
                    )
            )
            .build();

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                stateWithOtherNodes,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),

            // nodes from last-known cluster state could be in either order
            is(
                oneOf(
                    "master not discovered or elected yet, an election requires two nodes with ids [n1, n2], "
                        + "have only discovered non-quorum []; "
                        + "discovery will continue using [] from hosts providers and ["
                        + noAttr(localNode)
                        + ", "
                        + noAttr(otherMasterNode)
                        + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0",

                    "master not discovered or elected yet, an election requires two nodes with ids [n1, n2], "
                        + "have only discovered non-quorum []; "
                        + "discovery will continue using [] from hosts providers and ["
                        + noAttr(otherMasterNode)
                        + ", "
                        + noAttr(localNode)
                        + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
                )
            )
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                state(localNode, GatewayMetaState.STALE_STATE_CONFIG_NODE_ID),
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                emptyList()
            ).getDescription(),
            is(
                "master not discovered or elected yet, an election requires one or more nodes that have already participated as "
                    + "master-eligible nodes in the cluster but this node was not master-eligible the last time it joined the cluster, "
                    + "have only discovered non-quorum []; "
                    + "discovery will continue using [] from hosts providers and ["
                    + noAttr(localNode)
                    + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0"
            )
        );

    }

    public void testJoinStatusReporting() {
        final var localNode = makeDiscoveryNode("local");
        final var otherNode1 = makeDiscoveryNode("otherNode1");
        final var otherNode2 = makeDiscoveryNode("otherNode2");

        final var clusterState = state(localNode, "otherNode");

        final var prefix = "master not discovered or elected yet, an election requires a node with id [otherNode], "
            + "have only discovered non-quorum []; discovery will continue using [] from hosts providers and ["
            + noAttr(localNode)
            + "] from last-known cluster state; node term 0, last-accepted version 0 in term 0";

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                List.of(new JoinStatus(otherNode1, 1, JoinHelper.PENDING_JOIN_WAITING_RESPONSE, TimeValue.ZERO))
            ).getDescription(),
            is(prefix + "; joining [" + noAttr(otherNode1) + "] in term [1] has status [waiting for response] after [0ms]")
        );

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                List.of(
                    new JoinStatus(otherNode1, 2, JoinHelper.PENDING_JOIN_CONNECTING, TimeValue.timeValueMillis(10000)),
                    new JoinStatus(otherNode2, 2, JoinHelper.PENDING_JOIN_WAITING_RESPONSE, TimeValue.timeValueMillis(30000)),
                    new JoinStatus(otherNode2, 1, JoinHelper.PENDING_JOIN_WAITING_STATE, TimeValue.timeValueMillis(20000))
                )
            ).getDescription(),
            is(
                prefix
                    + "; joining ["
                    + noAttr(otherNode2)
                    + "] in term [2] has status [waiting for response] after [30s/30000ms]"
                    + "; joining ["
                    + noAttr(otherNode2)
                    + "] in term [1] has status [waiting to receive cluster state] after [20s/20000ms]"
                    + "; joining ["
                    + noAttr(otherNode1)
                    + "] in term [2] has status [waiting to connect] after [10s/10000ms]"
            )
        );

        final var expected = new StringBuilder(prefix);
        final var manyStatuses = new ArrayList<JoinStatus>();
        for (var term = 20; term > 0; term--) {
            final var node = randomFrom(otherNode1, otherNode2);
            if (manyStatuses.size() < 10) {
                expected.append("; joining [")
                    .append(noAttr(node))
                    .append("] in term [")
                    .append(term)
                    .append("] has status [status goes here] after [")
                    .append(term)
                    .append("ms]");
            }
            manyStatuses.add(new JoinStatus(node, term, "status goes here", TimeValue.timeValueMillis(term)));
        }
        Randomness.shuffle(manyStatuses);

        assertThat(
            new ClusterFormationState(
                Settings.EMPTY,
                clusterState,
                emptyList(),
                emptyList(),
                0L,
                electionStrategy,
                new StatusInfo(HEALTHY, "healthy-info"),
                manyStatuses
            ).getDescription(),
            is(expected.toString())
        );
    }

    public void testClusterFormationStateSerialization() {
        final DiscoveryNode localNode = makeDiscoveryNode("local");
        List<TransportAddress> resolvedAddresses = List.of(buildNewFakeTransportAddress(), buildNewFakeTransportAddress());
        List<DiscoveryNode> foundPeers = List.of(
            DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
            DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
            DiscoveryNodeUtils.create(UUID.randomUUID().toString())
        );
        List<JoinStatus> joinStatuses = List.of(
            new JoinStatus(
                DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
                1,
                "join status message",
                new TimeValue(500, TimeUnit.SECONDS)
            )
        );
        Settings settings = Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), List.of("a", "b", "c")).build();
        ClusterFormationState clusterFormationState = new ClusterFormationState(
            settings,
            state(localNode, new String[] { "n1" }, new String[] { "n2", "n3", "n4" }),
            resolvedAddresses,
            foundPeers,
            0L,
            electionStrategy,
            new StatusInfo(HEALTHY, "healthy-info"),
            joinStatuses
        );
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            clusterFormationState,
            history -> copyWriteable(clusterFormationState, writableRegistry(), ClusterFormationState::new),
            this::mutateClusterFormationState
        );
    }

    private static ClusterState state(DiscoveryNode localNode, VotingConfiguration acceptedConfig, VotingConfiguration committedConfig) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId()))
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .lastAcceptedConfiguration(acceptedConfig)
                            .lastCommittedConfiguration(committedConfig)
                            .build()
                    )
            )
            .build();
    }

    private ClusterFormationState mutateClusterFormationState(ClusterFormationState originalClusterFormationState) {
        Settings settings = Settings.builder()
            .putList(INITIAL_MASTER_NODES_SETTING.getKey(), originalClusterFormationState.initialMasterNodesSetting())
            .build();
        final DiscoveryNode localNode = originalClusterFormationState.localNode();
        List<TransportAddress> resolvedAddresses = originalClusterFormationState.resolvedAddresses();
        List<DiscoveryNode> foundPeers = originalClusterFormationState.foundPeers();
        long currentTerm = originalClusterFormationState.currentTerm();
        StatusInfo statusInfo = originalClusterFormationState.statusInfo();
        List<JoinStatus> joinStatuses = originalClusterFormationState.inFlightJoinStatuses();
        ClusterState clusterState = state(
            localNode,
            originalClusterFormationState.lastAcceptedConfiguration(),
            originalClusterFormationState.lastCommittedConfiguration()
        );
        switch (randomIntBetween(1, 5)) {
            case 1 -> {
                return new ClusterFormationState(
                    settings,
                    clusterState,
                    resolvedAddresses,
                    foundPeers,
                    currentTerm + 1,
                    electionStrategy,
                    statusInfo,
                    joinStatuses
                );
            }
            case 2 -> {
                List<DiscoveryNode> newFoundPeers = new ArrayList<>(foundPeers);
                newFoundPeers.add(DiscoveryNodeUtils.create(UUID.randomUUID().toString()));
                return new ClusterFormationState(
                    settings,
                    clusterState,
                    resolvedAddresses,
                    newFoundPeers,
                    currentTerm,
                    electionStrategy,
                    statusInfo,
                    joinStatuses
                );
            }
            case 3 -> {
                List<JoinStatus> newJoinStatuses = new ArrayList<>(joinStatuses);
                newJoinStatuses.add(
                    new JoinStatus(
                        DiscoveryNodeUtils.create(UUID.randomUUID().toString()),
                        1,
                        "join status message",
                        new TimeValue(500, TimeUnit.SECONDS)
                    )
                );
                return new ClusterFormationState(
                    settings,
                    clusterState,
                    resolvedAddresses,
                    foundPeers,
                    currentTerm,
                    electionStrategy,
                    statusInfo,
                    newJoinStatuses
                );
            }
            case 4 -> {
                StatusInfo newStatusInfo = new StatusInfo(randomFrom(HEALTHY, UNHEALTHY), randomAlphaOfLength(20));
                return new ClusterFormationState(
                    settings,
                    clusterState,
                    resolvedAddresses,
                    foundPeers,
                    currentTerm,
                    electionStrategy,
                    newStatusInfo,
                    joinStatuses
                );
            }
            case 5 -> {
                return new ClusterFormationState(
                    Settings.EMPTY,
                    clusterState,
                    resolvedAddresses,
                    foundPeers,
                    currentTerm,
                    electionStrategy,
                    statusInfo,
                    joinStatuses
                );
            }
            default -> throw new IllegalStateException();
        }
    }

    private static DiscoveryNode makeDiscoveryNode(String nodeId) {
        final Map<String, String> attributes = new HashMap<>();
        if (randomBoolean()) {
            attributes.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
        }
        return DiscoveryNodeUtils.create(nodeId, buildNewFakeTransportAddress(), attributes, DiscoveryNodeRole.roles());
    }

    private static String noAttr(DiscoveryNode discoveryNode) {
        final StringBuilder stringBuilder = new StringBuilder();
        discoveryNode.appendDescriptionWithoutAttributes(stringBuilder);
        return stringBuilder.toString();
    }
}
