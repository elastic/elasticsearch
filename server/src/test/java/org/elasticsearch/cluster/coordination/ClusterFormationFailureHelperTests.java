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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.ClusterFormationFailureHelper.ClusterFormationState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.elasticsearch.cluster.coordination.ClusterBootstrapService.INITIAL_MASTER_NODES_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ClusterFormationFailureHelperTests extends ESTestCase {
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

        final DeterministicTaskQueue deterministicTaskQueue
            = new DeterministicTaskQueue(Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build(), random());

        final AtomicLong warningCount = new AtomicLong();

        final ClusterFormationFailureHelper clusterFormationFailureHelper = new ClusterFormationFailureHelper(settingsBuilder.build(),
            () -> {
                warningCount.incrementAndGet();
                return new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList());
            },
            deterministicTaskQueue.getThreadPool());

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

        warningCount.set(0);
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
    }

    public void testDescriptionOnMasterIneligibleNodes() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId())).build();

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList()).getDescription(),
            is("master not discovered yet: have discovered []; discovery will continue using [] from hosts providers and [" + localNode +
                "] from last-known cluster state"));

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, singletonList(otherAddress), emptyList()).getDescription(),
            is("master not discovered yet: have discovered []; discovery will continue using [" + otherAddress +
                "] from hosts providers and [" + localNode + "] from last-known cluster state"));

        final DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(otherNode)).getDescription(),
            is("master not discovered yet: have discovered [" + otherNode + "]; discovery will continue using [] from hosts providers and ["
                + localNode + "] from last-known cluster state"));
    }

    public void testDescriptionBeforeBootstrapping() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(localNode).localNodeId(localNode.getId())).build();

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList()).getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped (v7+) cluster, and " +
                "[cluster.initial_master_nodes] is empty on this node: have discovered []; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, singletonList(otherAddress), emptyList()).getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped (v7+) cluster, and " +
                "[cluster.initial_master_nodes] is empty on this node: have discovered []; " +
                "discovery will continue using [" + otherAddress + "] from hosts providers and [" + localNode +
                "] from last-known cluster state"));

        final DiscoveryNode otherNode = new DiscoveryNode("other", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(otherNode)).getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped (v7+) cluster, and " +
                "[cluster.initial_master_nodes] is empty on this node: have discovered [" + otherNode + "]; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.builder().putList(INITIAL_MASTER_NODES_SETTING.getKey(), "other").build(),
                clusterState, emptyList(), emptyList()).getDescription(),
            is("master not discovered yet, this node has not previously joined a bootstrapped (v7+) cluster, and " +
                "this node must discover master-eligible nodes [other] to bootstrap a cluster: have discovered []; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));
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
            .metaData(MetaData.builder().coordinationMetaData(CoordinationMetaData.builder()
                .lastAcceptedConfiguration(config(acceptedConfig))
                .lastCommittedConfiguration(config(committedConfig)).build())).build();
    }

    public void testDescriptionAfterBootstrapping() {
        final DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);

        final ClusterState clusterState = state(localNode, "otherNode");

        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        final TransportAddress otherAddress = buildNewFakeTransportAddress();
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, singletonList(otherAddress), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [" + otherAddress + "] from hosts providers and [" + localNode +
                "] from last-known cluster state"));

        final DiscoveryNode otherNode = new DiscoveryNode("otherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(otherNode)).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have discovered [" + otherNode + "] which is a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        final DiscoveryNode yetAnotherNode = new DiscoveryNode("yetAnotherNode", buildNewFakeTransportAddress(), Version.CURRENT);
        assertThat(new ClusterFormationState(Settings.EMPTY, clusterState, emptyList(), singletonList(yetAnotherNode)).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [otherNode], " +
                "have discovered [" + yetAnotherNode + "] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2"), emptyList(), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires two nodes with ids [n1, n2], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3"), emptyList(), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires at least 2 nodes with ids from [n1, n2, n3], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3", "n4"), emptyList(), emptyList())
                .getDescription(),
            is("master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, "n1", "n2", "n3", "n4", "n5"), emptyList(), emptyList())
                .getDescription(),
            is("master not discovered or elected yet, an election requires at least 3 nodes with ids from [n1, n2, n3, n4, n5], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n1"}),
                emptyList(), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n2"}),
                emptyList(), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1] and a node with id [n2], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n2", "n3"}),
                emptyList(), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1] and two nodes with ids [n2, n3], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));

        assertThat(new ClusterFormationState(Settings.EMPTY, state(localNode, new String[]{"n1"}, new String[]{"n2", "n3", "n4"}),
                emptyList(), emptyList()).getDescription(),
            is("master not discovered or elected yet, an election requires a node with id [n1] and " +
                "at least 2 nodes with ids from [n2, n3, n4], " +
                "have discovered [] which is not a quorum; " +
                "discovery will continue using [] from hosts providers and [" + localNode + "] from last-known cluster state"));
    }
}
