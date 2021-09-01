/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.NodeRoles;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.singleton;
import static org.elasticsearch.cluster.routing.DelayedAllocationService.CLUSTER_UPDATE_TASK_SOURCE;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class DelayedAllocationServiceTests extends ESAllocationTestCase {

    private TestDelayAllocationService delayedAllocationService;
    private MockAllocationService allocationService;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    @Before
    public void createDelayedAllocationService() {
        threadPool = new TestThreadPool(getTestName());
        clusterService = mock(ClusterService.class);
        allocationService = createAllocationService(Settings.EMPTY, new DelayedShardsMockGatewayAllocator());
        when(clusterService.getSettings()).thenReturn(NodeRoles.masterOnlyNode());
        delayedAllocationService = new TestDelayAllocationService(threadPool, clusterService, allocationService);
        verify(clusterService).addListener(delayedAllocationService);
        verify(clusterService).getSettings();
    }

    @After
    public void shutdownThreadPool() throws Exception {
        terminate(threadPool);
    }

    public void testNoDelayedUnassigned() throws Exception {
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), "0"))
                .numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build()).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).localNodeId("node1").masterNodeId("node1"))
            .build();
        clusterState = allocationService.reroute(clusterState, "reroute");
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertThat(clusterState.getRoutingNodes().unassigned().size() > 0, equalTo(false));
        ClusterState prevState = clusterState;
        // remove node2 and reroute
        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder(clusterState.nodes()).remove("node2");
        boolean nodeAvailableForAllocation = randomBoolean();
        if (nodeAvailableForAllocation) {
            nodes.add(newNode("node3"));
        }
        clusterState = ClusterState.builder(clusterState).nodes(nodes).build();
        clusterState = allocationService.disassociateDeadNodes(clusterState, true, "reroute");
        ClusterState newState = clusterState;
        List<ShardRouting> unassignedShards = newState.getRoutingTable().shardsWithState(ShardRoutingState.UNASSIGNED);
        if (nodeAvailableForAllocation) {
            assertThat(unassignedShards.size(), equalTo(0));
        } else {
            assertThat(unassignedShards.size(), equalTo(1));
            assertThat(unassignedShards.get(0).unassignedInfo().isDelayed(), equalTo(false));
        }

        delayedAllocationService.clusterChanged(new ClusterChangedEvent("test", newState, prevState));
        verifyNoMoreInteractions(clusterService);
        assertNull(delayedAllocationService.delayedRerouteTask.get());
    }

    public void testDelayedUnassignedScheduleReroute() throws Exception {
        TimeValue delaySetting = timeValueMillis(100);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("test").settings(settings(Version.CURRENT)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delaySetting))
                .numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("test")).build()).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder().add(newNode("node1")).add(newNode("node2")).localNodeId("node1").masterNodeId("node1"))
            .build();
        final long baseTimestampNanos = System.nanoTime();
        allocationService.setNanoTimeOverride(baseTimestampNanos);
        clusterState = allocationService.reroute(clusterState, "reroute");
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertFalse("no shards should be unassigned", clusterState.getRoutingNodes().unassigned().size() > 0);
        String nodeId = null;
        final List<ShardRouting> allShards = clusterState.getRoutingTable().allShards("test");
        // we need to find the node with the replica otherwise we will not reroute
        for (ShardRouting shardRouting : allShards) {
            if (shardRouting.primary() == false) {
                nodeId = shardRouting.currentNodeId();
                break;
            }
        }
        assertNotNull(nodeId);

        // remove node that has replica and reroute
        clusterState = ClusterState.builder(clusterState).nodes(DiscoveryNodes.builder(clusterState.nodes()).remove(nodeId)).build();
        clusterState = allocationService.disassociateDeadNodes(clusterState, true, "reroute");
        ClusterState stateWithDelayedShard = clusterState;
        // make sure the replica is marked as delayed (i.e. not reallocated)
        assertEquals(1, UnassignedInfo.getNumberOfDelayedUnassigned(stateWithDelayedShard));
        ShardRouting delayedShard = stateWithDelayedShard.getRoutingNodes().unassigned().iterator().next();
        assertEquals(baseTimestampNanos, delayedShard.unassignedInfo().getUnassignedTimeInNanos());

        // mock ClusterService.submitStateUpdateTask() method
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<ClusterStateUpdateTask> clusterStateUpdateTask = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            clusterStateUpdateTask.set((ClusterStateUpdateTask)invocationOnMock.getArguments()[1]);
            latch.countDown();
            return null;
        }).when(clusterService).submitStateUpdateTask(eq(CLUSTER_UPDATE_TASK_SOURCE), any(ClusterStateUpdateTask.class));
        assertNull(delayedAllocationService.delayedRerouteTask.get());
        long delayUntilClusterChangeEvent = TimeValue.timeValueNanos(randomInt((int)delaySetting.nanos() - 1)).nanos();
        long clusterChangeEventTimestampNanos = baseTimestampNanos + delayUntilClusterChangeEvent;
        delayedAllocationService.setNanoTimeOverride(clusterChangeEventTimestampNanos);
        delayedAllocationService.clusterChanged(new ClusterChangedEvent("fake node left", stateWithDelayedShard, clusterState));

        // check that delayed reroute task was created and registered with the proper settings
        DelayedAllocationService.DelayedRerouteTask delayedRerouteTask = delayedAllocationService.delayedRerouteTask.get();
        assertNotNull(delayedRerouteTask);
        assertFalse(delayedRerouteTask.cancelScheduling.get());
        assertThat(delayedRerouteTask.baseTimestampNanos, equalTo(clusterChangeEventTimestampNanos));
        assertThat(delayedRerouteTask.nextDelay.nanos(),
            equalTo(delaySetting.nanos() - (clusterChangeEventTimestampNanos - baseTimestampNanos)));

        // check that submitStateUpdateTask() was invoked on the cluster service mock
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        verify(clusterService).submitStateUpdateTask(eq(CLUSTER_UPDATE_TASK_SOURCE), eq(clusterStateUpdateTask.get()));

        // advance the time on the allocation service to a timestamp that happened after the delayed scheduling
        long nanoTimeForReroute = clusterChangeEventTimestampNanos + delaySetting.nanos() + timeValueMillis(randomInt(200)).nanos();
        allocationService.setNanoTimeOverride(nanoTimeForReroute);
        // apply cluster state
        ClusterState stateWithRemovedDelay = clusterStateUpdateTask.get().execute(stateWithDelayedShard);
        // check that shard is not delayed anymore
        assertEquals(0, UnassignedInfo.getNumberOfDelayedUnassigned(stateWithRemovedDelay));
        // check that task is now removed
        assertNull(delayedAllocationService.delayedRerouteTask.get());

        // simulate calling listener (cluster change event)
        delayedAllocationService.setNanoTimeOverride(nanoTimeForReroute + timeValueMillis(randomInt(200)).nanos());
        delayedAllocationService.clusterChanged(
            new ClusterChangedEvent(CLUSTER_UPDATE_TASK_SOURCE, stateWithRemovedDelay, stateWithDelayedShard));
        // check that no new task is scheduled
        assertNull(delayedAllocationService.delayedRerouteTask.get());
        // check that no further cluster state update was submitted
        verifyNoMoreInteractions(clusterService);
    }

    /**
     * This tests that a new delayed reroute is scheduled right after a delayed reroute was run
     */
    public void testDelayedUnassignedScheduleRerouteAfterDelayedReroute() throws Exception {
        TimeValue shortDelaySetting = timeValueMillis(100);
        TimeValue longDelaySetting = TimeValue.timeValueSeconds(1);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("short_delay")
                .settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), shortDelaySetting))
                .numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("long_delay")
                .settings(settings(Version.CURRENT).put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), longDelaySetting))
                .numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).metadata(metadata)
            .routingTable(RoutingTable.builder().addAsNew(metadata.index("short_delay")).addAsNew(metadata.index("long_delay")).build())
            .nodes(DiscoveryNodes.builder()
                .add(newNode("node0", singleton(DiscoveryNodeRole.MASTER_ROLE))).localNodeId("node0").masterNodeId("node0")
                .add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4"))).build();
        // allocate shards
        clusterState = allocationService.reroute(clusterState, "reroute");
        // start primaries
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        // start replicas
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertThat("all shards should be started", clusterState.getRoutingNodes().shardsWithState(STARTED).size(), equalTo(4));

        // find replica of short_delay
        ShardRouting shortDelayReplica = null;
        for (ShardRouting shardRouting : clusterState.getRoutingTable().allShards("short_delay")) {
            if (shardRouting.primary() == false) {
                shortDelayReplica = shardRouting;
                break;
            }
        }
        assertNotNull(shortDelayReplica);

        // find replica of long_delay
        ShardRouting longDelayReplica = null;
        for (ShardRouting shardRouting : clusterState.getRoutingTable().allShards("long_delay")) {
            if (shardRouting.primary() == false) {
                longDelayReplica = shardRouting;
                break;
            }
        }
        assertNotNull(longDelayReplica);

        final long baseTimestampNanos = System.nanoTime();

        // remove node of shortDelayReplica and node of longDelayReplica and reroute
        ClusterState clusterStateBeforeNodeLeft = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder(clusterState.nodes())
                .remove(shortDelayReplica.currentNodeId())
                .remove(longDelayReplica.currentNodeId()))
            .build();
        // make sure both replicas are marked as delayed (i.e. not reallocated)
        allocationService.setNanoTimeOverride(baseTimestampNanos);
        clusterState = allocationService.disassociateDeadNodes(clusterState, true, "reroute");
        final ClusterState stateWithDelayedShards = clusterState;
        assertEquals(2, UnassignedInfo.getNumberOfDelayedUnassigned(stateWithDelayedShards));
        RoutingNodes.UnassignedShards.UnassignedIterator iter = stateWithDelayedShards.getRoutingNodes().unassigned().iterator();
        assertEquals(baseTimestampNanos, iter.next().unassignedInfo().getUnassignedTimeInNanos());
        assertEquals(baseTimestampNanos, iter.next().unassignedInfo().getUnassignedTimeInNanos());

        // mock ClusterService.submitStateUpdateTask() method
        CountDownLatch latch1 = new CountDownLatch(1);
        AtomicReference<ClusterStateUpdateTask> clusterStateUpdateTask1 = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            clusterStateUpdateTask1.set((ClusterStateUpdateTask)invocationOnMock.getArguments()[1]);
            latch1.countDown();
            return null;
        }).when(clusterService).submitStateUpdateTask(eq(CLUSTER_UPDATE_TASK_SOURCE), any(ClusterStateUpdateTask.class));
        assertNull(delayedAllocationService.delayedRerouteTask.get());
        long delayUntilClusterChangeEvent = TimeValue.timeValueNanos(randomInt((int)shortDelaySetting.nanos() - 1)).nanos();
        long clusterChangeEventTimestampNanos = baseTimestampNanos + delayUntilClusterChangeEvent;
        delayedAllocationService.setNanoTimeOverride(clusterChangeEventTimestampNanos);
        delayedAllocationService.clusterChanged(
            new ClusterChangedEvent("fake node left", stateWithDelayedShards, clusterStateBeforeNodeLeft));

        // check that delayed reroute task was created and registered with the proper settings
        DelayedAllocationService.DelayedRerouteTask firstDelayedRerouteTask = delayedAllocationService.delayedRerouteTask.get();
        assertNotNull(firstDelayedRerouteTask);
        assertFalse(firstDelayedRerouteTask.cancelScheduling.get());
        assertThat(firstDelayedRerouteTask.baseTimestampNanos, equalTo(clusterChangeEventTimestampNanos));
        assertThat(firstDelayedRerouteTask.nextDelay.nanos(),
            equalTo(UnassignedInfo.findNextDelayedAllocation(clusterChangeEventTimestampNanos, stateWithDelayedShards)));
        assertThat(firstDelayedRerouteTask.nextDelay.nanos(),
            equalTo(shortDelaySetting.nanos() - (clusterChangeEventTimestampNanos - baseTimestampNanos)));

        // check that submitStateUpdateTask() was invoked on the cluster service mock
        assertTrue(latch1.await(30, TimeUnit.SECONDS));
        verify(clusterService).submitStateUpdateTask(eq(CLUSTER_UPDATE_TASK_SOURCE), eq(clusterStateUpdateTask1.get()));

        // advance the time on the allocation service to a timestamp that happened after the delayed scheduling
        long nanoTimeForReroute = clusterChangeEventTimestampNanos + shortDelaySetting.nanos() + timeValueMillis(randomInt(50)).nanos();
        allocationService.setNanoTimeOverride(nanoTimeForReroute);
        // apply cluster state
        ClusterState stateWithOnlyOneDelayedShard = clusterStateUpdateTask1.get().execute(stateWithDelayedShards);
        // check that shard is not delayed anymore
        assertEquals(1, UnassignedInfo.getNumberOfDelayedUnassigned(stateWithOnlyOneDelayedShard));
        // check that task is now removed
        assertNull(delayedAllocationService.delayedRerouteTask.get());

        // mock ClusterService.submitStateUpdateTask() method again
        CountDownLatch latch2 = new CountDownLatch(1);
        AtomicReference<ClusterStateUpdateTask> clusterStateUpdateTask2 = new AtomicReference<>();
        doAnswer(invocationOnMock -> {
            clusterStateUpdateTask2.set((ClusterStateUpdateTask)invocationOnMock.getArguments()[1]);
            latch2.countDown();
            return null;
        }).when(clusterService).submitStateUpdateTask(eq(CLUSTER_UPDATE_TASK_SOURCE), any(ClusterStateUpdateTask.class));
        // simulate calling listener (cluster change event)
        delayUntilClusterChangeEvent = timeValueMillis(randomInt(50)).nanos();
        clusterChangeEventTimestampNanos = nanoTimeForReroute + delayUntilClusterChangeEvent;
        delayedAllocationService.setNanoTimeOverride(clusterChangeEventTimestampNanos);
        delayedAllocationService.clusterChanged(
            new ClusterChangedEvent(CLUSTER_UPDATE_TASK_SOURCE, stateWithOnlyOneDelayedShard, stateWithDelayedShards));

        // check that new delayed reroute task was created and registered with the proper settings
        DelayedAllocationService.DelayedRerouteTask secondDelayedRerouteTask = delayedAllocationService.delayedRerouteTask.get();
        assertNotNull(secondDelayedRerouteTask);
        assertFalse(secondDelayedRerouteTask.cancelScheduling.get());
        assertThat(secondDelayedRerouteTask.baseTimestampNanos, equalTo(clusterChangeEventTimestampNanos));
        assertThat(secondDelayedRerouteTask.nextDelay.nanos(),
            equalTo(UnassignedInfo.findNextDelayedAllocation(clusterChangeEventTimestampNanos, stateWithOnlyOneDelayedShard)));
        assertThat(secondDelayedRerouteTask.nextDelay.nanos(),
            equalTo(longDelaySetting.nanos() - (clusterChangeEventTimestampNanos - baseTimestampNanos)));

        // check that submitStateUpdateTask() was invoked on the cluster service mock
        assertTrue(latch2.await(30, TimeUnit.SECONDS));
        verify(clusterService).submitStateUpdateTask(eq(CLUSTER_UPDATE_TASK_SOURCE), eq(clusterStateUpdateTask2.get()));

        // advance the time on the allocation service to a timestamp that happened after the delayed scheduling
        nanoTimeForReroute = clusterChangeEventTimestampNanos + longDelaySetting.nanos() + timeValueMillis(randomInt(50)).nanos();
        allocationService.setNanoTimeOverride(nanoTimeForReroute);
        // apply cluster state
        ClusterState stateWithNoDelayedShards = clusterStateUpdateTask2.get().execute(stateWithOnlyOneDelayedShard);
        // check that shard is not delayed anymore
        assertEquals(0, UnassignedInfo.getNumberOfDelayedUnassigned(stateWithNoDelayedShards));
        // check that task is now removed
        assertNull(delayedAllocationService.delayedRerouteTask.get());

        // simulate calling listener (cluster change event)
        delayedAllocationService.setNanoTimeOverride(nanoTimeForReroute + timeValueMillis(randomInt(50)).nanos());
        delayedAllocationService.clusterChanged(
            new ClusterChangedEvent(CLUSTER_UPDATE_TASK_SOURCE, stateWithNoDelayedShards, stateWithOnlyOneDelayedShard));
        // check that no new task is scheduled
        assertNull(delayedAllocationService.delayedRerouteTask.get());
        // check that no further cluster state update was submitted
        verifyNoMoreInteractions(clusterService);
    }

    public void testDelayedUnassignedScheduleRerouteRescheduledOnShorterDelay() throws Exception {
        TimeValue delaySetting = timeValueSeconds(30);
        TimeValue shorterDelaySetting = timeValueMillis(100);
        Metadata metadata = Metadata.builder()
            .put(IndexMetadata.builder("foo").settings(settings(Version.CURRENT)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), delaySetting))
                .numberOfShards(1).numberOfReplicas(1))
            .put(IndexMetadata.builder("bar").settings(settings(Version.CURRENT)
                .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), shorterDelaySetting))
                .numberOfShards(1).numberOfReplicas(1))
            .build();
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY))
            .metadata(metadata)
            .routingTable(RoutingTable.builder()
                .addAsNew(metadata.index("foo"))
                .addAsNew(metadata.index("bar"))
                .build()).build();
        clusterState = ClusterState.builder(clusterState)
            .nodes(DiscoveryNodes.builder()
                .add(newNode("node1")).add(newNode("node2")).add(newNode("node3")).add(newNode("node4"))
                .localNodeId("node1").masterNodeId("node1"))
            .build();
        final long nodeLeftTimestampNanos = System.nanoTime();
        allocationService.setNanoTimeOverride(nodeLeftTimestampNanos);
        clusterState = allocationService.reroute(clusterState, "reroute");
        // starting primaries
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        // starting replicas
        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);
        assertFalse("no shards should be unassigned", clusterState.getRoutingNodes().unassigned().size() > 0);
        String nodeIdOfFooReplica = null;
        for (ShardRouting shardRouting : clusterState.getRoutingTable().allShards("foo")) {
            if (shardRouting.primary() == false) {
                nodeIdOfFooReplica = shardRouting.currentNodeId();
                break;
            }
        }
        assertNotNull(nodeIdOfFooReplica);

        // remove node that has replica and reroute
        clusterState = ClusterState.builder(clusterState).nodes(
            DiscoveryNodes.builder(clusterState.nodes()).remove(nodeIdOfFooReplica)).build();
        clusterState = allocationService.disassociateDeadNodes(clusterState, true, "fake node left");
        ClusterState stateWithDelayedShard = clusterState;
        // make sure the replica is marked as delayed (i.e. not reallocated)
        assertEquals(1, UnassignedInfo.getNumberOfDelayedUnassigned(stateWithDelayedShard));
        ShardRouting delayedShard = stateWithDelayedShard.getRoutingNodes().unassigned().iterator().next();
        assertEquals(nodeLeftTimestampNanos, delayedShard.unassignedInfo().getUnassignedTimeInNanos());

        assertNull(delayedAllocationService.delayedRerouteTask.get());
        long delayUntilClusterChangeEvent = TimeValue.timeValueNanos(randomInt((int)shorterDelaySetting.nanos() - 1)).nanos();
        long clusterChangeEventTimestampNanos = nodeLeftTimestampNanos + delayUntilClusterChangeEvent;
        delayedAllocationService.setNanoTimeOverride(clusterChangeEventTimestampNanos);
        delayedAllocationService.clusterChanged(new ClusterChangedEvent("fake node left", stateWithDelayedShard, clusterState));

        // check that delayed reroute task was created and registered with the proper settings
        DelayedAllocationService.DelayedRerouteTask delayedRerouteTask = delayedAllocationService.delayedRerouteTask.get();
        assertNotNull(delayedRerouteTask);
        assertFalse(delayedRerouteTask.cancelScheduling.get());
        assertThat(delayedRerouteTask.baseTimestampNanos, equalTo(clusterChangeEventTimestampNanos));
        assertThat(delayedRerouteTask.nextDelay.nanos(),
            equalTo(delaySetting.nanos() - (clusterChangeEventTimestampNanos - nodeLeftTimestampNanos)));

        if (randomBoolean()) {
            // update settings with shorter delay
            ClusterState stateWithShorterDelay = ClusterState.builder(stateWithDelayedShard).metadata(Metadata.builder(
                stateWithDelayedShard.metadata()).updateSettings(Settings.builder().put(
                UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), shorterDelaySetting).build(), "foo")).build();
            delayedAllocationService.setNanoTimeOverride(clusterChangeEventTimestampNanos);
            delayedAllocationService.clusterChanged(
                new ClusterChangedEvent("apply shorter delay", stateWithShorterDelay, stateWithDelayedShard));
        } else {
            // node leaves with replica shard of index bar that has shorter delay
            String nodeIdOfBarReplica = null;
            for (ShardRouting shardRouting : stateWithDelayedShard.getRoutingTable().allShards("bar")) {
                if (shardRouting.primary() == false) {
                    nodeIdOfBarReplica = shardRouting.currentNodeId();
                    break;
                }
            }
            assertNotNull(nodeIdOfBarReplica);

            // remove node that has replica and reroute
            clusterState = ClusterState.builder(stateWithDelayedShard).nodes(
                DiscoveryNodes.builder(stateWithDelayedShard.nodes()).remove(nodeIdOfBarReplica)).build();
            ClusterState stateWithShorterDelay = allocationService.disassociateDeadNodes(clusterState, true, "fake node left");
            delayedAllocationService.setNanoTimeOverride(clusterChangeEventTimestampNanos);
            delayedAllocationService.clusterChanged(
                new ClusterChangedEvent("fake node left", stateWithShorterDelay, stateWithDelayedShard));
        }

        // check that delayed reroute task was replaced by shorter reroute task
        DelayedAllocationService.DelayedRerouteTask shorterDelayedRerouteTask = delayedAllocationService.delayedRerouteTask.get();
        assertNotNull(shorterDelayedRerouteTask);
        assertNotEquals(shorterDelayedRerouteTask, delayedRerouteTask);
        assertTrue(delayedRerouteTask.cancelScheduling.get()); // existing task was cancelled
        assertFalse(shorterDelayedRerouteTask.cancelScheduling.get());
        assertThat(delayedRerouteTask.baseTimestampNanos, equalTo(clusterChangeEventTimestampNanos));
        assertThat(shorterDelayedRerouteTask.nextDelay.nanos(),
            equalTo(shorterDelaySetting.nanos() - (clusterChangeEventTimestampNanos - nodeLeftTimestampNanos)));
    }

    private static class TestDelayAllocationService extends DelayedAllocationService {
        private volatile long nanoTimeOverride = -1L;

        private TestDelayAllocationService(ThreadPool threadPool, ClusterService clusterService,
                                           AllocationService allocationService) {
            super(threadPool, clusterService, allocationService);
        }

        @Override
        protected void assertClusterOrMasterStateThread() {
            // do not check this in the unit tests
        }

        public void setNanoTimeOverride(long nanoTime) {
            this.nanoTimeOverride = nanoTime;
        }

        @Override
        protected long currentNanoTime() {
            return nanoTimeOverride == -1L ? super.currentNanoTime() : nanoTimeOverride;
        }
    }
}
