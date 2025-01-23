/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.selection.HealthNode;
import org.elasticsearch.health.node.tracker.HealthTracker;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalHealthMonitorTests extends ESTestCase {

    private static final DiskHealthInfo GREEN = new DiskHealthInfo(HealthStatus.GREEN, null);
    private static final DiskHealthInfo YELLOW = new DiskHealthInfo(HealthStatus.YELLOW, null);
    private static final DiskHealthInfo RED = new DiskHealthInfo(HealthStatus.RED, null);
    private static ThreadPool threadPool;
    private ClusterService clusterService;
    private DiscoveryNode node;
    private DiscoveryNode frozenNode;
    private HealthMetadata healthMetadata;
    private ClusterState clusterState;
    private Client client;
    private MockHealthTracker mockHealthTracker;
    private LocalHealthMonitor localHealthMonitor;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(LocalHealthMonitorTests.class.getSimpleName());
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        // Set-up cluster state
        healthMetadata = new HealthMetadata(
            HealthMetadata.Disk.newBuilder()
                .highWatermark(new RelativeByteSizeValue(ByteSizeValue.ofBytes(100)))
                .floodStageWatermark(new RelativeByteSizeValue(ByteSizeValue.ofBytes(50)))
                .frozenFloodStageWatermark(new RelativeByteSizeValue(ByteSizeValue.ofBytes(50)))
                .frozenFloodStageMaxHeadroom(ByteSizeValue.ofBytes(10))
                .build(),
            HealthMetadata.ShardLimits.newBuilder().maxShardsPerNode(999).maxShardsPerNodeFrozen(100).build()
        );
        node = DiscoveryNodeUtils.create("node", "node");
        frozenNode = DiscoveryNodeUtils.builder("frozen-node")
            .name("frozen-node")
            .roles(Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE))
            .build();
        var searchNode = DiscoveryNodeUtils.builder("search-node").name("search-node").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
        var searchAndIndexNode = DiscoveryNodeUtils.builder("search-and-index-node")
            .name("search-and-index-node")
            .roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE, DiscoveryNodeRole.INDEX_ROLE))
            .build();
        clusterState = ClusterStateCreationUtils.state(
            node,
            node,
            node,
            new DiscoveryNode[] { node, frozenNode, searchNode, searchAndIndexNode }
        ).copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        // Set-up cluster service
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.localNode()).thenReturn(node);

        // Set-up node service with a node with a healthy disk space usage

        client = mock(Client.class);

        mockHealthTracker = new MockHealthTracker();

        localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, threadPool, client, List.of(mockHealthTracker));
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();

        // Kill monitoring process running in the background after each test.
        localHealthMonitor.setEnabled(false);
    }

    @SuppressWarnings("unchecked")
    public void testUpdateHealthInfo() throws Exception {
        doAnswer(invocation -> {
            DiskHealthInfo diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertThat(diskHealthInfo, equalTo(GREEN));
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        // We override the poll interval like this to avoid the min value set by the setting which is too high for this test
        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(10));
        assertThat(mockHealthTracker.getLastDeterminedHealth(), nullValue());
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("initialize", clusterState, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(mockHealthTracker.getLastDeterminedHealth(), equalTo(GREEN)));
    }

    @SuppressWarnings("unchecked")
    public void testDoNotUpdateHealthInfoOnFailure() throws Exception {
        AtomicReference<Boolean> clientCalled = new AtomicReference<>(false);
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onFailure(new RuntimeException("simulated"));
            clientCalled.set(true);
            return null;
        }).when(client).execute(any(), any(), any());

        localHealthMonitor.clusterChanged(new ClusterChangedEvent("initialize", clusterState, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(clientCalled.get(), equalTo(true)));
        assertThat(mockHealthTracker.getLastDeterminedHealth(), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testSendHealthInfoToNewNode() throws Exception {
        ClusterState previous = ClusterStateCreationUtils.state(node, node, frozenNode, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        ClusterState current = ClusterStateCreationUtils.state(node, node, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        AtomicInteger counter = new AtomicInteger(0);
        doAnswer(invocation -> {
            DiskHealthInfo diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertThat(diskHealthInfo, equalTo(GREEN));
            counter.incrementAndGet();
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(10));
        when(clusterService.state()).thenReturn(previous);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("start-up", previous, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(mockHealthTracker.getLastDeterminedHealth(), equalTo(GREEN)));
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("health-node-switch", current, previous));
        assertBusy(() -> assertThat(counter.get(), equalTo(2)));
    }

    @SuppressWarnings("unchecked")
    public void testResendHealthInfoOnMasterChange() throws Exception {
        ClusterState previous = ClusterStateCreationUtils.state(node, node, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        ClusterState current = ClusterStateCreationUtils.state(node, frozenNode, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        AtomicInteger counter = new AtomicInteger(0);
        doAnswer(invocation -> {
            DiskHealthInfo diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertThat(diskHealthInfo, equalTo(GREEN));
            counter.incrementAndGet();
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(10));
        when(clusterService.state()).thenReturn(previous);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("start-up", previous, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(mockHealthTracker.getLastDeterminedHealth(), equalTo(GREEN)));
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("health-node-switch", current, previous));
        assertBusy(() -> assertThat(counter.get(), equalTo(2)));
    }

    @SuppressWarnings("unchecked")
    public void testEnablingAndDisabling() throws Exception {
        AtomicInteger clientCalledCount = new AtomicInteger();
        doAnswer(invocation -> {
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            clientCalledCount.incrementAndGet();
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());
        when(clusterService.state()).thenReturn(null);

        // Ensure that there are no issues if the cluster state hasn't been initialized yet
        localHealthMonitor.setEnabled(true);
        assertThat(mockHealthTracker.getLastDeterminedHealth(), nullValue());
        assertThat(clientCalledCount.get(), equalTo(0));

        when(clusterService.state()).thenReturn(clusterState);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(mockHealthTracker.getLastDeterminedHealth(), equalTo(GREEN)));
        assertBusy(() -> assertThat(clientCalledCount.get(), equalTo(1)));

        DiskHealthInfo nextHealthStatus = new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD);

        // Disable the local monitoring
        localHealthMonitor.setEnabled(false);
        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(10));
        mockHealthTracker.setHealthInfo(nextHealthStatus);
        assertThat(clientCalledCount.get(), equalTo(1));

        localHealthMonitor.setEnabled(true);
        assertBusy(() -> assertThat(mockHealthTracker.getLastDeterminedHealth(), equalTo(nextHealthStatus)));
    }

    /**
     * This test verifies that the local health monitor is able to deal with the more complex situation where it is forced to restart
     * (due to a health node change) while there is an in-flight request to the previous health node.
     */
    public void testResetDuringInFlightRequest() throws Exception {
        ClusterState initialState = ClusterStateCreationUtils.state(node, node, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        ClusterState newState = ClusterStateCreationUtils.state(node, frozenNode, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        when(clusterService.state()).thenReturn(initialState);

        var requestCounter = new AtomicInteger();
        doAnswer(invocation -> {
            var diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            assertThat(diskHealthInfo, equalTo(GREEN));
            var currentValue = requestCounter.incrementAndGet();
            // We only want to switch the health node during the first request. Any following request(s) should simply succeed.
            if (currentValue == 1) {
                when(clusterService.state()).thenReturn(newState);
                localHealthMonitor.clusterChanged(new ClusterChangedEvent("health-node-switch", newState, initialState));
            }
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(10));
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("start-up", initialState, ClusterState.EMPTY_STATE));
        // Assert that we've sent the update request twice, even though the health info itself hasn't changed (i.e. we send again due to
        // the health node change).
        assertBusy(() -> assertThat(requestCounter.get(), equalTo(2)));
    }

    /**
     * The aim of this test is to rapidly fire off a series of state changes and make sure that the health node in the last cluster
     * state actually gets the health info.
     */
    public void testRapidStateChanges() throws Exception {
        ClusterState state = ClusterStateCreationUtils.state(node, node, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        doReturn(state).when(clusterService).state();

        // Keep track of the "current" health node.
        var currentHealthNode = new AtomicReference<>(node);
        // Keep a list of all the health nodes that have received a request.
        var updatedHealthNodes = new ArrayList<DiscoveryNode>();
        doAnswer(invocation -> {
            var diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            assertThat(diskHealthInfo, equalTo(GREEN));
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            updatedHealthNodes.add(currentHealthNode.get());
            return null;
        }).when(client).execute(any(), any(), any());

        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(0));
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("start-up", state, ClusterState.EMPTY_STATE));

        int count = randomIntBetween(10, 20);
        for (int i = 0; i < count; i++) {
            var previous = state;
            state = mutateState(previous);
            currentHealthNode.set(HealthNode.findHealthNode(state));
            localHealthMonitor.clusterChanged(new ClusterChangedEvent("switch", state, previous));
        }

        var lastHealthNode = DiscoveryNodeUtils.create("health-node", "health-node");
        var previous = state;
        state = ClusterStateCreationUtils.state(
            node,
            previous.nodes().getMasterNode(),
            lastHealthNode,
            new DiscoveryNode[] { node, frozenNode, lastHealthNode }
        ).copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        currentHealthNode.set(lastHealthNode);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("switch", state, previous));

        assertBusy(() -> assertTrue(updatedHealthNodes.contains(lastHealthNode)));
    }

    private ClusterState mutateState(ClusterState previous) {
        var masterNode = previous.nodes().getMasterNode();
        var healthNode = HealthNode.findHealthNode(previous);
        var randomNode = DiscoveryNodeUtils.create(randomAlphaOfLength(10), randomAlphaOfLength(10));
        switch (randomInt(1)) {
            case 0 -> masterNode = randomValueOtherThan(masterNode, () -> randomFrom(node, frozenNode, randomNode));
            case 1 -> healthNode = randomValueOtherThan(healthNode, () -> randomFrom(node, frozenNode, randomNode));
        }
        return ClusterStateCreationUtils.state(node, masterNode, healthNode, new DiscoveryNode[] { node, frozenNode, randomNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
    }

    /**
     * The aim of this test is to change the health of the health tracker several times and make sure that every change is sent to the
     * health node (especially the last change).
     */
    public void testChangingHealth() throws Exception {
        // Keep a list of disk health info's that we've seen.
        var sentHealthInfos = new ArrayList<DiskHealthInfo>();
        doAnswer(invocation -> {
            var diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onResponse(null);
            sentHealthInfos.add(diskHealthInfo);
            return null;
        }).when(client).execute(any(), any(), any());

        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(0));
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("initialize", clusterState, ClusterState.EMPTY_STATE));
        // Make sure the initial health value has been registered.
        assertBusy(() -> assertFalse(sentHealthInfos.isEmpty()));

        var previousHealthInfo = mockHealthTracker.healthInfo;
        var healthChanges = new AtomicInteger(1);
        int count = randomIntBetween(10, 20);
        for (int i = 0; i < count; i++) {
            var newHealthInfo = randomFrom(GREEN, YELLOW);
            mockHealthTracker.setHealthInfo(newHealthInfo);
            // Check whether the health node has changed. If so, we're going to wait for it to be sent to the health node.
            healthChanges.addAndGet(newHealthInfo.equals(previousHealthInfo) ? 0 : 1);
            assertBusy(() -> assertEquals(healthChanges.get(), sentHealthInfos.size()));
            previousHealthInfo = newHealthInfo;
        }

        mockHealthTracker.setHealthInfo(RED);
        assertBusy(() -> assertTrue(sentHealthInfos.contains(RED)));
    }

    private static class MockHealthTracker extends HealthTracker<DiskHealthInfo> {
        private volatile DiskHealthInfo healthInfo = GREEN;

        @Override
        protected DiskHealthInfo determineCurrentHealth() {
            return healthInfo;
        }

        @Override
        protected void addToRequestBuilder(UpdateHealthInfoCacheAction.Request.Builder builder, DiskHealthInfo healthInfo) {
            builder.diskHealthInfo(healthInfo);
        }

        public void setHealthInfo(DiskHealthInfo healthInfo) {
            this.healthInfo = healthInfo;
        }
    }
}
