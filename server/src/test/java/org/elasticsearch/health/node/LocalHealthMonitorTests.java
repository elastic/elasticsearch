/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.selection.HealthNodeExecutorTests;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalHealthMonitorTests extends ESTestCase {

    private static final DiskHealthInfo GREEN = new DiskHealthInfo(HealthStatus.GREEN, null);
    private static ThreadPool threadPool;
    private NodeService nodeService;
    private ClusterService clusterService;
    private DiscoveryNode node;
    private DiscoveryNode frozenNode;
    private HealthMetadata healthMetadata;
    private ClusterState clusterState;
    private Client client;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(HealthNodeExecutorTests.class.getSimpleName());
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
        node = TestDiscoveryNode.create("node", "node");
        frozenNode = TestDiscoveryNode.create(
            "frozen-node",
            "frozen-node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE)
        );
        clusterState = ClusterStateCreationUtils.state(node, node, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        // Set-up cluster service
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterService.state()).thenReturn(clusterState);
        when(clusterService.localNode()).thenReturn(node);

        // Set-up node service with a node with a healthy disk space usage
        nodeService = mock(NodeService.class);

        client = mock(Client.class);
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
        simulateHealthDiskSpace();
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, nodeService, threadPool, client);
        // We override the poll interval like this to avoid the min value set by the setting which is too high for this test
        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(10));
        assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), nullValue());
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("initialize", clusterState, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(GREEN)));
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

        simulateHealthDiskSpace();
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, nodeService, threadPool, client);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("initialize", clusterState, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(clientCalled.get(), equalTo(true)));
        assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testSendHealthInfoToNewNode() throws Exception {
        ClusterState previous = ClusterStateCreationUtils.state(node, node, frozenNode, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        ClusterState current = ClusterStateCreationUtils.state(node, node, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        simulateHealthDiskSpace();

        AtomicInteger counter = new AtomicInteger(0);
        doAnswer(invocation -> {
            DiskHealthInfo diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertThat(diskHealthInfo, equalTo(GREEN));
            counter.incrementAndGet();
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        when(clusterService.state()).thenReturn(previous);
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, nodeService, threadPool, client);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("start-up", previous, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(GREEN)));
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("health-node-switch", current, previous));
        assertBusy(() -> assertThat(counter.get(), equalTo(2)));
    }

    @SuppressWarnings("unchecked")
    public void testResendHealthInfoOnMasterChange() throws Exception {
        ClusterState previous = ClusterStateCreationUtils.state(node, node, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        ClusterState current = ClusterStateCreationUtils.state(node, frozenNode, node, new DiscoveryNode[] { node, frozenNode })
            .copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));
        simulateHealthDiskSpace();

        AtomicInteger counter = new AtomicInteger(0);
        doAnswer(invocation -> {
            DiskHealthInfo diskHealthInfo = ((UpdateHealthInfoCacheAction.Request) invocation.getArgument(1)).getDiskHealthInfo();
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertThat(diskHealthInfo, equalTo(GREEN));
            counter.incrementAndGet();
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        when(clusterService.state()).thenReturn(previous);
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, nodeService, threadPool, client);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("start-up", previous, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(GREEN)));
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
        simulateHealthDiskSpace();
        when(clusterService.state()).thenReturn(null);
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, nodeService, threadPool, client);

        // Ensure that there are no issues if the cluster state hasn't been initialized yet
        localHealthMonitor.setEnabled(true);
        assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), nullValue());
        assertThat(clientCalledCount.get(), equalTo(0));

        when(clusterService.state()).thenReturn(clusterState);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(GREEN)));
        assertThat(clientCalledCount.get(), equalTo(1));

        // Disable the local monitoring
        localHealthMonitor.setEnabled(false);
        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(1));
        simulateDiskOutOfSpace();
        assertThat(clientCalledCount.get(), equalTo(1));
        localHealthMonitor.setMonitorInterval(TimeValue.timeValueSeconds(30));

        localHealthMonitor.setEnabled(true);
        DiskHealthInfo nextHealthStatus = new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD);
        assertBusy(() -> assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(nextHealthStatus)));
    }

    public void testNoDiskData() {
        when(
            nodeService.stats(
                eq(CommonStatsFlags.NONE),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(true),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false)
            )
        ).thenReturn(nodeStats());
        LocalHealthMonitor.DiskCheck diskCheck = new LocalHealthMonitor.DiskCheck(nodeService);
        DiskHealthInfo diskHealth = diskCheck.getHealth(healthMetadata, clusterState);
        assertThat(diskHealth, equalTo(new DiskHealthInfo(HealthStatus.UNKNOWN, DiskHealthInfo.Cause.NODE_HAS_NO_DISK_STATS)));
    }

    public void testGreenDiskStatus() {
        simulateHealthDiskSpace();
        LocalHealthMonitor.DiskCheck diskMonitor = new LocalHealthMonitor.DiskCheck(nodeService);
        DiskHealthInfo diskHealth = diskMonitor.getHealth(healthMetadata, clusterState);
        assertThat(diskHealth, equalTo(GREEN));
    }

    public void testYellowDiskStatus() {
        initializeIncreasedDiskSpaceUsage();
        LocalHealthMonitor.DiskCheck diskMonitor = new LocalHealthMonitor.DiskCheck(nodeService);
        DiskHealthInfo diskHealth = diskMonitor.getHealth(healthMetadata, clusterState);
        assertThat(diskHealth, equalTo(new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD)));
    }

    public void testRedDiskStatus() {
        simulateDiskOutOfSpace();
        LocalHealthMonitor.DiskCheck diskMonitor = new LocalHealthMonitor.DiskCheck(nodeService);
        DiskHealthInfo diskHealth = diskMonitor.getHealth(healthMetadata, clusterState);
        assertThat(diskHealth, equalTo(new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD)));
    }

    public void testFrozenGreenDiskStatus() {
        simulateHealthDiskSpace();
        ClusterState clusterStateFrozenLocalNode = clusterState.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(frozenNode).localNodeId(frozenNode.getId()).build())
        );
        LocalHealthMonitor.DiskCheck diskMonitor = new LocalHealthMonitor.DiskCheck(nodeService);
        DiskHealthInfo diskHealth = diskMonitor.getHealth(healthMetadata, clusterStateFrozenLocalNode);
        assertThat(diskHealth, equalTo(GREEN));
    }

    public void testFrozenRedDiskStatus() {
        simulateDiskOutOfSpace();
        ClusterState clusterStateFrozenLocalNode = clusterState.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(frozenNode).localNodeId(frozenNode.getId()).build())
        );
        LocalHealthMonitor.DiskCheck diskMonitor = new LocalHealthMonitor.DiskCheck(nodeService);
        DiskHealthInfo diskHealth = diskMonitor.getHealth(healthMetadata, clusterStateFrozenLocalNode);
        assertThat(diskHealth, equalTo(new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD)));
    }

    public void testYellowStatusForNonDataNode() {
        DiscoveryNode dedicatedMasterNode = TestDiscoveryNode.create(
            "master-node",
            "master-node-1",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        clusterState = ClusterStateCreationUtils.state(
            dedicatedMasterNode,
            dedicatedMasterNode,
            node,
            new DiscoveryNode[] { node, dedicatedMasterNode }
        ).copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        initializeIncreasedDiskSpaceUsage();
        LocalHealthMonitor.DiskCheck diskMonitor = new LocalHealthMonitor.DiskCheck(nodeService);
        DiskHealthInfo diskHealth = diskMonitor.getHealth(healthMetadata, clusterState);
        assertThat(diskHealth, equalTo(new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD)));
    }

    public void testHasRelocatingShards() {
        String indexName = "my-index";
        final ClusterState state = state(indexName, true, ShardRoutingState.RELOCATING);
        // local node coincides with the node hosting the (relocating) primary shard
        DiscoveryNode localNode = state.nodes().getLocalNode();
        assertThat(LocalHealthMonitor.DiskCheck.hasRelocatingShards(state, localNode), is(true));

        DiscoveryNode dedicatedMasterNode = TestDiscoveryNode.create(
            "master-node",
            "master-node-1",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        ClusterState newState = ClusterState.builder(state)
            .nodes(new DiscoveryNodes.Builder(state.nodes()).add(dedicatedMasterNode))
            .build();
        assertThat(LocalHealthMonitor.DiskCheck.hasRelocatingShards(newState, dedicatedMasterNode), is(false));
    }

    private void simulateDiskOutOfSpace() {
        when(
            nodeService.stats(
                eq(CommonStatsFlags.NONE),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(true),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false)
            )
        ).thenReturn(nodeStats(1000, 10));
    }

    private void initializeIncreasedDiskSpaceUsage() {
        when(
            nodeService.stats(
                eq(CommonStatsFlags.NONE),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(true),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false)
            )
        ).thenReturn(nodeStats(1000, 80));
    }

    private void simulateHealthDiskSpace() {
        when(
            nodeService.stats(
                eq(CommonStatsFlags.NONE),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(true),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false),
                eq(false)
            )
        ).thenReturn(nodeStats(1000, 110));
    }

    private NodeStats nodeStats(long total, long available) {
        final FsInfo fs = new FsInfo(-1, null, new FsInfo.Path[] { new FsInfo.Path(null, null, total, 10, available) });
        return nodeStats(fs);
    }

    private NodeStats nodeStats() {
        return nodeStats(null);
    }

    private NodeStats nodeStats(FsInfo fs) {
        return new NodeStats(
            node, // ignored
            randomMillisUpToYear9999(),
            null,
            null,
            null,
            null,
            null,
            fs,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
    }
}
