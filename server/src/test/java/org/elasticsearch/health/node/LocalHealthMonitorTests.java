/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LocalHealthMonitorTests extends ESTestCase {

    private static ThreadPool threadPool;
    private NodeService nodeService;
    private ClusterService clusterService;
    private DiscoveryNode node;
    private DiscoveryNode frozenNode;
    private HealthMetadata healthMetadata;
    private ClusterState clusterState;

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
                .build()
        );
        node = new DiscoveryNode(
            "node",
            "node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        frozenNode = new DiscoveryNode(
            "frozen-node",
            "frozen-node",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            Set.of(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE),
            Version.CURRENT
        );
        clusterState = ClusterState.EMPTY_STATE.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(frozenNode).localNodeId(node.getId()).build())
        ).copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        // Set-up cluster service
        clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
        when(clusterService.state()).thenReturn(clusterState);

        // Set-up node service with a node with a healthy disk space usage
        nodeService = mock(NodeService.class);
    }

    public void testUpdateNodeHealthStatus() {
        simulateHealthDiskSpace();
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, nodeService, threadPool);
        assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), nullValue());
        localHealthMonitor.monitorHealth();
        assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(new DiskHealthInfo(HealthStatus.GREEN, null)));
    }

    public void testEnablingAndDisabling() throws Exception {
        simulateHealthDiskSpace();
        DiskHealthInfo healthyNode = new DiskHealthInfo(HealthStatus.GREEN, null);
        when(clusterService.state()).thenReturn(null);
        LocalHealthMonitor localHealthMonitor = LocalHealthMonitor.create(Settings.EMPTY, clusterService, nodeService, threadPool);

        // Ensure that there are no issues if the cluster state hasn't been initialized yet
        localHealthMonitor.setEnabled(true);
        assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), nullValue());

        when(clusterService.state()).thenReturn(clusterState);
        localHealthMonitor.clusterChanged(new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE));
        assertBusy(() -> assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(healthyNode)));

        // Disable the local monitoring
        localHealthMonitor.setEnabled(false);
        localHealthMonitor.setMonitorInterval(TimeValue.timeValueMillis(1));
        simulateDiskOutOfSpace();
        assertRemainsUnchanged(localHealthMonitor::getLastReportedDiskHealthInfo, healthyNode);

        localHealthMonitor.setEnabled(true);
        DiskHealthInfo nextHealthStatus = new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD);
        assertBusy(() -> assertThat(localHealthMonitor.getLastReportedDiskHealthInfo(), equalTo(nextHealthStatus)));
    }

    private void assertRemainsUnchanged(Supplier<DiskHealthInfo> supplier, DiskHealthInfo expected) {
        expectThrows(AssertionError.class, () -> assertBusy(() -> assertThat(supplier.get(), not(expected)), 1, TimeUnit.SECONDS));
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
        assertThat(diskHealth, equalTo(new DiskHealthInfo(HealthStatus.GREEN, null)));
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
        assertThat(diskHealth, equalTo(new DiskHealthInfo(HealthStatus.GREEN, null)));
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
