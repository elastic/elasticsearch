/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.tracker;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.RelativeByteSizeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.metadata.HealthMetadata;
import org.elasticsearch.health.node.DiskHealthInfo;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Set;

import static org.elasticsearch.action.support.replication.ClusterStateCreationUtils.state;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiskHealthTrackerTests extends ESTestCase {

    private static final DiskHealthInfo GREEN = new DiskHealthInfo(HealthStatus.GREEN, null);
    private NodeService nodeService;
    private ClusterService clusterService;
    private DiscoveryNode node;
    private DiscoveryNode frozenNode;
    private DiscoveryNode searchNode;
    private DiscoveryNode searchAndIndexNode;
    private HealthMetadata healthMetadata;
    private ClusterState clusterState;
    private DiskHealthTracker diskHealthTracker;

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
        searchNode = DiscoveryNodeUtils.builder("search-node").name("search-node").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
        searchAndIndexNode = DiscoveryNodeUtils.builder("search-and-index-node")
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
        nodeService = mock(NodeService.class);

        diskHealthTracker = new DiskHealthTracker(nodeService, clusterService);
    }

    public void testNoDiskData() {
        when(
            nodeService.stats(
                eq(CommonStatsFlags.NONE),
                eq(false),
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
                eq(false),
                eq(false)
            )
        ).thenReturn(nodeStats());
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(new DiskHealthInfo(HealthStatus.UNKNOWN, DiskHealthInfo.Cause.NODE_HAS_NO_DISK_STATS), diskHealth);
    }

    public void testGreenDiskStatus() {
        simulateHealthDiskSpace();
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(GREEN, diskHealth);
    }

    public void testYellowDiskStatus() {
        initializeIncreasedDiskSpaceUsage();
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD), diskHealth);
    }

    public void testRedDiskStatus() {
        simulateDiskOutOfSpace();
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.NODE_OVER_THE_FLOOD_STAGE_THRESHOLD), diskHealth);
    }

    public void testFrozenGreenDiskStatus() {
        simulateHealthDiskSpace();
        ClusterState clusterStateFrozenLocalNode = clusterState.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(frozenNode).localNodeId(frozenNode.getId()).build())
        );
        when(clusterService.state()).thenReturn(clusterStateFrozenLocalNode);
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(GREEN, diskHealth);
    }

    public void testFrozenRedDiskStatus() {
        simulateDiskOutOfSpace();
        ClusterState clusterStateFrozenLocalNode = clusterState.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(frozenNode).localNodeId(frozenNode.getId()).build())
        );
        when(clusterService.state()).thenReturn(clusterStateFrozenLocalNode);
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD), diskHealth);
    }

    public void testSearchNodeGreenDiskStatus() {
        // Search-only nodes behave like frozen nodes -- they are RED at 95% full, GREEN otherwise.
        initializeIncreasedDiskSpaceUsage();
        ClusterState clusterStateSearchLocalNode = clusterState.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(searchNode).localNodeId(searchNode.getId()).build())
        );
        when(clusterService.state()).thenReturn(clusterStateSearchLocalNode);
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(GREEN, diskHealth);
    }

    public void testSearchNodeRedDiskStatus() {
        // Search-only nodes behave like frozen nodes -- they are RED at 95% full, GREEN otherwise.
        simulateDiskOutOfSpace();
        ClusterState clusterStateSearchLocalNode = clusterState.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(searchNode).localNodeId(searchNode.getId()).build())
        );
        when(clusterService.state()).thenReturn(clusterStateSearchLocalNode);
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(new DiskHealthInfo(HealthStatus.RED, DiskHealthInfo.Cause.FROZEN_NODE_OVER_FLOOD_STAGE_THRESHOLD), diskHealth);
    }

    public void testSearchAndIndexNodesYellowDiskStatus() {
        // A search role mixed with another data node role behaves like an ordinary data node -- YELLOW at 90% full.
        initializeIncreasedDiskSpaceUsage();
        ClusterState clusterStateSearchLocalNode = clusterState.copyAndUpdate(
            b -> b.nodes(DiscoveryNodes.builder().add(node).add(searchAndIndexNode).localNodeId(searchAndIndexNode.getId()).build())
        );
        when(clusterService.state()).thenReturn(clusterStateSearchLocalNode);
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD), diskHealth);
    }

    public void testYellowStatusForNonDataNode() {
        DiscoveryNode dedicatedMasterNode = DiscoveryNodeUtils.builder("master-node-1")
            .name("master-node")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
            .build();
        clusterState = ClusterStateCreationUtils.state(
            dedicatedMasterNode,
            dedicatedMasterNode,
            node,
            new DiscoveryNode[] { node, dedicatedMasterNode }
        ).copyAndUpdate(b -> b.putCustom(HealthMetadata.TYPE, healthMetadata));

        initializeIncreasedDiskSpaceUsage();
        DiskHealthInfo diskHealth = diskHealthTracker.checkCurrentHealth();
        assertEquals(new DiskHealthInfo(HealthStatus.YELLOW, DiskHealthInfo.Cause.NODE_OVER_HIGH_THRESHOLD), diskHealth);
    }

    public void testHasRelocatingShards() {
        String indexName = "my-index";
        final ClusterState state = state(indexName, true, ShardRoutingState.RELOCATING);
        // local node coincides with the node hosting the (relocating) primary shard
        DiscoveryNode localNode = state.nodes().getLocalNode();
        assertTrue(DiskHealthTracker.hasRelocatingShards(state, localNode));

        DiscoveryNode dedicatedMasterNode = DiscoveryNodeUtils.builder("master-node-1")
            .name("master-node")
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
            .build();
        ClusterState newState = ClusterState.builder(state)
            .nodes(new DiscoveryNodes.Builder(state.nodes()).add(dedicatedMasterNode))
            .build();
        assertFalse(DiskHealthTracker.hasRelocatingShards(newState, dedicatedMasterNode));
    }

    private void simulateDiskOutOfSpace() {
        when(
            nodeService.stats(
                eq(CommonStatsFlags.NONE),
                eq(false),
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
            null,
            null,
            null
        );
    }
}
