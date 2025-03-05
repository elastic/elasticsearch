/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.tasks.TaskResultsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.NodeRoles;

import java.util.Locale;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING;
import static org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING;
import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskThresholdMonitorIT extends DiskUsageIntegTestCase {

    private static final long FLOOD_STAGE_BYTES = ByteSizeValue.of(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), FLOOD_STAGE_BYTES * 2 + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), FLOOD_STAGE_BYTES * 2 + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), FLOOD_STAGE_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
            .build();
    }

    public void testFloodStageExceeded() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeName)
                .build()
        );
        // ensure we have a system index on the data node too.
        assertAcked(indicesAdmin().prepareCreate(TaskResultsService.TASK_INDEX));

        getTestFileStore(dataNodeName).setTotalSpace(1L);
        refreshClusterInfo();
        assertBusy(() -> {
            assertBlocked(prepareIndex(indexName).setId("1").setSource("f", "g"), IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
            assertThat(getIndexBlock(indexName, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE), equalTo("true"));
        });

        // Verify that we can adjust things like allocation filters even while blocked
        updateIndexSettings(
            Settings.builder().putNull(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey()),
            indexName
        );

        // Verify that we can still move shards around even while blocked
        final String newDataNodeName = internalCluster().startDataOnlyNode();
        final String newDataNodeId = clusterAdmin().prepareNodesInfo(newDataNodeName).get().getNodes().get(0).getNode().getId();
        assertBusy(() -> {
            final ShardRouting primaryShard = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
                .clear()
                .setRoutingTable(true)
                .setNodes(true)
                .setIndices(indexName)
                .get()
                .getState()
                .routingTable()
                .index(indexName)
                .shard(0)
                .primaryShard();
            assertThat(primaryShard.state(), equalTo(ShardRoutingState.STARTED));
            assertThat(primaryShard.currentNodeId(), equalTo(newDataNodeId));
        });

        // Verify that the block is removed once the shard migration is complete
        refreshClusterInfo();
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get().isTimedOut());
        assertNull(getIndexBlock(indexName, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE));
    }

    public void testRemoveExistingIndexBlocksWhenDiskThresholdMonitorIsDisabled() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .put(INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(), dataNodeName)
                .build()
        );
        // ensure we have a system index on the data node too.
        assertAcked(indicesAdmin().prepareCreate(TaskResultsService.TASK_INDEX));

        getTestFileStore(dataNodeName).setTotalSpace(1L);
        refreshClusterInfo();
        assertBusy(() -> {
            assertBlocked(prepareIndex(indexName).setId("1").setSource("f", "g"), IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK);
            assertThat(getIndexBlock(indexName, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE), equalTo("true"));
        });

        // Disable disk threshold monitoring
        updateClusterSettings(
            Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), false)
        );

        // Verify that the block is removed
        refreshClusterInfo();
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get().isTimedOut());
        assertNull(getIndexBlock(indexName, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE));

        // Re-enable and the blocks should be back!
        updateClusterSettings(
            Settings.builder().put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_THRESHOLD_ENABLED_SETTING.getKey(), true)
        );
        refreshClusterInfo();
        assertFalse(clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT).setWaitForEvents(Priority.LANGUID).get().isTimedOut());
        assertThat(getIndexBlock(indexName, IndexMetadata.SETTING_READ_ONLY_ALLOW_DELETE), equalTo("true"));
    }

    public void testNodeStatsIncludingDiskThreshold() {
        Settings.Builder masterNodeSettings = Settings.builder();
        masterNodeSettings.put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
            .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "80%")
            .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(), "90%");

        internalCluster().startMasterOnlyNode(masterNodeSettings.build());
        final String dataNodeName = internalCluster().startDataOnlyNode();
        final String frozenNodeName = internalCluster().startNode(NodeRoles.onlyRole(DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE));

        getTestFileStore(dataNodeName).setTotalSpace(10000L);
        getTestFileStore(frozenNodeName).setTotalSpace(20000L);

        // no cluster settings, but master node's yml watermark threshold is different, data node stats based on master node's thresholds
        NodesStatsResponse nodeStats = clusterAdmin().prepareNodesStats().setAllocationStats(true).setFs(true).get();
        NodeStats dataNode = nodeStats.getNodes().stream().filter(n -> dataNodeName.equals(n.getNode().getName())).findFirst().get();
        FsInfo.Path path = dataNode.getFs().iterator().next();
        assertEquals(4000, path.getLowWatermarkFreeSpace().getBytes());
        assertEquals(3000, path.getHighWatermarkFreeSpace().getBytes());
        assertEquals(2000, path.getFloodStageWatermarkFreeSpace().getBytes());
        assertNull(path.getFrozenFloodStageWatermarkFreeSpace());

        NodeStats frozenNode = nodeStats.getNodes().stream().filter(n -> frozenNodeName.equals(n.getNode().getName())).findFirst().get();
        path = frozenNode.getFs().iterator().next();
        assertEquals(8000, path.getLowWatermarkFreeSpace().getBytes());
        assertEquals(6000, path.getHighWatermarkFreeSpace().getBytes());
        assertEquals(4000, path.getFloodStageWatermarkFreeSpace().getBytes());
        assertEquals(2000, path.getFrozenFloodStageWatermarkFreeSpace().getBytes());

        // update dynamic cluster settings, use absolute value, both data and master node have the same thresholds
        updateClusterSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "3000b")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "2000b")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1000b")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_FROZEN_WATERMARK_SETTING.getKey(), "500b")
        );

        nodeStats = clusterAdmin().prepareNodesStats().setAllocationStats(true).setFs(true).get();
        dataNode = nodeStats.getNodes().stream().filter(n -> dataNodeName.equals(n.getNode().getName())).findFirst().get();
        path = dataNode.getFs().iterator().next();
        assertEquals(3000, path.getLowWatermarkFreeSpace().getBytes());
        assertEquals(2000, path.getHighWatermarkFreeSpace().getBytes());
        assertEquals(1000, path.getFloodStageWatermarkFreeSpace().getBytes());
        assertNull(path.getFrozenFloodStageWatermarkFreeSpace());

        frozenNode = nodeStats.getNodes().stream().filter(n -> frozenNodeName.equals(n.getNode().getName())).findFirst().get();
        path = frozenNode.getFs().iterator().next();
        assertEquals(3000, path.getLowWatermarkFreeSpace().getBytes());
        assertEquals(2000, path.getHighWatermarkFreeSpace().getBytes());
        assertEquals(1000, path.getFloodStageWatermarkFreeSpace().getBytes());
        assertEquals(500, path.getFrozenFloodStageWatermarkFreeSpace().getBytes());

        // effective threshold percent calculated based on headroom
        updateClusterSettings(
            Settings.builder()
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "60%")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "70%")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "80%")
                .put(CLUSTER_ROUTING_ALLOCATION_LOW_DISK_MAX_HEADROOM_SETTING.getKey(), "500b")
                .put(CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_MAX_HEADROOM_SETTING.getKey(), "300b")
                .put(CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_MAX_HEADROOM_SETTING.getKey(), "100b")
        );

        nodeStats = clusterAdmin().prepareNodesStats().setAllocationStats(true).setFs(true).get();
        dataNode = nodeStats.getNodes().stream().filter(n -> dataNodeName.equals(n.getNode().getName())).findFirst().get();
        path = dataNode.getFs().iterator().next();
        assertEquals(500, path.getLowWatermarkFreeSpace().getBytes());
        assertEquals(300, path.getHighWatermarkFreeSpace().getBytes());
        assertEquals(100, path.getFloodStageWatermarkFreeSpace().getBytes());
    }

    // Retrieves the value of the given block on an index.
    private static String getIndexBlock(String indexName, String blockName) {
        return indicesAdmin().prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
            .setNames(blockName)
            .get()
            .getSetting(indexName, blockName);
    }

}
