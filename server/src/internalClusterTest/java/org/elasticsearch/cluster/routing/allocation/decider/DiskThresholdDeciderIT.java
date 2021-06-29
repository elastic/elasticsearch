/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.Rebalance;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DiskThresholdDeciderIT extends DiskUsageIntegTestCase {

    private static final long WATERMARK_BYTES = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal, otherSettings))
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
                .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_REROUTE_INTERVAL_SETTING.getKey(), "0ms")
                .build();
    }

    public void testHighWatermarkNotExceeded() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        final InternalClusterInfoService clusterInfoService
                = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(
                event -> ClusterInfoServiceUtils.refresh(clusterInfoService));

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
                .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                .build());
        final long minShardSize = createReasonableSizedShards(indexName);

        // reduce disk size of node 0 so that no shards fit below the high watermark, forcing all shards onto the other data node
        // (subtract the translog size since the disk threshold decider ignores this and may therefore move the shard back again)
        getTestFileStore(dataNodeName).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, empty());

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        getTestFileStore(dataNodeName).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, hasSize(1));
    }

    public void testRestoreSnapshotAllocationDoesNotExceedWatermark() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        assertAcked(client().admin().cluster().preparePutRepository("repo")
            .setType(FsRepository.TYPE)
            .setSettings(Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", randomBoolean())));

        final InternalClusterInfoService clusterInfoService
            = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(
                event -> ClusterInfoServiceUtils.refresh(clusterInfoService));

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 6)
            .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
            .build());
        final long minShardSize = createReasonableSizedShards(indexName);

        final CreateSnapshotResponse createSnapshotResponse = client().admin().cluster().prepareCreateSnapshot("repo", "snap")
            .setWaitForCompletion(true).get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertAcked(client().admin().indices().prepareDelete(indexName).get());

        // reduce disk size of node 0 so that no shards fit below the low watermark, forcing shards to be assigned to the other data node
        getTestFileStore(dataNodeName).setTotalSpace(minShardSize + WATERMARK_BYTES - 1L);
        refreshDiskUsage();

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString())
                .build())
            .get());

        final RestoreSnapshotResponse restoreSnapshotResponse = client().admin().cluster().prepareRestoreSnapshot("repo", "snap")
            .setWaitForCompletion(true).get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), is(0));

        assertBusy(() -> assertThat(getShardRoutings(dataNode0Id, indexName), empty()));

        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .putNull(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey())
                .build())
            .get());

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        getTestFileStore(dataNodeName).setTotalSpace(minShardSize + WATERMARK_BYTES + 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, hasSize(1));
    }

    private Set<ShardRouting> getShardRoutings(final String nodeId, final String indexName) {
        final Set<ShardRouting> shardRoutings = new HashSet<>();
        for (IndexShardRoutingTable indexShardRoutingTable : client().admin().cluster().prepareState().clear().setRoutingTable(true)
                .get().getState().getRoutingTable().index(indexName)) {
            for (ShardRouting shard : indexShardRoutingTable.shards()) {
                assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
                if (shard.currentNodeId().equals(nodeId)) {
                    shardRoutings.add(shard);
                }
            }
        }
        return shardRoutings;
    }

    /**
     * Index documents until all the shards are at least WATERMARK_BYTES in size, and return the size of the smallest shard
     */
    private long createReasonableSizedShards(final String indexName) throws InterruptedException {
        while (true) {
            final IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[scaledRandomIntBetween(100, 10000)];
            for (int i = 0; i < indexRequestBuilders.length; i++) {
                indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", randomAlphaOfLength(10));
            }
            indexRandom(true, indexRequestBuilders);
            forceMerge();
            refresh();

            final ShardStats[] shardStatses = client().admin().indices().prepareStats(indexName)
                    .clear().setStore(true).setTranslog(true).get().getShards();
            final long[] shardSizes = new long[shardStatses.length];
            for (ShardStats shardStats : shardStatses) {
                shardSizes[shardStats.getShardRouting().id()] = shardStats.getStats().getStore().sizeInBytes();
            }

            final long minShardSize = Arrays.stream(shardSizes).min().orElseThrow(() -> new AssertionError("no shards"));
            if (minShardSize > WATERMARK_BYTES) {
                return minShardSize;
            }
        }
    }

    private void refreshDiskUsage() {
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
        // if the nodes were all under the low watermark already (but unbalanced) then a change in the disk usage doesn't trigger a reroute
        // even though it's now possible to achieve better balance, so we have to do an explicit reroute. TODO fix this?
        if (StreamSupport.stream(clusterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().values().spliterator(), false)
            .allMatch(cur -> cur.value.getFreeBytes() > WATERMARK_BYTES)) {
            assertAcked(client().admin().cluster().prepareReroute());
        }

        assertFalse(client().admin().cluster().prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNoRelocatingShards(true)
            .setWaitForNoInitializingShards(true)
            .get()
            .isTimedOut());
    }

    private void assertBusyWithDiskUsageRefresh(
        String nodeName,
        String indexName,
        Matcher<? super Set<ShardRouting>> matcher
    ) throws Exception {
        assertBusy(() -> {
            // refresh the master's ClusterInfoService before checking the assigned shards because DiskThresholdMonitor might still
            // be processing a previous ClusterInfo update and will skip the new one (see DiskThresholdMonitor#onNewInfo(ClusterInfo)
            // and its internal checkInProgress flag)
            refreshDiskUsage();

            final Set<ShardRouting> shardRoutings = getShardRoutings(nodeName, indexName);
            assertThat("Mismatching shard routings: " + shardRoutings, shardRoutings, matcher);
        }, 30L, TimeUnit.SECONDS);
    }
}
