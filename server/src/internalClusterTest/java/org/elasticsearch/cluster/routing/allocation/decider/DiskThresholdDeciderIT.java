/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
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
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.RestoreInfo;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestIssueLogging;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.cluster.routing.RoutingNodesHelper.numberOfShardsWithState;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.in;
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

        final InternalClusterInfoService clusterInfoService = getInternalClusterInfoService();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            ClusterInfoServiceUtils.refresh(clusterInfoService);
        });

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(6, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms").build());
        var shardSizes = createReasonableSizedShards(indexName);

        // reduce disk size of node 0 so that no shards fit below the high watermark, forcing all shards onto the other data node
        // (subtract the translog size since the disk threshold decider ignores this and may therefore move the shard back again)
        getTestFileStore(dataNodeName).setTotalSpace(shardSizes.getSmallestShardSize() + WATERMARK_BYTES - 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, empty());

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        getTestFileStore(dataNodeName).setTotalSpace(shardSizes.getSmallestShardSize() + WATERMARK_BYTES);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, contains(in(shardSizes.getSmallestShardIds())));
    }

    public void testRestoreSnapshotAllocationDoesNotExceedWatermark() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo")
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()))
        );

        final AtomicBoolean allowRelocations = new AtomicBoolean(true);
        final InternalClusterInfoService clusterInfoService = getInternalClusterInfoService();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            ClusterInfoServiceUtils.refresh(clusterInfoService);
            if (allowRelocations.get() == false) {
                assertThat(
                    "Expects no relocating shards but got: " + event.state().getRoutingNodes(),
                    numberOfShardsWithState(event.state().getRoutingNodes(), ShardRoutingState.RELOCATING),
                    equalTo(0)
                );
            }
        });

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(6, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms").build());
        var shardSizes = createReasonableSizedShards(indexName);

        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertAcked(indicesAdmin().prepareDelete(indexName).get());
        updateClusterSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString()));
        allowRelocations.set(false);

        // reduce disk size of node 0 so that no shards fit below the low watermark, forcing shards to be assigned to the other data node
        getTestFileStore(dataNodeName).setTotalSpace(shardSizes.getSmallestShardSize() + WATERMARK_BYTES - 1L);
        refreshDiskUsage();

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), is(0));

        assertThat(getShardIds(dataNode0Id, indexName), empty());

        allowRelocations.set(true);
        updateClusterSettings(Settings.builder().putNull(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey()));

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        getTestFileStore(dataNodeName).setTotalSpace(shardSizes.getSmallestShardSize() + WATERMARK_BYTES);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, contains(in(shardSizes.getSmallestShardIds())));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/105331")
    @TestIssueLogging(
        value = "org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceComputer:TRACE,"
            + "org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceReconciler:DEBUG,"
            + "org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceShardsAllocator:TRACE,"
            + "org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator:TRACE,"
            + "org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders:TRACE,"
            + "org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider:TRACE",
        issueUrl = "https://github.com/elastic/elasticsearch/issues/105331"
    )
    public void testRestoreSnapshotAllocationDoesNotExceedWatermarkWithMultipleShards() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo")
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()))
        );

        final AtomicBoolean allowRelocations = new AtomicBoolean(true);
        final InternalClusterInfoService clusterInfoService = getInternalClusterInfoService();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            ClusterInfoServiceUtils.refresh(clusterInfoService);
            if (allowRelocations.get() == false) {
                assertThat(
                    "Expects no relocating shards but got: " + event.state().getRoutingNodes(),
                    numberOfShardsWithState(event.state().getRoutingNodes(), ShardRoutingState.RELOCATING),
                    equalTo(0)
                );
            }
        });

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(6, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms").build());
        var shardSizes = createReasonableSizedShards(indexName);

        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertAcked(indicesAdmin().prepareDelete(indexName).get());
        updateClusterSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString()));
        allowRelocations.set(false);

        // reduce disk size of node 0 so that only 1 of 2 smallest shards can be allocated
        var usableSpace = shardSizes.sizes().get(1).size();
        getTestFileStore(dataNodeName).setTotalSpace(usableSpace + WATERMARK_BYTES);
        refreshDiskUsage();

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), is(0));

        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, contains(in(shardSizes.getShardIdsWithSizeSmallerOrEqual(usableSpace))));
    }

    private Set<ShardId> getShardIds(final String nodeId, final String indexName) {
        final Set<ShardId> shardIds = new HashSet<>();
        final IndexRoutingTable indexRoutingTable = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setRoutingTable(true)
            .get()
            .getState()
            .getRoutingTable()
            .index(indexName);
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            final IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < shardRoutingTable.size(); copy++) {
                ShardRouting shard = shardRoutingTable.shard(copy);
                assertThat(shard.state(), equalTo(ShardRoutingState.STARTED));
                if (shard.currentNodeId().equals(nodeId)) {
                    shardIds.add(removeIndexUUID(shard.shardId()));
                }
            }
        }
        return shardIds;
    }

    /**
     * Index documents until all the shards are at least WATERMARK_BYTES in size.
     * @return the shard sizes.
     */
    private ShardSizes createReasonableSizedShards(final String indexName) {
        ShardStats[] shardStats = indexAllShardsToAnEqualOrGreaterMinimumSize(indexName, WATERMARK_BYTES);
        var shardSizes = Arrays.stream(shardStats)
            .map(it -> new ShardSize(removeIndexUUID(it.getShardRouting().shardId()), it.getStats().getStore().sizeInBytes()))
            .sorted(Comparator.comparing(ShardSize::size))
            .toList();
        logger.info("Created shards with sizes {}", shardSizes);
        return new ShardSizes(shardSizes);
    }

    private record ShardSizes(List<ShardSize> sizes) {

        public long getSmallestShardSize() {
            return sizes.get(0).size();
        }

        public Set<ShardId> getShardIdsWithSizeSmallerOrEqual(long size) {
            return sizes.stream().filter(entry -> entry.size <= size).map(ShardSize::shardId).collect(toSet());
        }

        public Set<ShardId> getSmallestShardIds() {
            return getShardIdsWithSizeSmallerOrEqual(getSmallestShardSize());
        }
    }

    private record ShardSize(ShardId shardId, long size) {}

    private static ShardId removeIndexUUID(ShardId shardId) {
        return ShardId.fromString(shardId.toString());
    }

    private void refreshDiskUsage() {
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        var clusterInfo = ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
        logger.info("Refreshed cluster info: {}", clusterInfo);
        // if the nodes were all under the low watermark already (but unbalanced) then a change in the disk usage doesn't trigger a reroute
        // even though it's now possible to achieve better balance, so we have to do an explicit reroute. TODO fix this?
        if (clusterInfoService.getClusterInfo()
            .getNodeMostAvailableDiskUsages()
            .values()
            .stream()
            .allMatch(e -> e.freeBytes() > WATERMARK_BYTES)) {
            ClusterRerouteUtils.reroute(client());
        }

        assertFalse(
            clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .setWaitForNoInitializingShards(true)
                .get()
                .isTimedOut()
        );
    }

    private void assertBusyWithDiskUsageRefresh(String nodeId, String indexName, Matcher<? super Set<ShardId>> matcher) throws Exception {
        assertBusy(() -> {
            // refresh the master's ClusterInfoService before checking the assigned shards because DiskThresholdMonitor might still
            // be processing a previous ClusterInfo update and will skip the new one (see DiskThresholdMonitor#onNewInfo(ClusterInfo)
            // and its internal checkInProgress flag)
            refreshDiskUsage();

            final Set<ShardId> shardRoutings = getShardIds(nodeId, indexName);
            assertThat("Mismatching shard routings: " + shardRoutings, shardRoutings, matcher);
        }, 5L, TimeUnit.SECONDS);
    }

    private InternalClusterInfoService getInternalClusterInfoService() {
        return (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
    }
}
