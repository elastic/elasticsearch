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
import org.elasticsearch.action.support.ActionTestUtils;
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
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.hamcrest.Matcher;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
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
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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

    public void testRestoreSnapshotAllocationDoesNotExceedWatermarkWithMultipleRestores() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo")
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()))
        );

        final InternalClusterInfoService clusterInfoService = getInternalClusterInfoService();
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .addListener(event -> ClusterInfoServiceUtils.refresh(clusterInfoService));

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(6, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms").build());
        final var shardSizes = createReasonableSizedShards(indexName);

        final CreateSnapshotResponse createSnapshotResponse = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertAcked(indicesAdmin().prepareDelete(indexName).get());
        updateClusterSettings(Settings.builder().put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString()));

        // Verify that from this point on we do not do any rebalancing
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class).addListener(event -> {
            assertThat(
                "Expects no relocating shards but got: " + event.state().getRoutingNodes(),
                numberOfShardsWithState(event.state().getRoutingNodes(), ShardRoutingState.RELOCATING),
                equalTo(0)
            );
        });

        // reduce disk size of one data node so that only one shard copy fits there, forcing all the other shards to be assigned to the
        // other data node
        final var usableSpace = randomLongBetween(shardSizes.getSmallestShardSize(), shardSizes.getSmallestShardSize() * 2 - 1L);
        getTestFileStore(dataNodeName).setTotalSpace(usableSpace + WATERMARK_BYTES);
        refreshDiskUsage();

        // We're going to restore the index twice in quick succession and verify that we don't assign more than one shard in total to the
        // chosen node, but to do this we have to work backwards: first we have to set up listeners to react to events and then finally we
        // trigger the whole chain by starting the first restore.
        final var copyIndexName = indexName + "-copy";

        // set up a listener that explicitly forbids more than one shard to be assigned to the tiny node
        final var dataNodeId = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();
        final var allShardsActiveListener = ClusterServiceUtils.addTemporaryStateListener(cs -> {
            assertThat(cs.getRoutingNodes().toString(), cs.getRoutingNodes().node(dataNodeId).size(), lessThanOrEqualTo(1));
            var seenCopy = false;
            for (final IndexRoutingTable indexRoutingTable : cs.routingTable()) {
                if (indexRoutingTable.getIndex().getName().equals(copyIndexName)) {
                    seenCopy = true;
                }
                if (indexRoutingTable.allShardsActive() == false) {
                    return false;
                }
            }
            return seenCopy; // only remove this listener when we've started both restores and all the resulting shards are complete
        });

        // set up a listener which waits for the shards from the first restore to start initializing and then kick off another restore
        final var secondRestoreCompleteLatch = new CountDownLatch(1);
        final var secondRestoreStartedListener = ClusterServiceUtils.addTemporaryStateListener(cs -> {
            final var indexRoutingTable = cs.routingTable().index(indexName);
            if (indexRoutingTable != null && indexRoutingTable.shardsWithState(ShardRoutingState.INITIALIZING).isEmpty() == false) {
                clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
                    .setWaitForCompletion(true)
                    .setRenamePattern(indexName)
                    .setRenameReplacement(copyIndexName)
                    .execute(ActionTestUtils.assertNoFailureListener(restoreSnapshotResponse -> {
                        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
                        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
                        assertThat(restoreInfo.failedShards(), is(0));
                        secondRestoreCompleteLatch.countDown();
                    }));
                return true;
            }
            return false;
        });

        // now set the ball rolling by doing the first restore, waiting for it to complete
        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), is(0));

        // wait for the second restore to complete too
        safeAwait(secondRestoreStartedListener);
        safeAwait(secondRestoreCompleteLatch);

        // wait for all the shards to finish moving
        safeAwait(allShardsActiveListener);
        ensureGreen(indexName, copyIndexName);

        final var tinyNodeShardIds = getShardIds(dataNodeId, indexName);
        final var tinyNodeShardIdsCopy = getShardIds(dataNodeId, copyIndexName);
        assertThat(
            "expected just one shard from one index on the tiny node, instead got "
                + tinyNodeShardIds
                + " from the original index and "
                + tinyNodeShardIdsCopy
                + " from the copy",
            tinyNodeShardIds.size() + tinyNodeShardIdsCopy.size(),
            is(1)
        );
        final var useableSpaceShardSizes = shardSizes.getShardIdsWithSizeSmallerOrEqual(usableSpace);
        final var tinyNodeShardId = tinyNodeShardIds.isEmpty() == false
            ? tinyNodeShardIds.iterator().next()
            // shardSizes only contains the sizes from the original index, not the copy, so we map the copied shard back to the original idx
            : new ShardId(useableSpaceShardSizes.iterator().next().getIndex(), tinyNodeShardIdsCopy.iterator().next().id());
        assertThat(tinyNodeShardId, in(useableSpaceShardSizes));
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
     * Index documents until all the shards are at least WATERMARK_BYTES in size, and return the one with the smallest size
     */
    private ShardSizes createReasonableSizedShards(final String indexName) throws InterruptedException {
        while (true) {
            indexRandom(true, indexName, scaledRandomIntBetween(100, 10000));
            forceMerge();
            refresh();

            final ShardStats[] shardStates = indicesAdmin().prepareStats(indexName)
                .clear()
                .setStore(true)
                .setTranslog(true)
                .get()
                .getShards();

            var smallestShardSize = Arrays.stream(shardStates)
                .mapToLong(it -> it.getStats().getStore().sizeInBytes())
                .min()
                .orElseThrow(() -> new AssertionError("no shards"));

            if (smallestShardSize > WATERMARK_BYTES) {
                var shardSizes = Arrays.stream(shardStates)
                    .map(it -> new ShardSize(removeIndexUUID(it.getShardRouting().shardId()), it.getStats().getStore().sizeInBytes()))
                    .sorted(Comparator.comparing(ShardSize::size))
                    .toList();
                logger.info("Created shards with sizes {}", shardSizes);
                return new ShardSizes(shardSizes);
            }
        }
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
