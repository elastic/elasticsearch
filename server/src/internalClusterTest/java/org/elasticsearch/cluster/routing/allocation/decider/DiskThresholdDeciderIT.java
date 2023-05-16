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
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
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

        final InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(
            ClusterInfoService.class
        );
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .addListener(event -> ClusterInfoServiceUtils.refresh(clusterInfoService));

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(6, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms").build());
        var smallestShard = createReasonableSizedShards(indexName);

        // reduce disk size of node 0 so that no shards fit below the high watermark, forcing all shards onto the other data node
        // (subtract the translog size since the disk threshold decider ignores this and may therefore move the shard back again)
        getTestFileStore(dataNodeName).setTotalSpace(smallestShard.size + WATERMARK_BYTES - 1L);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, empty());

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        getTestFileStore(dataNodeName).setTotalSpace(smallestShard.size + WATERMARK_BYTES);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, new ContainsExactlyOneOf<>(smallestShard.shardIds));
    }

    public void testRestoreSnapshotAllocationDoesNotExceedWatermark() throws Exception {
        internalCluster().startMasterOnlyNode();
        internalCluster().startDataOnlyNode();
        final String dataNodeName = internalCluster().startDataOnlyNode();
        ensureStableCluster(3);

        assertAcked(
            clusterAdmin().preparePutRepository("repo")
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()).put("compress", randomBoolean()))
        );

        final InternalClusterInfoService clusterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(
            ClusterInfoService.class
        );
        internalCluster().getCurrentMasterNodeInstance(ClusterService.class)
            .addListener(event -> ClusterInfoServiceUtils.refresh(clusterInfoService));

        final String dataNode0Id = internalCluster().getInstance(NodeEnvironment.class, dataNodeName).nodeId();

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, indexSettings(6, 0).put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms").build());
        var smallestShard = createReasonableSizedShards(indexName);

        final CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot("repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));

        assertAcked(indicesAdmin().prepareDelete(indexName).get());

        // reduce disk size of node 0 so that no shards fit below the low watermark, forcing shards to be assigned to the other data node
        getTestFileStore(dataNodeName).setTotalSpace(smallestShard.size + WATERMARK_BYTES - 1L);
        refreshDiskUsage();

        updateClusterSettings(
            Settings.builder().put(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), Rebalance.NONE.toString())
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("repo", "snap")
            .setWaitForCompletion(true)
            .get();
        final RestoreInfo restoreInfo = restoreSnapshotResponse.getRestoreInfo();
        assertThat(restoreInfo.successfulShards(), is(snapshotInfo.totalShards()));
        assertThat(restoreInfo.failedShards(), is(0));

        assertBusy(() -> assertThat(getShardIds(dataNode0Id, indexName), empty()));

        updateClusterSettings(Settings.builder().putNull(EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey()));

        // increase disk size of node 0 to allow just enough room for one shard, and check that it's rebalanced back
        getTestFileStore(dataNodeName).setTotalSpace(smallestShard.size + WATERMARK_BYTES);
        assertBusyWithDiskUsageRefresh(dataNode0Id, indexName, new ContainsExactlyOneOf<>(smallestShard.shardIds));
    }

    private Set<ShardId> getShardIds(final String nodeId, final String indexName) {
        final Set<ShardId> shardIds = new HashSet<>();
        final IndexRoutingTable indexRoutingTable = clusterAdmin().prepareState()
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
    private SmallestShards createReasonableSizedShards(final String indexName) throws InterruptedException {
        while (true) {
            final IndexRequestBuilder[] indexRequestBuilders = new IndexRequestBuilder[scaledRandomIntBetween(100, 10000)];
            for (int i = 0; i < indexRequestBuilders.length; i++) {
                indexRequestBuilders[i] = client().prepareIndex(indexName).setSource("field", randomAlphaOfLength(10));
            }
            indexRandom(true, indexRequestBuilders);
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
                var smallestShardIds = Arrays.stream(shardStates)
                    .filter(it -> it.getStats().getStore().sizeInBytes() == smallestShardSize)
                    .map(it -> removeIndexUUID(it.getShardRouting().shardId()))
                    .collect(toSet());

                logger.info(
                    "Created shards with sizes {}",
                    Arrays.stream(shardStates)
                        .collect(toMap(it -> it.getShardRouting().shardId(), it -> it.getStats().getStore().sizeInBytes()))
                );

                return new SmallestShards(smallestShardSize, smallestShardIds);
            }
        }
    }

    private record SmallestShards(long size, Set<ShardId> shardIds) {}

    private static ShardId removeIndexUUID(ShardId shardId) {
        return ShardId.fromString(shardId.toString());
    }

    private void refreshDiskUsage() {
        final ClusterInfoService clusterInfoService = internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(((InternalClusterInfoService) clusterInfoService));
        // if the nodes were all under the low watermark already (but unbalanced) then a change in the disk usage doesn't trigger a reroute
        // even though it's now possible to achieve better balance, so we have to do an explicit reroute. TODO fix this?
        if (clusterInfoService.getClusterInfo()
            .getNodeMostAvailableDiskUsages()
            .values()
            .stream()
            .allMatch(e -> e.getFreeBytes() > WATERMARK_BYTES)) {
            assertAcked(clusterAdmin().prepareReroute());
        }

        assertFalse(
            clusterAdmin().prepareHealth()
                .setWaitForEvents(Priority.LANGUID)
                .setWaitForNoRelocatingShards(true)
                .setWaitForNoInitializingShards(true)
                .get()
                .isTimedOut()
        );
    }

    private void assertBusyWithDiskUsageRefresh(String nodeName, String indexName, Matcher<? super Set<ShardId>> matcher) throws Exception {
        assertBusy(() -> {
            // refresh the master's ClusterInfoService before checking the assigned shards because DiskThresholdMonitor might still
            // be processing a previous ClusterInfo update and will skip the new one (see DiskThresholdMonitor#onNewInfo(ClusterInfo)
            // and its internal checkInProgress flag)
            refreshDiskUsage();

            final Set<ShardId> shardRoutings = getShardIds(nodeName, indexName);
            assertThat("Mismatching shard routings: " + shardRoutings, shardRoutings, matcher);
        }, 30L, TimeUnit.SECONDS);
    }

    private static final class ContainsExactlyOneOf<T> extends TypeSafeMatcher<Set<T>> {

        private final Set<T> expectedValues;

        ContainsExactlyOneOf(Set<T> expectedValues) {
            this.expectedValues = expectedValues;
        }

        @Override
        protected boolean matchesSafely(Set<T> item) {
            return item.size() == 1 && expectedValues.contains(item.iterator().next());
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("Expected to contain exactly one value from ").appendValueList("[", ",", "]", expectedValues);
        }
    }
}
