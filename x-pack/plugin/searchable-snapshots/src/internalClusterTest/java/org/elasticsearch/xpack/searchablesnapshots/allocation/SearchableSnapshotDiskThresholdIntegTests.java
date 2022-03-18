/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.allocation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.node.NodeRoleSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.searchablesnapshots.LocalStateSearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.cache.shared.FrozenCacheService;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.store.Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING;
import static org.elasticsearch.license.LicenseService.SELF_GENERATED_LICENSE_TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage.FULL_COPY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SearchableSnapshotDiskThresholdIntegTests extends DiskUsageIntegTestCase {

    private static final long WATERMARK_BYTES = new ByteSizeValue(10, ByteSizeUnit.KB).getBytes();

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), WATERMARK_BYTES + "b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .put(SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            // we want to control the refresh of cluster info updates
            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), LocalStateSearchableSnapshots.class);
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    public void testHighWatermarkCanBeExceededOnColdOrFrozenNode() throws Exception {
        internalCluster().startMasterOnlyNode();
        final String dataHotNode = internalCluster().startNode(
            Settings.builder()
                .putList(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_HOT_NODE_ROLE.roleName())
                .build()
        );

        final var masterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.refresh(masterInfoService);

        final int nbIndices = randomIntBetween(1, 5);
        final CountDownLatch latch = new CountDownLatch(nbIndices);

        for (int i = 0; i < nbIndices; i++) {
            final String index = "index-" + i;
            var thread = new Thread(() -> {
                try {
                    createIndex(
                        index,
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                            .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
                            .put(INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), "0ms")
                            .put(DataTier.TIER_PREFERENCE_SETTING.getKey(), DataTier.DATA_HOT)
                            .build()
                    );
                    int nbDocs = 100;
                    try (BackgroundIndexer indexer = new BackgroundIndexer(index, client(), nbDocs)) {
                        while (true) {
                            waitForDocs(nbDocs, indexer);
                            indexer.assertNoFailures();
                            assertNoFailures(
                                client().admin().indices().prepareForceMerge().setFlush(true).setIndices(index).setMaxNumSegments(1).get()
                            );
                            Map<String, Long> storeSize = sizeOfShardsStores(index);
                            if (storeSize.get(index) > WATERMARK_BYTES) {
                                break;
                            }
                            int moreDocs = scaledRandomIntBetween(100, 1_000);
                            indexer.continueIndexing(moreDocs);
                            nbDocs += moreDocs;
                        }
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                } finally {
                    latch.countDown();
                }
            });
            thread.start();
        }
        latch.await();

        final String repository = "repository";
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(repository)
                .setType(FsRepository.TYPE)
                .setSettings(Settings.builder().put("location", randomRepoPath()).build())
        );

        final String snapshot = "snapshot";
        var snapshotInfo = client().admin()
            .cluster()
            .prepareCreateSnapshot(repository, snapshot)
            .setIndices("index-*")
            .setIncludeGlobalState(false)
            .setWaitForCompletion(true)
            .get()
            .getSnapshotInfo();
        assertThat(snapshotInfo.state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotInfo.successfulShards(), equalTo(nbIndices));
        assertThat(snapshotInfo.failedShards(), equalTo(0));

        final Map<String, Long> indicesStoresSizes = sizeOfShardsStores("index-*");
        assertAcked(client().admin().indices().prepareDelete("index-*"));

        final Storage storage = randomFrom(Storage.values());
        logger.info("--> using storage [{}]", storage);

        final Settings.Builder otherDataNodeSettings = Settings.builder();
        if (storage == FULL_COPY) {
            otherDataNodeSettings.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_COLD_NODE_ROLE.roleName());
        } else {
            otherDataNodeSettings.put(NodeRoleSettings.NODE_ROLES_SETTING.getKey(), DiscoveryNodeRole.DATA_FROZEN_NODE_ROLE.roleName())
                .put(
                    FrozenCacheService.SHARED_CACHE_SIZE_SETTING.getKey(),
                    ByteSizeValue.ofBytes(Math.min(indicesStoresSizes.values().stream().mapToLong(value -> value).sum(), 5 * 1024L * 1024L))
                );
        }
        final String otherDataNode = internalCluster().startNode(otherDataNodeSettings.build());
        ensureStableCluster(3);

        final String otherDataNodeId = internalCluster().getInstance(NodeEnvironment.class, otherDataNode).nodeId();
        logger.info("--> reducing disk size of node [{}/{}] so that all shards can fit on the node", otherDataNode, otherDataNodeId);
        final long totalSpace = indicesStoresSizes.values().stream().mapToLong(size -> size).sum() + WATERMARK_BYTES + 1024L;
        getTestFileStore(otherDataNode).setTotalSpace(totalSpace);

        logger.info("--> refreshing cluster info");
        ClusterInfoServiceUtils.refresh(masterInfoService);

        assertThat(
            masterInfoService.getClusterInfo().getNodeMostAvailableDiskUsages().get(otherDataNodeId).getTotalBytes(),
            equalTo(totalSpace)
        );

        final CountDownLatch mountLatch = new CountDownLatch(indicesStoresSizes.size());
        final String prefix = "mounted-";

        logger.info("--> mounting [{}] indices with [{}] prefix", indicesStoresSizes.size(), prefix);
        for (String index : indicesStoresSizes.keySet()) {
            client().execute(
                MountSearchableSnapshotAction.INSTANCE,
                new MountSearchableSnapshotRequest(
                    prefix + index,
                    repository,
                    snapshot,
                    index,
                    Settings.EMPTY,
                    Strings.EMPTY_ARRAY,
                    false,
                    storage
                ),
                ActionListener.wrap(response -> mountLatch.countDown(), e -> mountLatch.countDown())
            );
        }
        mountLatch.await();

        // The cold/frozen data node has enough disk space to hold all the shards
        assertBusy(() -> {
            var state = client().admin().cluster().prepareState().setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .stream()
                    .filter(shardRouting -> state.metadata().index(shardRouting.shardId().getIndex()).isSearchableSnapshot())
                    .allMatch(
                        shardRouting -> shardRouting.state() == ShardRoutingState.STARTED
                            && otherDataNodeId.equals(shardRouting.currentNodeId())
                    ),
                equalTo(true)
            );
        });

        final CountDownLatch extraLatch = new CountDownLatch(indicesStoresSizes.size());
        final String extraPrefix = "extra-";

        logger.info("--> mounting [{}] indices with [{}] prefix", indicesStoresSizes.size(), extraPrefix);
        for (String index : indicesStoresSizes.keySet()) {
            client().execute(
                MountSearchableSnapshotAction.INSTANCE,
                new MountSearchableSnapshotRequest(
                    extraPrefix + index,
                    repository,
                    snapshot,
                    randomFrom(indicesStoresSizes.keySet()),
                    Settings.EMPTY,
                    Strings.EMPTY_ARRAY,
                    false,
                    storage
                ),
                ActionListener.wrap(response -> extraLatch.countDown(), e -> extraLatch.countDown())
            );
        }
        extraLatch.await();

        // TODO Indices should not be allocated without checking the node disk usage first
        assertBusy(() -> {
            var state = client().admin().cluster().prepareState().setRoutingTable(true).get().getState();
            assertThat(
                state.routingTable()
                    .allShards()
                    .stream()
                    .filter(
                        shardRouting -> shardRouting.shardId().getIndexName().startsWith(extraPrefix)
                            && state.metadata().index(shardRouting.shardId().getIndex()).isSearchableSnapshot()
                    )
                    .allMatch(
                        shardRouting -> shardRouting.state() == ShardRoutingState.STARTED
                            && otherDataNodeId.equals(shardRouting.currentNodeId())
                    ),
                equalTo(true)
            );
        });
    }

    private static Map<String, Long> sizeOfShardsStores(String indexPattern) {
        return Arrays.stream(client().admin().indices().prepareStats(indexPattern).clear().setStore(true).get().getShards())
            .collect(
                Collectors.toUnmodifiableMap(s -> s.getShardRouting().getIndexName(), s -> s.getStats().getStore().sizeInBytes(), Long::sum)
            );
    }
}
