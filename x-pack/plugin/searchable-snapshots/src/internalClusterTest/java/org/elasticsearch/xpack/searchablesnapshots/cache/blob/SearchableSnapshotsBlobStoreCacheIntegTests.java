/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexingStats;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.full.CacheService;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.blobcache.shared.SharedBytes.pageAligned;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectory.unwrapDirectory;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SearchableSnapshotsBlobStoreCacheIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    private static Settings cacheSettings = null;
    private static ByteSizeValue blobCacheMaxLength = null;

    @BeforeClass
    public static void setUpCacheSettings() {
        blobCacheMaxLength = pageAligned(new ByteSizeValue(randomLongBetween(64L, 128L), ByteSizeUnit.KB));

        final Settings.Builder builder = Settings.builder();
        // Align ranges to match the blob cache max length
        builder.put(CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(), blobCacheMaxLength);
        builder.put(CacheService.SNAPSHOT_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), blobCacheMaxLength);

        // Frozen (shared cache) cache should be large enough to not cause direct reads
        builder.put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(128));
        // Align ranges to match the blob cache max length
        builder.put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), blobCacheMaxLength);
        builder.put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), blobCacheMaxLength);
        builder.put(SharedBlobCacheService.SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), blobCacheMaxLength);
        cacheSettings = builder.build();
    }

    @AfterClass
    public static void tearDownCacheSettings() {
        blobCacheMaxLength = null;
        cacheSettings = null;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(WaitForSnapshotBlobCacheShardsActivePlugin.class);
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    protected int numberOfShards() {
        return 1;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings)).put(cacheSettings).build();
    }

    public void testBlobStoreCache() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final NumShards numberOfShards = getNumShards(indexName);

        final int numberOfDocs = scaledRandomIntBetween(10, 20_000);
        logger.info("--> indexing [{}] documents in [{}]", numberOfDocs, indexName);

        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = numberOfDocs; i > 0; i--) {
            XContentBuilder builder = XContentFactory.smileBuilder();
            builder.startObject().field("text", randomRealisticUnicodeOfCodepointLengthBetween(5, 50)).field("num", i).endObject();
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource(builder));
        }
        indexRandom(true, true, true, indexRequestBuilders);

        if (randomBoolean()) {
            logger.info("--> force-merging index before snapshotting");
            final ForceMergeResponse forceMergeResponse = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
            assertThat(forceMergeResponse.getSuccessfulShards(), equalTo(numberOfShards.totalNumShards));
            assertThat(forceMergeResponse.getFailedShards(), equalTo(0));
        }

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Path repositoryLocation = randomRepoPath();
        createRepository(repositoryName, "fs", repositoryLocation);

        final SnapshotId snapshot = createSnapshot(repositoryName, "test-snapshot", List.of(indexName)).snapshotId();
        assertAcked(indicesAdmin().prepareDelete(indexName));

        expectThrows(
            IndexNotFoundException.class,
            ".snapshot-blob-cache system index should not be created yet",
            () -> systemClient().admin().indices().prepareGetIndex().addIndices(SNAPSHOT_BLOB_CACHE_INDEX).get()
        );

        final Storage storage1 = randomFrom(Storage.values());
        logger.info(
            "--> mount snapshot [{}] as an index for the first time [storage={}, max length={}]",
            snapshot,
            storage1,
            blobCacheMaxLength.getStringRep()
        );
        final String restoredIndex = "restored-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            restoredIndex,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                .put(SearchableSnapshots.SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH, blobCacheMaxLength)
                .build(),
            storage1
        );
        ensureGreen(restoredIndex);

        assertRecoveryStats(restoredIndex, false);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        waitForBlobCacheFillsToComplete();

        for (final SearchableSnapshotShardStats shardStats : client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest()
        ).actionGet().getStats()) {
            for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                assertThat(Strings.toString(indexInputStats), indexInputStats.getBlobStoreBytesRequested().getCount(), greaterThan(0L));
            }
        }

        logger.info("--> verifying cached documents in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        ensureYellow(SNAPSHOT_BLOB_CACHE_INDEX);
        refreshSystemIndex();

        logger.info("--> verifying system index [{}] data tiers preference", SNAPSHOT_BLOB_CACHE_INDEX);
        assertThat(
            systemClient().admin()
                .indices()
                .prepareGetSettings(SNAPSHOT_BLOB_CACHE_INDEX)
                .get()
                .getSetting(SNAPSHOT_BLOB_CACHE_INDEX, DataTier.TIER_PREFERENCE),
            equalTo("data_content,data_hot")
        );

        refreshSystemIndex();

        final long numberOfCachedBlobs = systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .get()
            .getHits()
            .getTotalHits().value;

        IndexingStats indexingStats = systemClient().admin()
            .indices()
            .prepareStats(SNAPSHOT_BLOB_CACHE_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .clear()
            .setIndexing(true)
            .get()
            .getTotal()
            .getIndexing();
        final long numberOfCacheWrites = indexingStats != null ? indexingStats.getTotal().getIndexCount() : 0L;

        logger.info("--> verifying number of documents in index [{}]", restoredIndex);
        assertHitCount(client().prepareSearch(restoredIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);

        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                if (indexService.index().getName().equals(restoredIndex)) {
                    for (IndexShard indexShard : indexService) {
                        try {
                            unwrapDirectory(indexShard.store().directory()).clearStats();
                        } catch (AlreadyClosedException ignore) {
                            // ok to ignore these
                        }
                    }
                }
            }
        }

        final Storage storage2 = randomFrom(Storage.values());
        logger.info("--> mount snapshot [{}] as an index for the second time [storage={}]", snapshot, storage2);
        final String restoredAgainIndex = "restored-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            restoredAgainIndex,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                .put(SearchableSnapshots.SNAPSHOT_BLOB_CACHE_METADATA_FILES_MAX_LENGTH, blobCacheMaxLength)
                .build(),
            storage2
        );
        ensureGreen(restoredAgainIndex);

        assertRecoveryStats(restoredAgainIndex, false);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        waitForBlobCacheFillsToComplete();

        logger.info("--> verifying shards of [{}] were started without using the blob store more than necessary", restoredAgainIndex);
        checkNoBlobStoreAccess();

        logger.info("--> verifying number of documents in index [{}]", restoredAgainIndex);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);

        logger.info("--> verifying that no extra cached blobs were indexed [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        refreshSystemIndex();
        assertHitCount(
            systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN).setSize(0).get(),
            numberOfCachedBlobs
        );
        indexingStats = systemClient().admin()
            .indices()
            .prepareStats(SNAPSHOT_BLOB_CACHE_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .clear()
            .setIndexing(true)
            .get()
            .getTotal()
            .getIndexing();
        assertThat(indexingStats != null ? indexingStats.getTotal().getIndexCount() : 0L, equalTo(numberOfCacheWrites));

        logger.info("--> restarting cluster");
        internalCluster().fullRestart(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) throws Exception {
                return Settings.builder()
                    .put(super.onNodeStopped(nodeName))
                    .put(WaitForSnapshotBlobCacheShardsActivePlugin.ENABLED.getKey(), true)
                    .build();
            }
        });

        ensureGreen("restored-*");

        assertRecoveryStats(restoredAgainIndex, false);
        assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
        waitForBlobCacheFillsToComplete();

        logger.info("--> shards of [{}] should start without downloading bytes from the blob store", restoredAgainIndex);
        checkNoBlobStoreAccess();

        logger.info("--> verifying that no cached blobs were indexed in system index [{}] after restart", SNAPSHOT_BLOB_CACHE_INDEX);
        assertHitCount(
            systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN).setSize(0).get(),
            numberOfCachedBlobs
        );
        indexingStats = systemClient().admin()
            .indices()
            .prepareStats(SNAPSHOT_BLOB_CACHE_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .clear()
            .setIndexing(true)
            .get()
            .getTotal()
            .getIndexing();
        assertThat(indexingStats != null ? indexingStats.getTotal().getIndexCount() : 0L, equalTo(0L));

        logger.info("--> verifying number of documents in index [{}]", restoredAgainIndex);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);

        logger.info("--> deleting indices, maintenance service should clean up [{}] docs in system index", numberOfCachedBlobs);
        assertAcked(indicesAdmin().prepareDelete("restored-*"));

        assertBusy(() -> {
            refreshSystemIndex();
            assertHitCount(
                systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX)
                    .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                    .setSize(0)
                    .get(),
                0L
            );
        }, 30L, TimeUnit.SECONDS);
    }

    private void checkNoBlobStoreAccess() {
        for (final SearchableSnapshotShardStats shardStats : client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest()
        ).actionGet().getStats()) {
            for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                assertThat(Strings.toString(indexInputStats), indexInputStats.getBlobStoreBytesRequested().getCount(), equalTo(0L));
            }
        }
    }

    /**
     * @return a {@link Client} that can be used to query the blob store cache system index
     */
    private Client systemClient() {
        return new OriginSettingClient(client(), ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN);
    }

    private void refreshSystemIndex() {
        try {
            final RefreshResponse refreshResponse = systemClient().admin().indices().prepareRefresh(SNAPSHOT_BLOB_CACHE_INDEX).get();
            assertThat(refreshResponse.getSuccessfulShards(), greaterThan(0));
            assertThat(refreshResponse.getFailedShards(), equalTo(0));
        } catch (IndexNotFoundException indexNotFoundException) {
            throw new AssertionError("unexpected", indexNotFoundException);
        }
    }

    /**
     * This plugin declares an {@link AllocationDecider} that forces searchable snapshot shards to be allocated after
     * the primary shards of the snapshot blob cache index are started. This way we can ensure that searchable snapshot
     * shards can use the snapshot blob cache index after the cluster restarted.
     */
    public static class WaitForSnapshotBlobCacheShardsActivePlugin extends Plugin implements ClusterPlugin {

        public static Setting<Boolean> ENABLED = Setting.boolSetting(
            "wait_for_snapshot_blob_cache_shards_active.enabled",
            false,
            Setting.Property.NodeScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(ENABLED);
        }

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            if (ENABLED.get(settings) == false) {
                return List.of();
            }
            final String name = "wait_for_snapshot_blob_cache_shards_active";
            return List.of(new AllocationDecider() {

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
                    return canAllocate(shardRouting, allocation);
                }

                @Override
                public Decision canAllocate(ShardRouting shardRouting, RoutingAllocation allocation) {
                    final IndexMetadata indexMetadata = allocation.metadata().index(shardRouting.index());
                    if (indexMetadata.isSearchableSnapshot() == false) {
                        return allocation.decision(Decision.YES, name, "index is not a searchable snapshot shard - can allocate");
                    }
                    if (allocation.metadata().hasIndex(SNAPSHOT_BLOB_CACHE_INDEX) == false) {
                        return allocation.decision(Decision.YES, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not created yet");
                    }
                    if (allocation.routingTable().hasIndex(SNAPSHOT_BLOB_CACHE_INDEX) == false) {
                        return allocation.decision(Decision.THROTTLE, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not active yet");
                    }
                    final IndexRoutingTable indexRoutingTable = allocation.routingTable().index(SNAPSHOT_BLOB_CACHE_INDEX);
                    if (indexRoutingTable.allPrimaryShardsActive() == false) {
                        return allocation.decision(Decision.THROTTLE, name, SNAPSHOT_BLOB_CACHE_INDEX + " is not active yet");
                    }
                    return allocation.decision(Decision.YES, name, "primary shard for this replica is already active");
                }
            });
        }
    }
}
