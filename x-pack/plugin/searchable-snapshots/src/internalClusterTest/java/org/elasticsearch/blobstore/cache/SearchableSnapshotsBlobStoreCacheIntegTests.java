/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobstore.cache;

import org.elasticsearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.INDEX_SHARD_SNAPSHOT_FORMAT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.DATA_TIERS_CACHE_INDEX_PREFERENCE;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class SearchableSnapshotsBlobStoreCacheIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), WaitForSnapshotBlobCacheShardsActivePlugin.class);
    }

    @Override
    protected int numberOfReplicas() {
        return 0;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(
                CacheService.SNAPSHOT_CACHE_RANGE_SIZE_SETTING.getKey(),
                randomLongBetween(new ByteSizeValue(4, ByteSizeUnit.KB).getBytes(), new ByteSizeValue(20, ByteSizeUnit.KB).getBytes()) + "b"
            )
            .put(CacheService.SNAPSHOT_CACHE_SIZE_SETTING.getKey(), new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES))
            .build();
    }

    public void testBlobStoreCache() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = scaledRandomIntBetween(0, 10_000); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("text", randomUnicodeOfLength(10), "num", i));
        }
        indexRandom(true, false, true, indexRequestBuilders);
        final long numberOfDocs = indexRequestBuilders.size();
        final NumShards numberOfShards = getNumShards(indexName);

        if (randomBoolean()) {
            logger.info("--> force-merging index before snapshotting");
            final ForceMergeResponse forceMergeResponse = client().admin()
                .indices()
                .prepareForceMerge(indexName)
                .setMaxNumSegments(1)
                .get();
            assertThat(forceMergeResponse.getSuccessfulShards(), equalTo(numberOfShards.totalNumShards));
            assertThat(forceMergeResponse.getFailedShards(), equalTo(0));
        }

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Path repositoryLocation = randomRepoPath();
        createRepository(repositoryName, "fs", repositoryLocation);

        final SnapshotId snapshot = createSnapshot(repositoryName, "test-snapshot", List.of(indexName)).snapshotId();
        assertAcked(client().admin().indices().prepareDelete(indexName));

        // extract the list of blobs per shard from the snapshot directory on disk
        final Map<String, BlobStoreIndexShardSnapshot> blobsInSnapshot = blobsInSnapshot(repositoryLocation, snapshot.getUUID());
        assertThat("Failed to load all shard snapshot metadata files", blobsInSnapshot.size(), equalTo(numberOfShards.numPrimaries));

        expectThrows(
            IndexNotFoundException.class,
            ".snapshot-blob-cache system index should not be created yet",
            () -> systemClient().admin().indices().prepareGetIndex().addIndices(SNAPSHOT_BLOB_CACHE_INDEX).get()
        );

        logger.info("--> mount snapshot [{}] as an index for the first time", snapshot);
        final String restoredIndex = mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                .build()
        );
        ensureGreen(restoredIndex);

        // wait for all async cache fills to complete
        assertBusy(() -> {
            for (final SearchableSnapshotShardStats shardStats : client().execute(
                SearchableSnapshotsStatsAction.INSTANCE,
                new SearchableSnapshotsStatsRequest()
            ).actionGet().getStats()) {
                for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                    assertThat(Strings.toString(indexInputStats), indexInputStats.getCurrentIndexCacheFills(), equalTo(0L));
                }
            }
        });

        for (final SearchableSnapshotShardStats shardStats : client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest()
        ).actionGet().getStats()) {
            for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                assertThat(Strings.toString(indexInputStats), indexInputStats.getBlobStoreBytesRequested().getCount(), greaterThan(0L));
            }
        }

        logger.info("--> verifying cached documents in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, blobsInSnapshot);

        logger.info("--> verifying system index [{}] data tiers preference", SNAPSHOT_BLOB_CACHE_INDEX);
        assertThat(
            systemClient().admin()
                .indices()
                .prepareGetSettings(SNAPSHOT_BLOB_CACHE_INDEX)
                .get()
                .getSetting(SNAPSHOT_BLOB_CACHE_INDEX, DataTierAllocationDecider.INDEX_ROUTING_PREFER),
            equalTo(DATA_TIERS_CACHE_INDEX_PREFERENCE)
        );

        final long numberOfCachedBlobs = systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).get().getHits().getTotalHits().value;
        final long numberOfCacheWrites = systemClient().admin()
            .indices()
            .prepareStats(SNAPSHOT_BLOB_CACHE_INDEX)
            .clear()
            .setIndexing(true)
            .get()
            .getTotal().indexing.getTotal().getIndexCount();

        logger.info("--> verifying documents in index [{}]", restoredIndex);
        assertHitCount(client().prepareSearch(restoredIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);
        assertHitCount(
            client().prepareSearch(restoredIndex)
                .setQuery(QueryBuilders.rangeQuery("num").lte(numberOfDocs))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            numberOfDocs
        );
        assertHitCount(
            client().prepareSearch(restoredIndex)
                .setQuery(QueryBuilders.rangeQuery("num").gt(numberOfDocs + 1))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            0L
        );

        assertAcked(client().admin().indices().prepareDelete(restoredIndex));

        logger.info("--> mount snapshot [{}] as an index for the second time", snapshot);
        final String restoredAgainIndex = mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                .build()
        );
        ensureGreen(restoredAgainIndex);

        logger.info("--> verifying shards of [{}] were started without using the blob store more than necessary", restoredAgainIndex);
        for (final SearchableSnapshotShardStats shardStats : client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest()
        ).actionGet().getStats()) {
            for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                // we read the header of each file contained within the .cfs file, which could be anywhere
                final boolean mayReadMoreThanHeader = indexInputStats.getFileName().endsWith(".cfs");
                if (indexInputStats.getFileLength() <= BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE * 2
                    || mayReadMoreThanHeader == false) {
                    assertThat(Strings.toString(indexInputStats), indexInputStats.getBlobStoreBytesRequested().getCount(), equalTo(0L));
                }
            }
        }

        logger.info("--> verifying documents in index [{}]", restoredAgainIndex);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").lte(numberOfDocs))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            numberOfDocs
        );
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").gt(numberOfDocs + 1))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            0L
        );

        logger.info("--> verifying cached documents (again) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, blobsInSnapshot);

        logger.info("--> verifying that no extra cached blobs were indexed [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        refreshSystemIndex();
        assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), numberOfCachedBlobs);
        assertThat(
            systemClient().admin().indices().prepareStats(SNAPSHOT_BLOB_CACHE_INDEX).clear().setIndexing(true).get().getTotal().indexing
                .getTotal()
                .getIndexCount(),
            equalTo(numberOfCacheWrites)
        );

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
        ensureGreen(restoredAgainIndex);

        logger.info("--> verifying documents in index [{}]", restoredAgainIndex);
        assertHitCount(client().prepareSearch(restoredAgainIndex).setSize(0).setTrackTotalHits(true).get(), numberOfDocs);
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").lte(numberOfDocs))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            numberOfDocs
        );
        assertHitCount(
            client().prepareSearch(restoredAgainIndex)
                .setQuery(QueryBuilders.rangeQuery("num").gt(numberOfDocs + 1))
                .setSize(0)
                .setTrackTotalHits(true)
                .get(),
            0L
        );

        logger.info("--> verifying cached documents (after restart) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, blobsInSnapshot);

        logger.info("--> verifying that no cached blobs were indexed in system index [{}] after restart", SNAPSHOT_BLOB_CACHE_INDEX);
        assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), numberOfCachedBlobs);
        assertThat(
            systemClient().admin().indices().prepareStats(SNAPSHOT_BLOB_CACHE_INDEX).clear().setIndexing(true).get().getTotal().indexing
                .getTotal()
                .getIndexCount(),
            equalTo(0L)
        );

        // TODO also test when the index is frozen
        // TODO also test when prewarming is enabled
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
     * Reads a repository location on disk and extracts the list of blobs for each shards
     */
    private Map<String, BlobStoreIndexShardSnapshot> blobsInSnapshot(Path repositoryLocation, String snapshotId) throws IOException {
        final Map<String, BlobStoreIndexShardSnapshot> blobsPerShard = new HashMap<>();
        forEachFileRecursively(repositoryLocation.resolve("indices"), ((file, basicFileAttributes) -> {
            final String fileName = file.getFileName().toString();
            if (fileName.equals(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(snapshotId))) {
                blobsPerShard.put(
                    String.join(
                        "/",
                        snapshotId,
                        file.getParent().getParent().getFileName().toString(),
                        file.getParent().getFileName().toString()
                    ),
                    INDEX_SHARD_SNAPSHOT_FORMAT.deserialize(fileName, xContentRegistry(), Streams.readFully(Files.newInputStream(file)))
                );
            }
        }));
        return Map.copyOf(blobsPerShard);
    }

    private void assertCachedBlobsInSystemIndex(final String repositoryName, final Map<String, BlobStoreIndexShardSnapshot> blobsInSnapshot)
        throws Exception {
        assertBusy(() -> {
            refreshSystemIndex();

            long numberOfCachedBlobs = 0L;
            for (Map.Entry<String, BlobStoreIndexShardSnapshot> blob : blobsInSnapshot.entrySet()) {
                for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : blob.getValue().indexFiles()) {
                    if (fileInfo.name().startsWith("__") == false) {
                        continue;
                    }

                    final String path = String.join("/", repositoryName, blob.getKey(), fileInfo.physicalName());
                    if (fileInfo.length() <= BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE * 2) {
                        // file has been fully cached
                        final GetResponse getResponse = systemClient().prepareGet(SNAPSHOT_BLOB_CACHE_INDEX, path + "/@0").get();
                        assertThat("not cached: [" + path + "/@0] for blob [" + fileInfo + "]", getResponse.isExists(), is(true));
                        final CachedBlob cachedBlob = CachedBlob.fromSource(getResponse.getSourceAsMap());
                        assertThat(cachedBlob.from(), equalTo(0L));
                        assertThat(cachedBlob.to(), equalTo(fileInfo.length()));
                        assertThat((long) cachedBlob.length(), equalTo(fileInfo.length()));
                        numberOfCachedBlobs += 1;

                    } else {
                        // first region of file has been cached
                        GetResponse getResponse = systemClient().prepareGet(SNAPSHOT_BLOB_CACHE_INDEX, path + "/@0").get();
                        assertThat(
                            "not cached: [" + path + "/@0] for first region of blob [" + fileInfo + "]",
                            getResponse.isExists(),
                            is(true)
                        );

                        CachedBlob cachedBlob = CachedBlob.fromSource(getResponse.getSourceAsMap());
                        assertThat(cachedBlob.from(), equalTo(0L));
                        assertThat(cachedBlob.to(), equalTo((long) BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE));
                        assertThat(cachedBlob.length(), equalTo(BlobStoreCacheService.DEFAULT_CACHED_BLOB_SIZE));
                        numberOfCachedBlobs += 1;
                    }
                }
            }

            refreshSystemIndex();
            assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), numberOfCachedBlobs);
        });
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
                    if (SearchableSnapshotsConstants.isSearchableSnapshotStore(indexMetadata.getSettings()) == false) {
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
