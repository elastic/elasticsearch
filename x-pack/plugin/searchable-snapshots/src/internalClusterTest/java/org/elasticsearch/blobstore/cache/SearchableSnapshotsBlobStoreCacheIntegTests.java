/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.blobstore.cache;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene50.CompoundReaderUtils;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
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
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.BaseSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.cache.ByteRange;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.blobstore.cache.BlobStoreCacheService.computeHeaderByteRange;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.DATA_TIERS_CACHE_INDEX_PREFERENCE;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsUtils.toIntBytes;
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
        flushAndRefresh(indexName);

        // Per-shard list of Lucene files with their respective lengths
        final Map<ShardId, Map<String, Long>> expectedLuceneFiles = new HashMap<>();
        // Per-shard list of Lucene compound files with their respective offset and lengths in the .cfs
        final Map<ShardId, Map<String, Map<String, Tuple<Long, Long>>>> expectedLuceneCompoundFiles = new HashMap<>();

        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                for (IndexShard indexShard : indexService) {
                    final ShardId shardId = indexShard.shardId();
                    if (indexName.equals(shardId.getIndexName())) {
                        final Directory directory = indexShard.store().directory();

                        // load regular Lucene files
                        for (String file : directory.listAll()) {
                            String extension = IndexFileNames.getExtension(file);
                            if (extension != null && extension.equals("si") == false && extension.equals("lock") == false) {
                                expectedLuceneFiles.computeIfAbsent(shardId, s -> new HashMap<>()).put(file, directory.fileLength(file));
                            }
                        }

                        // load Lucene compound files
                        expectedLuceneCompoundFiles.put(shardId, CompoundReaderUtils.extractCompoundFiles(directory));
                    }
                }
            }
        }
        assertThat("Failed to load Lucene files", expectedLuceneFiles.size(), equalTo(numberOfShards.numPrimaries));

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Path repositoryLocation = randomRepoPath();
        createRepository(repositoryName, "fs", repositoryLocation);

        final SnapshotId snapshot = createSnapshot(repositoryName, "test-snapshot", List.of(indexName)).snapshotId();
        assertAcked(client().admin().indices().prepareDelete(indexName));

        // extract the paths of each shards from the snapshot directory on disk
        final Map<Integer, String> shardsPathsInSnapshot = shardsPathsInSnapshot(repositoryLocation, snapshot.getUUID());
        assertThat("Failed to load all shard snapshot paths", shardsPathsInSnapshot.size(), equalTo(numberOfShards.numPrimaries));

        expectThrows(
            IndexNotFoundException.class,
            ".snapshot-blob-cache system index should not be created yet",
            () -> systemClient().admin().indices().prepareGetIndex().addIndices(SNAPSHOT_BLOB_CACHE_INDEX).get()
        );

        boolean cacheEnabled = true; // always enable cache the first time to populate the SNAPSHOT_BLOB_CACHE_INDEX
        Storage storage = Storage.FULL_COPY;
        logger.info("--> mount snapshot [{}] as an index for the first time [cache={}, storage={}]", snapshot, cacheEnabled, storage);
        final String restoredIndex = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            restoredIndex,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), cacheEnabled)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                .build(),
            storage
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
        }, 30L, TimeUnit.SECONDS);

        for (final SearchableSnapshotShardStats shardStats : client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest()
        ).actionGet().getStats()) {
            for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                assertThat(Strings.toString(indexInputStats), indexInputStats.getBlobStoreBytesRequested().getCount(), greaterThan(0L));
            }
        }

        logger.info("--> verifying cached documents in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, shardsPathsInSnapshot, expectedLuceneFiles, expectedLuceneCompoundFiles);

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

        cacheEnabled = randomBoolean();
        storage = cacheEnabled ? randomFrom(Storage.values()) : Storage.FULL_COPY;
        logger.info("--> mount snapshot [{}] as an index for the second time [cache={}, storage={}]", snapshot, cacheEnabled, storage);
        final String restoredAgainIndex = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        mountSnapshot(
            repositoryName,
            snapshot.getName(),
            indexName,
            restoredAgainIndex,
            Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), cacheEnabled)
                .put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), false)
                .build(),
            storage
        );
        ensureGreen(restoredAgainIndex);

        logger.info("--> shards of [{}] should start without downloading bytes from the blob store", restoredAgainIndex);
        for (final SearchableSnapshotShardStats shardStats : client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest()
        ).actionGet().getStats()) {
            for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                assertThat(Strings.toString(indexInputStats), indexInputStats.getBlobStoreBytesRequested().getCount(), equalTo(0L));
            }
        }

        logger.info("--> verifying cached documents (before search) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, shardsPathsInSnapshot, expectedLuceneFiles, expectedLuceneCompoundFiles);

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

        logger.info("--> verifying cached documents (after restart) in system index [{}]", SNAPSHOT_BLOB_CACHE_INDEX);
        assertCachedBlobsInSystemIndex(repositoryName, shardsPathsInSnapshot, expectedLuceneFiles, expectedLuceneCompoundFiles);

        logger.info("--> shards of [{}] should start without downloading bytes from the blob store", restoredAgainIndex);
        for (final SearchableSnapshotShardStats shardStats : client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest()
        ).actionGet().getStats()) {
            for (final SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : shardStats.getStats()) {
                assertThat(Strings.toString(indexInputStats), indexInputStats.getBlobStoreBytesRequested().getCount(), equalTo(0L));
            }
        }

        logger.info("--> verifying that no cached blobs were indexed in system index [{}] after restart", SNAPSHOT_BLOB_CACHE_INDEX);
        assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), numberOfCachedBlobs);
        assertThat(
            systemClient().admin().indices().prepareStats(SNAPSHOT_BLOB_CACHE_INDEX).clear().setIndexing(true).get().getTotal().indexing
                .getTotal()
                .getIndexCount(),
            equalTo(0L)
        );

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
     * Reads a repository location on disk and extracts the paths of each shards
     */
    private Map<Integer, String> shardsPathsInSnapshot(Path repositoryLocation, String snapshotId) throws IOException {
        final Map<Integer, String> shards = new HashMap<>();
        forEachFileRecursively(repositoryLocation.resolve("indices"), ((file, basicFileAttributes) -> {
            final String fileName = file.getFileName().toString();
            if (fileName.equals(BlobStoreRepository.SNAPSHOT_FORMAT.blobName(snapshotId))) {
                final Integer shardId = Integer.parseInt(file.getParent().getFileName().toString());
                shards.put(
                    shardId,
                    String.join(
                        "/",
                        snapshotId,
                        file.getParent().getParent().getFileName().toString(),
                        file.getParent().getFileName().toString()
                    )
                );
            }
        }));
        return Collections.unmodifiableMap(shards);
    }

    private void assertCachedBlobsInSystemIndex(
        final String repositoryName,
        final Map<Integer, String> shardsPathsInSnapshot,
        final Map<ShardId, Map<String, Long>> expectedLuceneFiles,
        final Map<ShardId, Map<String, Map<String, Tuple<Long, Long>>>> expectedLuceneCompoundFiles
    ) throws Exception {

        final Map<String, ByteRange> expectedCachedBlobs = new HashMap<>();

        for (ShardId shardId : expectedLuceneFiles.keySet()) {
            final String shardPath = shardsPathsInSnapshot.get(shardId.getId());
            for (Map.Entry<String, Long> luceneFile : expectedLuceneFiles.get(shardId).entrySet()) {
                final ByteRange header = computeHeaderByteRange(luceneFile.getKey(), luceneFile.getValue());
                expectedCachedBlobs.put(String.join("/", repositoryName, shardPath, luceneFile.getKey(), "@" + header.start()), header);
                // footer of regular Lucene files is never cached in blob store cache,
                // it is extracted from the shard snapshot metadata instead.
            }
        }

        for (ShardId shardId : expectedLuceneCompoundFiles.keySet()) {
            final String shardPath = shardsPathsInSnapshot.get(shardId.getId());
            final Map<String, Map<String, Tuple<Long, Long>>> luceneCompoundFiles = expectedLuceneCompoundFiles.get(shardId);

            for (Map.Entry<String, Map<String, Tuple<Long, Long>>> luceneCompoundFile : luceneCompoundFiles.entrySet()) {
                final String segmentName = luceneCompoundFile.getKey();
                final String cfsFileName = segmentName + ".cfs";
                for (Map.Entry<String, Tuple<Long, Long>> innerFile : luceneCompoundFile.getValue().entrySet()) {
                    final long offset = innerFile.getValue().v1();
                    final long length = innerFile.getValue().v2();

                    final ByteRange header = computeHeaderByteRange(innerFile.getKey(), length).withOffset(offset);
                    expectedCachedBlobs.put(String.join("/", repositoryName, shardPath, cfsFileName, "@" + header.start()), header);

                    if (header.length() < length) {
                        final ByteRange footer = ByteRange.of(length - CodecUtil.footerLength(), length).withOffset(offset);
                        expectedCachedBlobs.put(String.join("/", repositoryName, shardPath, cfsFileName, "@" + footer.start()), footer);
                    }
                }
            }
        }

        assertBusy(() -> {
            refreshSystemIndex();
            assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), expectedCachedBlobs.size());
        });

        for (Map.Entry<String, ByteRange> expectedCachedBlob : expectedCachedBlobs.entrySet()) {
            final String cachedBlobId = expectedCachedBlob.getKey();

            final GetResponse getResponse = systemClient().prepareGet(SNAPSHOT_BLOB_CACHE_INDEX, cachedBlobId).get();
            assertThat("Cached blob not found: " + cachedBlobId, getResponse.isExists(), is(true));

            final CachedBlob cachedBlob = CachedBlob.fromSource(getResponse.getSourceAsMap());
            assertThat(cachedBlob.from(), equalTo(expectedCachedBlob.getValue().start()));
            assertThat(cachedBlob.to(), equalTo(expectedCachedBlob.getValue().end()));
            assertThat(cachedBlob.length(), equalTo(toIntBytes(expectedCachedBlob.getValue().length())));
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
