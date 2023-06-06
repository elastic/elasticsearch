/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.LuceneFilesExtensions;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_BLOB_CACHE_INDEX;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_INDEX_NAME_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class SearchableSnapshotsBlobStoreCacheMaintenanceIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    /**
     * Test that snapshot blob cache entries are deleted from the system index after the corresponding searchable snapshot index is deleted
     */
    public void testCleanUpAfterIndicesAreDeleted() throws Exception {
        final String repositoryName = "repository";
        createRepository(repositoryName, FsRepository.TYPE);

        final Map<String, Tuple<Settings, Long>> mountedIndices = mountRandomIndicesWithCache(repositoryName, 3, 10);
        ensureYellow(SNAPSHOT_BLOB_CACHE_INDEX);
        refreshSystemIndex(true);

        final long numberOfEntriesInCache = numberOfEntriesInCache();
        logger.info("--> found [{}] entries in snapshot blob cache", numberOfEntriesInCache);
        assertThat(numberOfEntriesInCache, equalTo(mountedIndices.values().stream().mapToLong(Tuple::v2).sum()));

        final List<String> indicesToDelete = randomSubsetOf(randomIntBetween(1, mountedIndices.size()), mountedIndices.keySet());
        assertAcked(client().admin().indices().prepareDelete(indicesToDelete.toArray(String[]::new)));

        final long expectedDeletedEntriesInCache = mountedIndices.entrySet()
            .stream()
            .filter(e -> indicesToDelete.contains(e.getKey()))
            .mapToLong(entry -> entry.getValue().v2())
            .sum();
        logger.info("--> deleting indices [{}] with [{}] entries in snapshot blob cache", indicesToDelete, expectedDeletedEntriesInCache);

        assertBusy(() -> {
            refreshSystemIndex(true);
            assertThat(numberOfEntriesInCache(), equalTo(numberOfEntriesInCache - expectedDeletedEntriesInCache));

            for (String mountedIndex : mountedIndices.keySet()) {
                final Settings indexSettings = mountedIndices.get(mountedIndex).v1();
                assertHitCount(
                    systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX)
                        .setQuery(
                            BlobStoreCacheMaintenanceService.buildDeleteByQuery(
                                INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings),
                                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings),
                                SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
                            )
                        )
                        .setSize(0)
                        .get(),
                    indicesToDelete.contains(mountedIndex) ? 0L : mountedIndices.get(mountedIndex).v2()
                );
            }
        });

        final Set<String> remainingIndices = mountedIndices.keySet()
            .stream()
            .filter(Predicate.not(indicesToDelete::contains))
            .collect(Collectors.toSet());

        if (remainingIndices.isEmpty() == false) {
            final List<String> moreIndicesToDelete = randomSubsetOf(randomIntBetween(1, remainingIndices.size()), remainingIndices);

            final String randomMountedIndex = randomFrom(moreIndicesToDelete);
            final Settings randomIndexSettings = getIndexSettings(randomMountedIndex);
            final String snapshotId = SNAPSHOT_SNAPSHOT_ID_SETTING.get(randomIndexSettings);
            final String snapshotName = SNAPSHOT_SNAPSHOT_NAME_SETTING.get(randomIndexSettings);
            final String snapshotIndexName = SNAPSHOT_INDEX_NAME_SETTING.get(randomIndexSettings);

            final String remainingMountedIndex = "mounted-remaining-index";
            mountSnapshot(
                repositoryName,
                snapshotName,
                snapshotIndexName,
                remainingMountedIndex,
                Settings.EMPTY,
                randomFrom(Storage.values())
            );
            ensureGreen(remainingMountedIndex);

            assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
            assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
            waitForBlobCacheFillsToComplete();
            ensureClusterStateConsistency();

            logger.info(
                "--> deleting more mounted indices [{}] with snapshot [{}/{}] of index [{}] is still mounted as index [{}]",
                moreIndicesToDelete,
                snapshotId,
                snapshotIndexName,
                snapshotIndexName,
                remainingMountedIndex
            );
            assertAcked(client().admin().indices().prepareDelete(moreIndicesToDelete.toArray(String[]::new)));

            assertBusy(() -> {
                refreshSystemIndex(true);

                for (String mountedIndex : mountedIndices.keySet()) {
                    final Settings indexSettings = mountedIndices.get(mountedIndex).v1();

                    final long remainingEntriesInCache = systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX)
                        .setQuery(
                            BlobStoreCacheMaintenanceService.buildDeleteByQuery(
                                INDEX_NUMBER_OF_SHARDS_SETTING.get(indexSettings),
                                SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings),
                                SNAPSHOT_INDEX_ID_SETTING.get(indexSettings)
                            )
                        )
                        .setSize(0)
                        .get()
                        .getHits()
                        .getTotalHits().value;

                    if (indicesToDelete.contains(mountedIndex)) {
                        assertThat(remainingEntriesInCache, equalTo(0L));
                    } else if (snapshotId.equals(SNAPSHOT_SNAPSHOT_ID_SETTING.get(indexSettings))) {
                        assertThat(remainingEntriesInCache, greaterThanOrEqualTo(mountedIndices.get(randomMountedIndex).v2()));
                    } else if (moreIndicesToDelete.contains(mountedIndex)) {
                        assertThat(remainingEntriesInCache, equalTo(0L));
                    } else {
                        assertThat(remainingEntriesInCache, equalTo(mountedIndices.get(mountedIndex).v2()));
                    }
                }
            });
        }

        logger.info("--> deleting indices, maintenance service should clean up snapshot blob cache index");
        assertAcked(client().admin().indices().prepareDelete("mounted-*"));
        assertBusy(() -> {
            refreshSystemIndex(true);
            assertHitCount(systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX).setSize(0).get(), 0L);
        });
    }

    /**
     * Test that obsolete blob cache entries are deleted from the system index by the periodic maintenance task.
     */
    public void testPeriodicMaintenance() throws Exception {
        ensureStableCluster(internalCluster().getNodeNames().length, TimeValue.timeValueSeconds(60L));

        createRepository("repo", FsRepository.TYPE);
        Map<String, Tuple<Settings, Long>> mountedIndices = mountRandomIndicesWithCache("repo", 1, 3);
        ensureYellow(SNAPSHOT_BLOB_CACHE_INDEX);

        final long nbEntriesInCacheForMountedIndices = mountedIndices.values().stream().mapToLong(Tuple::v2).sum();
        refreshSystemIndex(true);
        assertThat(numberOfEntriesInCache(), equalTo(nbEntriesInCacheForMountedIndices));

        createRepository("other", FsRepository.TYPE);
        Map<String, Tuple<Settings, Long>> otherMountedIndices = mountRandomIndicesWithCache("other", 1, 3);

        final long nbEntriesInCacheForOtherIndices = otherMountedIndices.values().stream().mapToLong(Tuple::v2).sum();
        refreshSystemIndex(true);
        assertThat(numberOfEntriesInCache(), equalTo(nbEntriesInCacheForMountedIndices + nbEntriesInCacheForOtherIndices));

        if (randomBoolean()) {
            final int oldDocsInCache = indexRandomDocsInCache(1, 50, Instant.now().minus(Duration.ofDays(7L)).toEpochMilli());
            refreshSystemIndex(true);
            assertThat(
                numberOfEntriesInCache(),
                equalTo(nbEntriesInCacheForMountedIndices + nbEntriesInCacheForOtherIndices + oldDocsInCache)
            );
        }

        // creates a backup of the system index cache to be restored later
        createRepository("backup", FsRepository.TYPE);
        createSnapshot("backup", "backup", Collections.emptyList(), Collections.singletonList("searchable_snapshots"));

        final Set<String> indicesToDelete = new HashSet<>(mountedIndices.keySet());
        indicesToDelete.add(randomFrom(otherMountedIndices.keySet()));

        assertAcked(systemClient().admin().indices().prepareDelete(SNAPSHOT_BLOB_CACHE_INDEX));
        assertAcked(client().admin().indices().prepareDelete(indicesToDelete.toArray(String[]::new)));
        assertAcked(client().admin().cluster().prepareDeleteRepository("repo"));
        ensureClusterStateConsistency();

        assertThat(numberOfEntriesInCache(), equalTo(0L));

        updateClusterSettings(
            Settings.builder()
                .put(BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1L))
        );
        try {
            // restores the .snapshot-blob-cache index with - now obsolete - documents
            final RestoreSnapshotResponse restoreResponse = client().admin()
                .cluster()
                .prepareRestoreSnapshot("backup", "backup")
                // We only want to restore the blob cache index. Since we can't do that by name, specify an index that doesn't exist and
                // allow no indices - this way, only the indices resolved from the feature state will be resolved.
                .setIndices("this-index-doesnt-exist-i-know-because-#-is-illegal-in-index-names")
                .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                .setFeatureStates("searchable_snapshots")
                .setWaitForCompletion(true)
                .get();
            assertThat(restoreResponse.getRestoreInfo().successfulShards(), equalTo(1));
            assertThat(restoreResponse.getRestoreInfo().failedShards(), equalTo(0));

            final int recentDocsInCache;
            if (randomBoolean()) {
                // recent as in the future, actually
                recentDocsInCache = indexRandomDocsInCache(1, 50, Instant.now().plus(Duration.ofDays(10L)).toEpochMilli());
            } else {
                recentDocsInCache = 0;
            }

            // only very old docs should have been deleted
            assertBusy(() -> {
                refreshSystemIndex(true);
                assertThat(
                    numberOfEntriesInCache(),
                    equalTo(nbEntriesInCacheForMountedIndices + nbEntriesInCacheForOtherIndices + recentDocsInCache)
                );
            }, 30L, TimeUnit.SECONDS);

            // updating the retention period from 1H to immediate
            updateClusterSettings(
                Settings.builder()
                    .put(
                        BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_RETENTION_PERIOD.getKey(),
                        TimeValue.timeValueSeconds(0L)
                    )
            );

            // only used documents should remain
            final long expectNumberOfRemainingCacheEntries = otherMountedIndices.entrySet()
                .stream()
                .filter(e -> indicesToDelete.contains(e.getKey()) == false)
                .mapToLong(e -> e.getValue().v2())
                .sum();

            assertBusy(() -> {
                refreshSystemIndex(true);
                assertThat(numberOfEntriesInCache(), equalTo(expectNumberOfRemainingCacheEntries + recentDocsInCache));
            }, 30L, TimeUnit.SECONDS);

        } finally {
            updateClusterSettings(
                Settings.builder()
                    .putNull(BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_INTERVAL_SETTING.getKey())
                    .putNull(BlobStoreCacheMaintenanceService.SNAPSHOT_SNAPSHOT_CLEANUP_RETENTION_PERIOD.getKey())
            );
        }
    }

    /**
     * @return a {@link Client} that can be used to query the blob store cache system index
     */
    private Client systemClient() {
        return new OriginSettingClient(client(), ClientHelper.SEARCHABLE_SNAPSHOTS_ORIGIN);
    }

    private long numberOfEntriesInCache() {
        return systemClient().prepareSearch(SNAPSHOT_BLOB_CACHE_INDEX)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
            .setTrackTotalHits(true)
            .setSize(0)
            .get()
            .getHits()
            .getTotalHits().value;
    }

    private void refreshSystemIndex(boolean failIfNotExist) {
        try {
            final RefreshResponse refreshResponse = systemClient().admin()
                .indices()
                .prepareRefresh(SNAPSHOT_BLOB_CACHE_INDEX)
                .setIndicesOptions(failIfNotExist ? RefreshRequest.DEFAULT_INDICES_OPTIONS : IndicesOptions.LENIENT_EXPAND_OPEN)
                .get();
            assertThat(refreshResponse.getSuccessfulShards(), failIfNotExist ? greaterThan(0) : greaterThanOrEqualTo(0));
            assertThat(refreshResponse.getFailedShards(), equalTo(0));
        } catch (IndexNotFoundException indexNotFoundException) {
            throw new AssertionError("unexpected", indexNotFoundException);
        }
    }

    private Settings getIndexSettings(String indexName) {
        return client().admin().indices().prepareGetSettings(indexName).get().getIndexToSettings().get(indexName);
    }

    private Map<String, Tuple<Settings, Long>> mountRandomIndicesWithCache(String repositoryName, int min, int max) throws Exception {
        refreshSystemIndex(false);
        long previousNumberOfCachedEntries = numberOfEntriesInCache();

        final int nbIndices = randomIntBetween(min, max);
        logger.info("--> generating [{}] indices with cached entries in system index...", nbIndices);
        final Map<String, Tuple<Settings, Long>> mountedIndices = new HashMap<>();

        int i = 0;
        while (mountedIndices.size() < nbIndices) {
            final String indexName = "index-" + i;
            createIndex(indexName, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build());

            while (true) {
                final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
                for (int n = 500; n > 0; n--) {
                    final XContentBuilder builder = XContentFactory.jsonBuilder();
                    builder.startObject();
                    for (int j = 0; j < 10; j++) {
                        builder.field("text_" + j, randomRealisticUnicodeOfCodepointLength(10));
                        builder.field("int_" + j, randomInt());
                    }
                    builder.endObject();
                    indexRequestBuilders.add(client().prepareIndex(indexName).setSource(builder));
                }
                indexRandom(true, indexRequestBuilders);

                final String snapshot = "snapshot-" + i;
                createSnapshot(repositoryName, snapshot, List.of(indexName));

                final String mountedIndex = "mounted-" + indexName + "-in-" + repositoryName;
                mountSnapshot(repositoryName, snapshot, indexName, mountedIndex, Settings.EMPTY, randomFrom(Storage.values()));

                ensureGreen(mountedIndex);
                assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
                assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
                waitForBlobCacheFillsToComplete();

                refreshSystemIndex(false);
                final long numberOfEntriesInCache = numberOfEntriesInCache();
                if (numberOfEntriesInCache > previousNumberOfCachedEntries) {
                    final long nbEntries = numberOfEntriesInCache - previousNumberOfCachedEntries;
                    logger.info("--> mounted index [{}] has [{}] entries in cache", mountedIndex, nbEntries);
                    mountedIndices.put(mountedIndex, Tuple.tuple(getIndexSettings(mountedIndex), nbEntries));
                    previousNumberOfCachedEntries = numberOfEntriesInCache;
                    break;

                } else {
                    logger.info("--> mounted index [{}] did not generate any entry in cache", mountedIndex);
                    assertAcked(client().admin().cluster().prepareDeleteSnapshot(repositoryName, snapshot).get());
                    assertAcked(client().admin().indices().prepareDelete(mountedIndex));
                }
            }
            assertAcked(client().admin().indices().prepareDelete(indexName));
            i += 1;
        }
        return Collections.unmodifiableMap(mountedIndices);
    }

    private int indexRandomDocsInCache(final int minDocs, final int maxDocs, final long creationTimeInEpochMillis) {
        final int nbDocs = randomIntBetween(minDocs, maxDocs);
        final CountDownLatch latch = new CountDownLatch(nbDocs);

        String repository = randomAlphaOfLength(5).toLowerCase(Locale.ROOT);
        SnapshotId snapshotId = new SnapshotId("snap", UUIDs.randomBase64UUID());
        IndexId indexId = new IndexId("index", UUIDs.randomBase64UUID());
        ShardId shardId = new ShardId("index", UUIDs.randomBase64UUID(), randomInt(5));
        String fileName = randomAlphaOfLength(5).toLowerCase(Locale.ROOT) + '.' + randomFrom(LuceneFilesExtensions.values()).getExtension();
        byte[] bytes = randomByteArrayOfLength(randomIntBetween(1, 64));

        final BlobStoreCacheService blobStoreCacheService = internalCluster().getDataNodeInstance(BlobStoreCacheService.class);
        for (int i = 0; i < nbDocs; i++) {
            int length = randomIntBetween(1, Math.max(1, bytes.length - 1));
            blobStoreCacheService.putAsync(
                repository,
                snapshotId,
                indexId,
                shardId,
                fileName,
                ByteRange.of(i, i + length),
                new BytesArray(bytes, 0, length),
                creationTimeInEpochMillis,
                ActionListener.running(latch::countDown)
            );
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            throw new AssertionError(e);
        }
        return nbDocs;
    }
}
