/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.blob;

import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest.Storage;
import org.elasticsearch.xpack.searchablesnapshots.BaseFrozenSearchableSnapshotsIntegTestCase;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
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
    public void testMaintenance() throws Exception {
        final String repositoryName = "repository";
        createRepository(repositoryName, FsRepository.TYPE);

        final int nbIndices = randomIntBetween(3, 10);

        logger.info("--> generating [{}] indices with cached entries in system index...", nbIndices);
        final Map<String, Long> mountedIndices = new HashMap<>();
        final Map<String, Settings> mountedIndicesSettings = new HashMap<>();

        int i = 0;
        long previousNumberOfCachedEntries = 0;
        while (mountedIndices.size() < nbIndices) {
            final String indexName = "index-" + i;
            createIndex(indexName);

            final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
            for (int n = 100; n > 0; n--) {
                indexRequestBuilders.add(
                    client().prepareIndex(indexName, SINGLE_MAPPING_NAME)
                        .setSource(
                            XContentFactory.smileBuilder()
                                .startObject()
                                .field("text", randomRealisticUnicodeOfCodepointLength(10))
                                .endObject()
                        )
                );
            }
            indexRandom(true, indexRequestBuilders);

            createSnapshot(repositoryName, "snapshot-" + i, Collections.singletonList(indexName));
            assertAcked(client().admin().indices().prepareDelete(indexName));

            final String mountedIndex = "mounted-index-" + i;
            mountSnapshot(repositoryName, "snapshot-" + i, "index-" + i, mountedIndex, Settings.EMPTY, randomFrom(Storage.values()));

            ensureGreen(mountedIndex);
            assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
            assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
            waitForBlobCacheFillsToComplete();

            refreshSystemIndex(false);

            final long numberOfEntriesInCache = numberOfEntriesInCache();
            if (numberOfEntriesInCache > previousNumberOfCachedEntries) {
                final long nbEntries = numberOfEntriesInCache - previousNumberOfCachedEntries;
                logger.info("--> mounted index [{}] has [{}] entries in cache", mountedIndex, nbEntries);
                mountedIndices.put(mountedIndex, nbEntries);
                mountedIndicesSettings.put(mountedIndex, getIndexSettings(mountedIndex));

            } else {
                logger.info("--> mounted index [{}] did not generate any entry in cache, skipping", mountedIndex);
                assertAcked(client().admin().indices().prepareDelete(mountedIndex));
            }

            previousNumberOfCachedEntries = numberOfEntriesInCache;
            i += 1;
        }

        ensureYellow(SNAPSHOT_BLOB_CACHE_INDEX);
        refreshSystemIndex(true);

        final long numberOfEntriesInCache = numberOfEntriesInCache();
        logger.info("--> found [{}] entries in snapshot blob cache", numberOfEntriesInCache);
        assertThat(numberOfEntriesInCache, equalTo(mountedIndices.values().stream().mapToLong(l -> l).sum()));

        final List<String> indicesToDelete = randomSubsetOf(randomIntBetween(1, mountedIndices.size()), mountedIndices.keySet());
        assertAcked(client().admin().indices().prepareDelete(indicesToDelete.toArray(new String[0])));

        final long expectedDeletedEntriesInCache = mountedIndices.entrySet()
            .stream()
            .filter(e -> indicesToDelete.contains(e.getKey()))
            .mapToLong(Map.Entry::getValue)
            .sum();
        logger.info("--> deleting indices [{}] with [{}] entries in snapshot blob cache", indicesToDelete, expectedDeletedEntriesInCache);

        assertBusy(() -> {
            refreshSystemIndex(true);
            assertThat(numberOfEntriesInCache(), equalTo(numberOfEntriesInCache - expectedDeletedEntriesInCache));

            for (String mountedIndex : mountedIndices.keySet()) {
                final Settings indexSettings = mountedIndicesSettings.get(mountedIndex);
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
                    indicesToDelete.contains(mountedIndex) ? 0L : mountedIndices.get(mountedIndex)
                );
            }
        });

        final Set<String> remainingIndices = mountedIndices.keySet()
            .stream()
            .filter(index -> indicesToDelete.contains(index) == false)
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
            mountedIndicesSettings.put(remainingMountedIndex, getIndexSettings(remainingMountedIndex));

            assertExecutorIsIdle(SearchableSnapshots.CACHE_FETCH_ASYNC_THREAD_POOL_NAME);
            assertExecutorIsIdle(SearchableSnapshots.CACHE_PREWARMING_THREAD_POOL_NAME);
            waitForBlobCacheFillsToComplete();

            logger.info(
                "--> deleting more mounted indices [{}] with snapshot [{}/{}] of index [{}] is still mounted as index [{}]",
                moreIndicesToDelete,
                snapshotId,
                snapshotIndexName,
                snapshotIndexName,
                remainingMountedIndex
            );
            assertAcked(client().admin().indices().prepareDelete(moreIndicesToDelete.toArray(new String[0])));

            assertBusy(() -> {
                refreshSystemIndex(true);

                for (String mountedIndex : mountedIndices.keySet()) {
                    final Settings indexSettings = mountedIndicesSettings.get(mountedIndex);

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
                        assertThat(remainingEntriesInCache, greaterThanOrEqualTo(mountedIndices.get(randomMountedIndex)));
                    } else if (moreIndicesToDelete.contains(mountedIndex)) {
                        assertThat(remainingEntriesInCache, equalTo(0L));
                    } else {
                        assertThat(remainingEntriesInCache, equalTo(mountedIndices.get(mountedIndex)));
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
}
