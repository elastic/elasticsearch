/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_DIRECTORY_FACTORY_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SearchableSnapshotsIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testCreateAndRestoreSearchableSnapshot() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String aliasName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final Path repo = randomRepoPath();
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(fsRepoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repo).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES))
        );

        // Peer recovery always copies .liv files but we do not permit writing to searchable snapshot directories so this doesn't work, but
        // we can bypass this by forcing soft deletes to be used. TODO this restriction can be lifted when #55142 is resolved.
        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        assertAcked(client().admin().indices().prepareAliases().addAlias(indexName, aliasName));

        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        for (int i = between(10, 10_000); i >= 0; i--) {
            indexRequestBuilders.add(client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
        }
        indexRandom(true, true, indexRequestBuilders);
        refresh(indexName);
        assertThat(
            client().admin().indices().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
            equalTo(0)
        );

        final TotalHits originalAllHits = internalCluster().client()
            .prepareSearch(indexName)
            .setTrackTotalHits(true)
            .get()
            .getHits()
            .getTotalHits();
        final TotalHits originalBarHits = internalCluster().client()
            .prepareSearch(indexName)
            .setTrackTotalHits(true)
            .setQuery(matchQuery("foo", "bar"))
            .get()
            .getHits()
            .getTotalHits();
        logger.info("--> [{}] in total, of which [{}] match the query", originalAllHits, originalBarHits);

        expectThrows(
            ResourceNotFoundException.class,
            "Searchable snapshot stats on a non snapshot searchable index should fail",
            () -> client().execute(SearchableSnapshotsStatsAction.INSTANCE, new SearchableSnapshotsStatsRequest()).actionGet()
        );

        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(fsRepoName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final boolean cacheEnabled = randomBoolean();
        logger.info("--> restoring index [{}] with cache [{}]", restoredIndexName, cacheEnabled ? "enabled" : "disabled");

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), cacheEnabled)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString());
        if (cacheEnabled) {
            indexSettingsBuilder.put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), randomBoolean());
        }
        final List<String> nonCachedExtensions;
        if (randomBoolean()) {
            nonCachedExtensions = randomSubsetOf(Arrays.asList("fdt", "fdx", "nvd", "dvd", "tip", "cfs", "dim"));
            indexSettingsBuilder.putList(SearchableSnapshots.SNAPSHOT_CACHE_EXCLUDED_FILE_TYPES_SETTING.getKey(), nonCachedExtensions);
        } else {
            nonCachedExtensions = Collections.emptyList();
        }
        if (randomBoolean()) {
            indexSettingsBuilder.put(
                SearchableSnapshots.SNAPSHOT_UNCACHED_CHUNK_SIZE_SETTING.getKey(),
                new ByteSizeValue(randomLongBetween(10, 100_000))
            );
        }
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotInfo.snapshotId().getName(),
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        final Settings settings = client().admin()
            .indices()
            .prepareGetSettings(restoredIndexName)
            .get()
            .getIndexToSettings()
            .get(restoredIndexName);
        assertThat(SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING.get(settings), equalTo(fsRepoName));
        assertThat(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(settings), equalTo(snapshotName));
        assertThat(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_DIRECTORY_FACTORY_KEY));
        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.exists(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.exists(settings));

        assertRecovered(restoredIndexName, originalAllHits, originalBarHits);
        assertSearchableSnapshotStats(restoredIndexName, cacheEnabled, nonCachedExtensions);

        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
        assertAcked(client().admin().indices().prepareAliases().addAlias(restoredIndexName, aliasName));
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(1));
        assertRecovered(aliasName, originalAllHits, originalBarHits, false);

        internalCluster().fullRestart();
        assertRecovered(restoredIndexName, originalAllHits, originalBarHits);
        assertRecovered(aliasName, originalAllHits, originalBarHits, false);
        assertSearchableSnapshotStats(restoredIndexName, cacheEnabled, nonCachedExtensions);

        internalCluster().ensureAtLeastNumDataNodes(2);

        final DiscoveryNode dataNode = randomFrom(
            StreamSupport.stream(
                client().admin().cluster().prepareState().get().getState().nodes().getDataNodes().values().spliterator(),
                false
            ).map(c -> c.value).toArray(DiscoveryNode[]::new)
        );

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(restoredIndexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(
                            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                            dataNode.getName()
                        )
                )
        );

        assertFalse(
            client().admin()
                .cluster()
                .prepareHealth(restoredIndexName)
                .setWaitForNoRelocatingShards(true)
                .setWaitForEvents(Priority.LANGUID)
                .get()
                .isTimedOut()
        );

        assertRecovered(restoredIndexName, originalAllHits, originalBarHits);
        assertSearchableSnapshotStats(restoredIndexName, cacheEnabled, nonCachedExtensions);

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings(restoredIndexName)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                        .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey())
                )
        );

        final String clonedIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex(restoredIndexName, clonedIndexName)
                .setResizeType(ResizeType.CLONE)
                .setSettings(Settings.builder().putNull(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()).build())
        );
        ensureGreen(clonedIndexName);
        assertRecovered(clonedIndexName, originalAllHits, originalBarHits, false);

        final Settings clonedIndexSettings = client().admin()
            .indices()
            .prepareGetSettings(clonedIndexName)
            .get()
            .getIndexToSettings()
            .get(clonedIndexName);
        assertFalse(clonedIndexSettings.hasValue(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_REPOSITORY_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.getKey()));

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
        assertAcked(client().admin().indices().prepareAliases().addAlias(clonedIndexName, aliasName));
        assertRecovered(aliasName, originalAllHits, originalBarHits, false);

    }

    public void testCanMountSnapshotTakenWhileConcurrentlyIndexing() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final Path repo = randomRepoPath();
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(fsRepoName)
                .setType("fs")
                .setSettings(Settings.builder().put("location", repo).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES))
        );

        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));

        final CyclicBarrier cyclicBarrier = new CyclicBarrier(2);

        final Thread indexingThead = new Thread(() -> {
            final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
            for (int i = between(10, 10_000); i >= 0; i--) {
                indexRequestBuilders.add(client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"));
            }
            try {
                cyclicBarrier.await();
                indexRandom(true, true, indexRequestBuilders);
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new AssertionError(e);
            }
            refresh(indexName);
            assertThat(
                client().admin().indices().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
                equalTo(0)
            );
        });

        final Thread snapshotThread = new Thread(() -> {
            try {
                cyclicBarrier.await();
            } catch (InterruptedException | BrokenBarrierException e) {
                throw new AssertionError(e);
            }
            CreateSnapshotResponse createSnapshotResponse = client().admin()
                .cluster()
                .prepareCreateSnapshot(fsRepoName, snapshotName)
                .setWaitForCompletion(true)
                .get();
            final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
            assertThat(snapshotInfo.successfulShards(), greaterThan(0));
            assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));
        });

        indexingThead.start();
        snapshotThread.start();
        indexingThead.join();
        snapshotThread.join();

        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> restoring index [{}]", restoredIndexName);

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString());
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotName,
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restoredIndexName);
    }

    public void testMaxRestoreBytesPerSecIsUsed() throws Exception {
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Settings.Builder repositorySettings = Settings.builder().put("location", randomRepoPath());
        final boolean useRateLimits = randomBoolean();
        if (useRateLimits) {
            repositorySettings.put("max_restore_bytes_per_sec", new ByteSizeValue(10, ByteSizeUnit.KB));
        } else {
            repositorySettings.put("max_restore_bytes_per_sec", ByteSizeValue.ZERO);
        }
        assertAcked(
            client().admin().cluster().preparePutRepository(repositoryName).setType(FsRepository.TYPE).setSettings(repositorySettings)
        );

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 3))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
            )
        );
        final int nbDocs = between(10, 50);
        indexRandom(
            true,
            false,
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"))
                .collect(Collectors.toList())
        );
        refresh(indexName);

        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        CreateSnapshotResponse createSnapshotResponse = client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .get();
        final SnapshotInfo snapshotInfo = createSnapshotResponse.getSnapshotInfo();
        assertThat(snapshotInfo.successfulShards(), greaterThan(0));
        assertThat(snapshotInfo.successfulShards(), equalTo(snapshotInfo.totalShards()));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> restoring index [{}] using rate limits [{}]", restoredIndexName, useRateLimits);
        final MountSearchableSnapshotRequest mount = new MountSearchableSnapshotRequest(
            restoredIndexName,
            repositoryName,
            snapshotName,
            indexName,
            Settings.builder().put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString()).build(),
            Strings.EMPTY_ARRAY,
            true
        );

        final RestoreSnapshotResponse restore = client().execute(MountSearchableSnapshotAction.INSTANCE, mount).get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restoredIndexName);

        assertHitCount(client().prepareSearch(restoredIndexName).setSize(0).get(), nbDocs);

        final Index restoredIndex = resolveIndex(restoredIndexName);
        for (String node : internalCluster().getNodeNames()) {
            final IndicesService service = internalCluster().getInstance(IndicesService.class, node);
            if (service != null && service.hasIndex(restoredIndex)) {
                final RepositoriesService repositoriesService = internalCluster().getInstance(RepositoriesService.class, node);
                assertThat(
                    repositoriesService.repository(repositoryName).getRestoreThrottleTimeInNanos(),
                    useRateLimits ? greaterThan(0L) : equalTo(0L)
                );
            }
        }
    }

    private void assertRecovered(String indexName, TotalHits originalAllHits, TotalHits originalBarHits) throws Exception {
        assertRecovered(indexName, originalAllHits, originalBarHits, true);
    }

    private void assertRecovered(String indexName, TotalHits originalAllHits, TotalHits originalBarHits, boolean checkRecoveryStats)
        throws Exception {

        final Thread[] threads = new Thread[between(1, 5)];
        final AtomicArray<TotalHits> allHits = new AtomicArray<>(threads.length);
        final AtomicArray<TotalHits> barHits = new AtomicArray<>(threads.length);

        final CountDownLatch latch = new CountDownLatch(1);
        for (int i = 0; i < threads.length; i++) {
            int t = i;
            threads[i] = new Thread(() -> {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                allHits.set(t, client().prepareSearch(indexName).setTrackTotalHits(true).get().getHits().getTotalHits());
                barHits.set(
                    t,
                    client().prepareSearch(indexName)
                        .setTrackTotalHits(true)
                        .setQuery(matchQuery("foo", "bar"))
                        .get()
                        .getHits()
                        .getTotalHits()
                );
            });
            threads[i].start();
        }

        ensureGreen(indexName);
        latch.countDown();

        if (checkRecoveryStats) {
            final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).get();
            for (List<RecoveryState> recoveryStates : recoveryResponse.shardRecoveryStates().values()) {
                for (RecoveryState recoveryState : recoveryStates) {
                    logger.info("Checking {}[{}]", recoveryState.getShardId(), recoveryState.getPrimary() ? "p" : "r");
                    assertThat(
                        Strings.toString(recoveryState), // we make a new commit so we write a new `segments_n` file
                        recoveryState.getIndex().recoveredFileCount(),
                        lessThanOrEqualTo(1)
                    );
                }
            }
        }

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();

            final TotalHits allTotalHits = allHits.get(i);
            final TotalHits barTotalHits = barHits.get(i);

            logger.info("--> thread #{} has [{}] hits in total, of which [{}] match the query", i, allTotalHits, barTotalHits);
            assertThat(allTotalHits, equalTo(originalAllHits));
            assertThat(barTotalHits, equalTo(originalBarHits));
        }
    }

    private void assertSearchableSnapshotStats(String indexName, boolean cacheEnabled, List<String> nonCachedExtensions) {
        final SearchableSnapshotsStatsResponse statsResponse = client().execute(
            SearchableSnapshotsStatsAction.INSTANCE,
            new SearchableSnapshotsStatsRequest(indexName)
        ).actionGet();
        final NumShards restoredNumShards = getNumShards(indexName);
        assertThat(statsResponse.getStats(), hasSize(restoredNumShards.totalNumShards));

        final long totalSize = statsResponse.getStats()
            .stream()
            .flatMap(s -> s.getStats().stream())
            .mapToLong(SearchableSnapshotShardStats.CacheIndexInputStats::getFileLength)
            .sum();
        final Set<String> nodeIdsWithLargeEnoughCache = new HashSet<>();
        for (ObjectCursor<DiscoveryNode> nodeCursor : client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setNodes(true)
            .get()
            .getState()
            .nodes()
            .getDataNodes()
            .values()) {
            final Settings nodeSettings = internalCluster().getInstance(Environment.class, nodeCursor.value.getName()).settings();
            if (totalSize <= CacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(nodeSettings).getBytes()) {
                nodeIdsWithLargeEnoughCache.add(nodeCursor.value.getId());
            }
        }
        assertThat("Expecting stats to exist for at least one Lucene file", totalSize, greaterThan(0L));

        for (SearchableSnapshotShardStats stats : statsResponse.getStats()) {
            final ShardRouting shardRouting = stats.getShardRouting();
            assertThat(stats.getShardRouting().getIndexName(), equalTo(indexName));
            if (shardRouting.started()) {
                for (SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : stats.getStats()) {
                    final String fileName = indexInputStats.getFileName();
                    assertThat(
                        "Unexpected open count for " + fileName + " of shard " + shardRouting,
                        indexInputStats.getOpenCount(),
                        greaterThan(0L)
                    );
                    assertThat(
                        "Unexpected close count for " + fileName + " of shard " + shardRouting,
                        indexInputStats.getCloseCount(),
                        lessThanOrEqualTo(indexInputStats.getOpenCount())
                    );
                    assertThat(
                        "Unexpected file length for " + fileName + " of shard " + shardRouting,
                        indexInputStats.getFileLength(),
                        greaterThan(0L)
                    );

                    if (cacheEnabled == false || nonCachedExtensions.contains(IndexFileNames.getExtension(fileName))) {
                        assertThat(
                            "Expected at least 1 optimized or direct read for " + fileName + " of shard " + shardRouting,
                            Math.max(indexInputStats.getOptimizedBytesRead().getCount(), indexInputStats.getDirectBytesRead().getCount()),
                            greaterThan(0L)
                        );
                        assertThat(
                            "Expected no cache read or write for " + fileName + " of shard " + shardRouting,
                            Math.max(indexInputStats.getCachedBytesRead().getCount(), indexInputStats.getCachedBytesWritten().getCount()),
                            equalTo(0L)
                        );
                    } else if (nodeIdsWithLargeEnoughCache.contains(stats.getShardRouting().currentNodeId())) {
                        assertThat(
                            "Expected at least 1 cache read or write for " + fileName + " of shard " + shardRouting,
                            Math.max(indexInputStats.getCachedBytesRead().getCount(), indexInputStats.getCachedBytesWritten().getCount()),
                            greaterThan(0L)
                        );
                        assertThat(
                            "Expected no optimized read for " + fileName + " of shard " + shardRouting,
                            indexInputStats.getOptimizedBytesRead().getCount(),
                            equalTo(0L)
                        );
                        assertThat(
                            "Expected no direct read for " + fileName + " of shard " + shardRouting,
                            indexInputStats.getDirectBytesRead().getCount(),
                            equalTo(0L)
                        );
                    } else {
                        assertThat(
                            "Expected at least 1 read or write of any kind for " + fileName + " of shard " + shardRouting,
                            Math.max(
                                Math.max(
                                    indexInputStats.getCachedBytesRead().getCount(),
                                    indexInputStats.getCachedBytesWritten().getCount()
                                ),
                                Math.max(
                                    indexInputStats.getOptimizedBytesRead().getCount(),
                                    indexInputStats.getDirectBytesRead().getCount()
                                )
                            ),
                            greaterThan(0L)
                        );
                    }
                }
            }
        }
    }
}
