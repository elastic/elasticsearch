/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.xpack.cluster.routing.allocation.DataTierAllocationDecider;
import org.elasticsearch.xpack.core.DataTier;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsResponse;
import org.elasticsearch.xpack.searchablesnapshots.cache.CacheService;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.getDataTiersPreference;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_DIRECTORY_FACTORY_KEY;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotsConstants.SNAPSHOT_RECOVERY_STATE_FACTORY_KEY;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

public class SearchableSnapshotsIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    public void testCreateAndRestoreSearchableSnapshot() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String aliasName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(
            fsRepoName,
            "fs",
            Settings.builder().put("location", randomRepoPath()).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        // Peer recovery always copies .liv files but we do not permit writing to searchable snapshot directories so this doesn't work, but
        // we can bypass this by forcing soft deletes to be used. TODO this restriction can be lifted when #55142 is resolved.
        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        assertAcked(client().admin().indices().prepareAliases().addAlias(indexName, aliasName));

        populateIndex(indexName, 10_000);

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

        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, snapshotName);
        ensureGreen(indexName);

        assertShardFolders(indexName, false);

        assertThat(
            client().admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setIndices(indexName)
                .get()
                .getState()
                .metadata()
                .index(indexName)
                .getTimestampRange(),
            sameInstance(IndexLongFieldRange.UNKNOWN)
        );

        final boolean deletedBeforeMount = randomBoolean();
        if (deletedBeforeMount) {
            assertAcked(client().admin().indices().prepareDelete(indexName));
        } else {
            assertAcked(client().admin().indices().prepareClose(indexName));
        }

        final boolean cacheEnabled = randomBoolean();
        logger.info("--> restoring index [{}] with cache [{}]", restoredIndexName, cacheEnabled ? "enabled" : "disabled");

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), cacheEnabled)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString());
        boolean preWarmEnabled = false;
        if (cacheEnabled) {
            preWarmEnabled = randomBoolean();
            indexSettingsBuilder.put(SearchableSnapshots.SNAPSHOT_CACHE_PREWARM_ENABLED_SETTING.getKey(), preWarmEnabled);
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
        final int expectedReplicas;
        if (randomBoolean()) {
            expectedReplicas = numberOfReplicas();
            indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, expectedReplicas);
        } else {
            expectedReplicas = 0;
        }
        final String expectedDataTiersPreference;
        if (randomBoolean()) {
            expectedDataTiersPreference = String.join(",", randomSubsetOf(DataTier.ALL_DATA_TIERS));
            indexSettingsBuilder.put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, expectedDataTiersPreference);
        } else {
            expectedDataTiersPreference = getDataTiersPreference(MountSearchableSnapshotRequest.Storage.FULL_COPY);
        }

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotInfo.snapshotId().getName(),
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.FULL_COPY
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        final RepositoryMetadata repositoryMetadata = client().admin()
            .cluster()
            .prepareGetRepositories(fsRepoName)
            .get()
            .repositories()
            .get(0);
        assertThat(repositoryMetadata.name(), equalTo(fsRepoName));
        assertThat(repositoryMetadata.uuid(), not(equalTo(RepositoryData.MISSING_UUID)));

        final Settings settings = client().admin()
            .indices()
            .prepareGetSettings(restoredIndexName)
            .get()
            .getIndexToSettings()
            .get(restoredIndexName);
        assertThat(SearchableSnapshots.SNAPSHOT_REPOSITORY_UUID_SETTING.get(settings), equalTo(repositoryMetadata.uuid()));
        assertThat(SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING.get(settings), equalTo(fsRepoName));
        assertThat(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(settings), equalTo(snapshotName));
        assertThat(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_DIRECTORY_FACTORY_KEY));
        assertThat(IndexModule.INDEX_RECOVERY_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_RECOVERY_STATE_FACTORY_KEY));
        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.exists(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.exists(settings));
        assertThat(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).toString(), equalTo("false"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), equalTo(expectedReplicas));
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(settings), equalTo(expectedDataTiersPreference));

        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, preWarmEnabled);
        assertSearchableSnapshotStats(restoredIndexName, cacheEnabled, nonCachedExtensions);
        ensureGreen(restoredIndexName);
        assertShardFolders(restoredIndexName, true);

        assertThat(
            client().admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setIndices(restoredIndexName)
                .get()
                .getState()
                .metadata()
                .index(restoredIndexName)
                .getTimestampRange(),
            sameInstance(IndexLongFieldRange.UNKNOWN)
        );

        if (deletedBeforeMount) {
            assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
            assertAcked(client().admin().indices().prepareAliases().addAlias(restoredIndexName, aliasName));
        } else if (indexName.equals(restoredIndexName) == false) {
            assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(1));
            assertAcked(
                client().admin()
                    .indices()
                    .prepareAliases()
                    .addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(indexName).alias(aliasName).mustExist(true))
                    .addAlias(restoredIndexName, aliasName)
            );
        }
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(1));
        assertTotalHits(aliasName, originalAllHits, originalBarHits);

        internalCluster().fullRestart();
        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, preWarmEnabled);
        assertTotalHits(aliasName, originalAllHits, originalBarHits);
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

        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, preWarmEnabled);
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

        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, preWarmEnabled);

        final String clonedIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex(restoredIndexName, clonedIndexName)
                .setResizeType(ResizeType.CLONE)
                .setSettings(
                    Settings.builder()
                        .putNull(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
                        .putNull(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey())
                        .build()
                )
        );
        ensureGreen(clonedIndexName);
        assertTotalHits(clonedIndexName, originalAllHits, originalBarHits);

        final Settings clonedIndexSettings = client().admin()
            .indices()
            .prepareGetSettings(clonedIndexName)
            .get()
            .getIndexToSettings()
            .get(clonedIndexName);
        assertFalse(clonedIndexSettings.hasValue(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_REPOSITORY_NAME_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey()));

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
        assertAcked(client().admin().indices().prepareAliases().addAlias(clonedIndexName, aliasName));
        assertTotalHits(aliasName, originalAllHits, originalBarHits);
    }

    public void testCreateAndRestorePartialSearchableSnapshot() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String aliasName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(
            fsRepoName,
            "fs",
            Settings.builder().put("location", randomRepoPath()).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
        );

        // Peer recovery always copies .liv files but we do not permit writing to searchable snapshot directories so this doesn't work, but
        // we can bypass this by forcing soft deletes to be used. TODO this restriction can be lifted when #55142 is resolved.
        assertAcked(prepareCreate(indexName, Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        assertAcked(client().admin().indices().prepareAliases().addAlias(indexName, aliasName));

        populateIndex(indexName, 10_000);

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

        final SnapshotInfo snapshotInfo = createFullSnapshot(fsRepoName, snapshotName);
        ensureGreen(indexName);

        assertShardFolders(indexName, false);

        assertThat(
            client().admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setIndices(indexName)
                .get()
                .getState()
                .metadata()
                .index(indexName)
                .getTimestampRange(),
            sameInstance(IndexLongFieldRange.UNKNOWN)
        );

        final boolean deletedBeforeMount = randomBoolean();
        if (deletedBeforeMount) {
            assertAcked(client().admin().indices().prepareDelete(indexName));
        } else {
            assertAcked(client().admin().indices().prepareClose(indexName));
        }

        logger.info("--> restoring partial index [{}] with cache enabled", restoredIndexName);

        Settings.Builder indexSettingsBuilder = Settings.builder()
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString());
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
        final int expectedReplicas;
        if (randomBoolean()) {
            expectedReplicas = numberOfReplicas();
            indexSettingsBuilder.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, expectedReplicas);
        } else {
            expectedReplicas = 0;
        }
        final String expectedDataTiersPreference;
        if (randomBoolean()) {
            expectedDataTiersPreference = String.join(",", randomSubsetOf(DataTier.ALL_DATA_TIERS));
            indexSettingsBuilder.put(DataTierAllocationDecider.INDEX_ROUTING_PREFER, expectedDataTiersPreference);
        } else {
            expectedDataTiersPreference = getDataTiersPreference(MountSearchableSnapshotRequest.Storage.SHARED_CACHE);
        }

        final AtomicBoolean statsWatcherRunning = new AtomicBoolean(true);
        final Thread statsWatcher = new Thread(() -> {
            while (statsWatcherRunning.get()) {
                final IndicesStatsResponse indicesStatsResponse;
                try {
                    indicesStatsResponse = client().admin().indices().prepareStats(restoredIndexName).clear().setStore(true).get();
                } catch (IndexNotFoundException | IndexClosedException e) {
                    continue;
                    // ok
                }

                for (ShardStats shardStats : indicesStatsResponse.getShards()) {
                    assertThat(
                        shardStats.getShardRouting().toString(),
                        shardStats.getStats().getStore().getReservedSize().getBytes(),
                        equalTo(0L)
                    );
                }
            }
        }, "test-stats-watcher");
        statsWatcher.start();

        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            restoredIndexName,
            fsRepoName,
            snapshotInfo.snapshotId().getName(),
            indexName,
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));

        statsWatcherRunning.set(false);
        statsWatcher.join();

        final Settings settings = client().admin()
            .indices()
            .prepareGetSettings(restoredIndexName)
            .get()
            .getIndexToSettings()
            .get(restoredIndexName);
        assertThat(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(settings), equalTo(snapshotName));
        assertThat(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_DIRECTORY_FACTORY_KEY));
        assertThat(IndexModule.INDEX_RECOVERY_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_RECOVERY_STATE_FACTORY_KEY));
        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.exists(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.exists(settings));
        assertThat(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).toString(), equalTo("false"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), equalTo(expectedReplicas));
        assertThat(DataTierAllocationDecider.INDEX_ROUTING_PREFER_SETTING.get(settings), equalTo(expectedDataTiersPreference));
        assertTrue(SearchableSnapshots.SNAPSHOT_PARTIAL_SETTING.get(settings));
        assertTrue(DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.get(settings));

        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, false);
        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, true, nonCachedExtensions);
        ensureGreen(restoredIndexName);
        assertShardFolders(restoredIndexName, true);

        assertThat(
            client().admin()
                .cluster()
                .prepareState()
                .clear()
                .setMetadata(true)
                .setIndices(restoredIndexName)
                .get()
                .getState()
                .metadata()
                .index(restoredIndexName)
                .getTimestampRange(),
            sameInstance(IndexLongFieldRange.UNKNOWN)
        );

        if (deletedBeforeMount) {
            assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
            assertAcked(client().admin().indices().prepareAliases().addAlias(restoredIndexName, aliasName));
        } else if (indexName.equals(restoredIndexName) == false) {
            assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(1));
            assertAcked(
                client().admin()
                    .indices()
                    .prepareAliases()
                    .addAliasAction(IndicesAliasesRequest.AliasActions.remove().index(indexName).alias(aliasName).mustExist(true))
                    .addAlias(restoredIndexName, aliasName)
            );
        }
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(1));
        assertTotalHits(aliasName, originalAllHits, originalBarHits);

        final Decision diskDeciderDecision = client().admin()
            .cluster()
            .prepareAllocationExplain()
            .setIndex(restoredIndexName)
            .setShard(0)
            .setPrimary(true)
            .setIncludeYesDecisions(true)
            .get()
            .getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getCanRemainDecision()
            .getDecisions()
            .stream()
            .filter(d -> d.label().equals(DiskThresholdDecider.NAME))
            .findFirst()
            .orElseThrow();
        assertThat(diskDeciderDecision.type(), equalTo(Decision.Type.YES));
        assertThat(
            diskDeciderDecision.getExplanation(),
            oneOf("disk watermarks are ignored on this index", "there is only a single data node present")
        );

        internalCluster().fullRestart();
        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, false);
        assertTotalHits(aliasName, originalAllHits, originalBarHits);
        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, false, nonCachedExtensions);

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

        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, false);
        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, false, nonCachedExtensions);

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

        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, false);

        final String clonedIndexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(
            client().admin()
                .indices()
                .prepareResizeIndex(restoredIndexName, clonedIndexName)
                .setResizeType(ResizeType.CLONE)
                .setSettings(
                    Settings.builder()
                        .putNull(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())
                        .putNull(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey())
                        .build()
                )
        );
        ensureGreen(clonedIndexName);
        assertTotalHits(clonedIndexName, originalAllHits, originalBarHits);

        final Settings clonedIndexSettings = client().admin()
            .indices()
            .prepareGetSettings(clonedIndexName)
            .get()
            .getIndexToSettings()
            .get(clonedIndexName);
        assertFalse(clonedIndexSettings.hasValue(IndexModule.INDEX_STORE_TYPE_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.getKey()));
        assertFalse(clonedIndexSettings.hasValue(IndexModule.INDEX_RECOVERY_TYPE_SETTING.getKey()));

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        assertThat(client().admin().indices().prepareGetAliases(aliasName).get().getAliases().size(), equalTo(0));
        assertAcked(client().admin().indices().prepareAliases().addAlias(clonedIndexName, aliasName));
        assertTotalHits(aliasName, originalAllHits, originalBarHits);
    }

    private void assertShardFolders(String indexName, boolean snapshotDirectory) throws IOException {
        final Index restoredIndex = resolveIndex(indexName);
        final String customDataPath = resolveCustomDataPath(indexName);
        final ShardId shardId = new ShardId(restoredIndex, 0);
        boolean shardFolderFound = false;
        for (String node : internalCluster().getNodeNames()) {
            final NodeEnvironment service = internalCluster().getInstance(NodeEnvironment.class, node);
            final ShardPath shardPath = ShardPath.loadShardPath(logger, service, shardId, customDataPath);
            if (shardPath != null && Files.exists(shardPath.getDataPath())) {
                shardFolderFound = true;
                assertEquals(snapshotDirectory, Files.notExists(shardPath.resolveIndex()));

                assertTrue(Files.exists(shardPath.resolveTranslog()));
                try (Stream<Path> dir = Files.list(shardPath.resolveTranslog())) {
                    final long translogFiles = dir.filter(path -> path.getFileName().toString().contains("translog")).count();
                    if (snapshotDirectory) {
                        assertEquals(2L, translogFiles);
                    } else {
                        assertThat(translogFiles, greaterThanOrEqualTo(2L));
                    }
                }
            }
        }
        assertTrue("no shard folder found", shardFolderFound);
    }

    public void testCanMountSnapshotTakenWhileConcurrentlyIndexing() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(
            fsRepoName,
            "fs",
            Settings.builder().put("location", randomRepoPath()).put("chunk_size", randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
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
            createFullSnapshot(fsRepoName, snapshotName);
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
            true,
            MountSearchableSnapshotRequest.Storage.FULL_COPY
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restoredIndexName);

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
    }

    public void testMaxRestoreBytesPerSecIsUsed() throws Exception {
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(indexName, indexSettingsNoReplicas(between(1, 3)).put(INDEX_SOFT_DELETES_SETTING.getKey(), true)));
        final int nbDocs = between(10, 50);
        indexRandom(
            true,
            false,
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setSource("foo", randomBoolean() ? "bar" : "baz"))
                .collect(Collectors.toList())
        );
        refresh(indexName);
        forceMerge();

        final Settings.Builder repositorySettings = Settings.builder().put("location", randomRepoPath());
        final boolean useRateLimits = randomBoolean();
        if (useRateLimits) {
            // we compute the min across all the max shard sizes by node in order to
            // trigger the rate limiter in all nodes. We could just use the min shard size
            // but that would make this test too slow.
            long rateLimitInBytes = getMaxShardSizeByNodeInBytes(indexName).values().stream().min(Long::compareTo).get();
            repositorySettings.put("max_restore_bytes_per_sec", new ByteSizeValue(rateLimitInBytes, ByteSizeUnit.BYTES));
        } else {
            repositorySettings.put("max_restore_bytes_per_sec", ByteSizeValue.ZERO);
        }
        createRepository(repositoryName, "fs", repositorySettings);

        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final SnapshotInfo snapshotInfo = createFullSnapshot(repositoryName, snapshotName);
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
            true,
            MountSearchableSnapshotRequest.Storage.FULL_COPY
        );

        final RestoreSnapshotResponse restore = client().execute(MountSearchableSnapshotAction.INSTANCE, mount).get();
        assertThat(restore.getRestoreInfo().failedShards(), equalTo(0));
        ensureGreen(restoredIndexName);

        assertHitCount(client().prepareSearch(restoredIndexName).setSize(0).get(), nbDocs);

        final Index restoredIndex = resolveIndex(restoredIndexName);
        for (String node : internalCluster().getNodeNames()) {
            final IndicesService service = internalCluster().getInstance(IndicesService.class, node);
            if (service != null && service.hasIndex(restoredIndex)) {
                assertThat(
                    getRepositoryOnNode(repositoryName, node).getRestoreThrottleTimeInNanos(),
                    useRateLimits ? greaterThan(0L) : equalTo(0L)
                );
            }
        }

        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
    }

    private Map<String, Long> getMaxShardSizeByNodeInBytes(String indexName) {
        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(indexName).get();
        IndexStats indexStats = indicesStats.getIndex(indexName);
        Map<String, Long> maxShardSizeByNode = new HashMap<>();
        for (ShardStats shard : indexStats.getShards()) {
            long sizeInBytes = shard.getStats().getStore().getSizeInBytes();
            if (sizeInBytes > 0) {
                maxShardSizeByNode.compute(
                    shard.getShardRouting().currentNodeId(),
                    (nodeId, maxSize) -> Math.max(maxSize == null ? 0L : maxSize, sizeInBytes)
                );
            }
        }

        return maxShardSizeByNode;
    }

    public void testMountedSnapshotHasNoReplicasByDefault() throws Exception {
        final String fsRepoName = randomAlphaOfLength(10);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String restoredIndexName = randomBoolean() ? indexName : randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final String snapshotName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);

        createRepository(fsRepoName, "fs");

        final int dataNodesCount = internalCluster().numDataNodes();
        final Settings.Builder originalIndexSettings = Settings.builder();
        originalIndexSettings.put(INDEX_SOFT_DELETES_SETTING.getKey(), true);
        if (randomBoolean()) {
            originalIndexSettings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas());
        }
        if (randomBoolean()) {
            final int replicaLimit = between(0, dataNodesCount);
            originalIndexSettings.put(
                IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS,
                replicaLimit == dataNodesCount ? "0-all" : "0-" + replicaLimit
            );
        }
        createAndPopulateIndex(indexName, originalIndexSettings);

        createFullSnapshot(fsRepoName, snapshotName);

        assertAcked(client().admin().indices().prepareDelete(indexName));

        {
            logger.info("--> restoring index [{}] with default replica counts", restoredIndexName);
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
                true,
                MountSearchableSnapshotRequest.Storage.FULL_COPY
            );

            final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
            assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
            ensureGreen(restoredIndexName);

            final Settings settings = client().admin()
                .indices()
                .prepareGetSettings(restoredIndexName)
                .get()
                .getIndexToSettings()
                .get(restoredIndexName);
            assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), equalTo(0));
            assertThat(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).toString(), equalTo("false"));

            assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        }

        {
            final int replicaCount = numberOfReplicas();

            logger.info("--> restoring index [{}] with specific replica count", restoredIndexName);
            Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString())
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, replicaCount);
            final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
                restoredIndexName,
                fsRepoName,
                snapshotName,
                indexName,
                indexSettingsBuilder.build(),
                Strings.EMPTY_ARRAY,
                true,
                MountSearchableSnapshotRequest.Storage.FULL_COPY
            );

            final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
            assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
            ensureGreen(restoredIndexName);

            final Settings settings = client().admin()
                .indices()
                .prepareGetSettings(restoredIndexName)
                .get()
                .getIndexToSettings()
                .get(restoredIndexName);
            assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), equalTo(replicaCount));
            assertThat(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).toString(), equalTo("false"));

            assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        }

        {
            final int replicaLimit = between(0, dataNodesCount);
            logger.info("--> restoring index [{}] with auto-expand replicas configured", restoredIndexName);
            Settings.Builder indexSettingsBuilder = Settings.builder()
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), Boolean.FALSE.toString())
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, replicaLimit == dataNodesCount ? "0-all" : "0-" + replicaLimit);
            final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
                restoredIndexName,
                fsRepoName,
                snapshotName,
                indexName,
                indexSettingsBuilder.build(),
                Strings.EMPTY_ARRAY,
                true,
                MountSearchableSnapshotRequest.Storage.FULL_COPY
            );

            final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
            assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
            ensureGreen(restoredIndexName);

            final ClusterState state = client().admin().cluster().prepareState().clear().setRoutingTable(true).get().getState();
            assertThat(
                state.toString(),
                state.routingTable().index(restoredIndexName).shard(0).size(),
                equalTo(Math.min(replicaLimit + 1, dataNodesCount))
            );

            assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        }
    }

    public void testSnapshotMountedIndexLeavesBlobsUntouched() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numShards = between(1, 3);
        createAndPopulateIndex(indexName, indexSettingsNoReplicas(numShards).put(INDEX_SOFT_DELETES_SETTING.getKey(), true));
        ensureGreen(indexName);
        forceMerge();

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "fs");

        final SnapshotId snapshotOne = createSnapshot(repositoryName, "snapshot-1", List.of(indexName)).snapshotId();
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final SnapshotStatus snapshotOneStatus = client().admin()
            .cluster()
            .prepareSnapshotStatus(repositoryName)
            .setSnapshots(snapshotOne.getName())
            .get()
            .getSnapshots()
            .get(0);
        final int snapshotOneTotalFileCount = snapshotOneStatus.getStats().getTotalFileCount();
        assertThat(snapshotOneTotalFileCount, greaterThan(0));

        mountSnapshot(repositoryName, snapshotOne.getName(), indexName, indexName, Settings.EMPTY);
        ensureGreen(indexName);

        final SnapshotId snapshotTwo = createSnapshot(repositoryName, "snapshot-2", List.of(indexName)).snapshotId();
        final SnapshotStatus snapshotTwoStatus = client().admin()
            .cluster()
            .prepareSnapshotStatus(repositoryName)
            .setSnapshots(snapshotTwo.getName())
            .get()
            .getSnapshots()
            .get(0);
        assertThat(snapshotTwoStatus.getStats().getTotalFileCount(), equalTo(numShards)); // one segment_N per shard
        assertThat(snapshotTwoStatus.getStats().getIncrementalFileCount(), equalTo(0));
        assertThat(snapshotTwoStatus.getStats().getProcessedFileCount(), equalTo(0));
    }

    public void testSnapshotMountedIndexWithTimestampsRecordsTimestampRangeInIndexMetadata() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numShards = between(1, 3);

        final String dateType = randomFrom("date", "date_nanos");
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setMapping(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject("properties")
                        .startObject(DataStream.TimestampField.FIXED_TIMESTAMP_FIELD)
                        .field("type", dateType)
                        .field("format", "strict_date_optional_time_nanos")
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .setSettings(indexSettingsNoReplicas(numShards).put(INDEX_SOFT_DELETES_SETTING.getKey(), true))
        );
        ensureGreen(indexName);

        final List<IndexRequestBuilder> indexRequestBuilders = new ArrayList<>();
        final int docCount = between(0, 1000);
        for (int i = 0; i < docCount; i++) {
            indexRequestBuilders.add(
                client().prepareIndex(indexName)
                    .setSource(
                        DataStream.TimestampField.FIXED_TIMESTAMP_FIELD,
                        String.format(
                            Locale.ROOT,
                            "2020-11-26T%02d:%02d:%02d.%09dZ",
                            between(0, 23),
                            between(0, 59),
                            between(0, 59),
                            randomLongBetween(0, 999999999L)
                        )
                    )
            );
        }
        indexRandom(true, false, indexRequestBuilders);
        assertThat(
            client().admin().indices().prepareForceMerge(indexName).setOnlyExpungeDeletes(true).setFlush(true).get().getFailedShards(),
            equalTo(0)
        );
        refresh(indexName);
        forceMerge();

        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(repositoryName, "fs");

        final SnapshotId snapshotOne = createSnapshot(repositoryName, "snapshot-1", List.of(indexName)).snapshotId();
        assertAcked(client().admin().indices().prepareDelete(indexName));

        mountSnapshot(repositoryName, snapshotOne.getName(), indexName, indexName, Settings.EMPTY);
        ensureGreen(indexName);

        final IndexLongFieldRange timestampRange = client().admin()
            .cluster()
            .prepareState()
            .clear()
            .setMetadata(true)
            .setIndices(indexName)
            .get()
            .getState()
            .metadata()
            .index(indexName)
            .getTimestampRange();

        assertTrue(timestampRange.isComplete());
        assertThat(timestampRange, not(sameInstance(IndexLongFieldRange.UNKNOWN)));
        if (docCount == 0) {
            assertThat(timestampRange, sameInstance(IndexLongFieldRange.EMPTY));
        } else {
            assertThat(timestampRange, not(sameInstance(IndexLongFieldRange.EMPTY)));
            DateFieldMapper.Resolution resolution = dateType.equals("date")
                ? DateFieldMapper.Resolution.MILLISECONDS
                : DateFieldMapper.Resolution.NANOSECONDS;
            assertThat(timestampRange.getMin(), greaterThanOrEqualTo(resolution.convert(Instant.parse("2020-11-26T00:00:00Z"))));
            assertThat(timestampRange.getMin(), lessThanOrEqualTo(resolution.convert(Instant.parse("2020-11-27T00:00:00Z"))));
        }
    }

    public void testSnapshotOfSearchableSnapshotIncludesNoDataButCanBeRestored() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createAndPopulateIndex(
            indexName,
            Settings.builder().put(INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1).put(INDEX_SOFT_DELETES_SETTING.getKey(), true)
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

        // The repository that contains the actual data
        final String repositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final boolean hasRepositoryUuid = randomBoolean();
        if (hasRepositoryUuid) {
            createRepository(repositoryName, "fs");
        } else {
            // Prepare the repo with an older version first, to suppress the repository UUID and fall back to matching by repo name
            final String tmpRepositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createRepositoryNoVerify(tmpRepositoryName, "fs");
            final Path repoPath = internalCluster().getCurrentMasterNodeInstance(Environment.class)
                .resolveRepoFile(
                    client().admin()
                        .cluster()
                        .prepareGetRepositories(tmpRepositoryName)
                        .get()
                        .repositories()
                        .get(0)
                        .settings()
                        .get("location")
                );
            initWithSnapshotVersion(
                tmpRepositoryName,
                repoPath,
                randomFrom(
                    SnapshotsService.OLD_SNAPSHOT_FORMAT,
                    SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION,
                    SnapshotsService.INDEX_GEN_IN_REPO_DATA_VERSION
                )
            );
            assertAcked(client().admin().cluster().prepareDeleteRepository(tmpRepositoryName));
            createRepository(repositoryName, "fs", repoPath);
        }

        final SnapshotId snapshotOne = createSnapshot(repositoryName, "snapshot-1", List.of(indexName)).snapshotId();
        for (final SnapshotStatus snapshotStatus : client().admin()
            .cluster()
            .prepareSnapshotStatus(repositoryName)
            .setSnapshots(snapshotOne.getName())
            .get()
            .getSnapshots()) {
            for (final SnapshotIndexShardStatus snapshotIndexShardStatus : snapshotStatus.getShards()) {
                final SnapshotStats stats = snapshotIndexShardStatus.getStats();
                assertThat(stats.getIncrementalFileCount(), greaterThan(1));
                assertThat(stats.getProcessedFileCount(), greaterThan(1));
                assertThat(stats.getTotalFileCount(), greaterThan(1));
            }
        }
        assertAcked(client().admin().indices().prepareDelete(indexName));

        assertThat(
            client().admin().cluster().prepareGetRepositories(repositoryName).get().repositories().get(0).uuid(),
            hasRepositoryUuid ? not(equalTo(RepositoryData.MISSING_UUID)) : equalTo(RepositoryData.MISSING_UUID)
        );

        final String restoredIndexName = randomValueOtherThan(indexName, () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
        mountSnapshot(repositoryName, snapshotOne.getName(), indexName, restoredIndexName, Settings.EMPTY);
        ensureGreen(restoredIndexName);

        if (randomBoolean()) {
            logger.info("--> closing index before snapshot");
            assertAcked(client().admin().indices().prepareClose(restoredIndexName));
        }

        // The repository that contains the cluster snapshot (may be different from the one containing the data)
        final String backupRepositoryName;
        if (randomBoolean()) {
            backupRepositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createRepository(backupRepositoryName, "fs");
        } else {
            backupRepositoryName = repositoryName;
        }

        logger.info("--> starting to take snapshot-2");
        final SnapshotId snapshotTwo = createSnapshot(backupRepositoryName, "snapshot-2", List.of(restoredIndexName)).snapshotId();
        logger.info("--> finished taking snapshot-2");
        for (final SnapshotStatus snapshotStatus : client().admin()
            .cluster()
            .prepareSnapshotStatus(backupRepositoryName)
            .setSnapshots(snapshotTwo.getName())
            .get()
            .getSnapshots()) {
            assertThat(snapshotStatus.getIndices().size(), equalTo(1));
            assertTrue(snapshotStatus.getIndices().containsKey(restoredIndexName));
            for (final SnapshotIndexShardStatus snapshotIndexShardStatus : snapshotStatus.getShards()) {
                final SnapshotStats stats = snapshotIndexShardStatus.getStats();
                assertThat(stats.getIncrementalFileCount(), equalTo(1));
                assertThat(stats.getProcessedFileCount(), equalTo(1));
                assertThat(stats.getTotalFileCount(), equalTo(1));
            }
        }
        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));

        // The repository that contains the cluster snapshot -- may be different from backupRepositoryName if we're only using one repo and
        // we rename it.
        final String restoreRepositoryName;
        if (hasRepositoryUuid && randomBoolean()) {
            // Re-mount the repository containing the actual data under a different name
            final RepositoryMetadata repositoryMetadata = client().admin()
                .cluster()
                .prepareGetRepositories(repositoryName)
                .get()
                .repositories()
                .get(0);

            // Rename the repository containing the actual data.
            final String newRepositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            assertAcked(client().admin().cluster().prepareDeleteRepository(repositoryName));
            final Settings.Builder settings = Settings.builder().put(repositoryMetadata.settings());
            if (randomBoolean()) {
                settings.put(READONLY_SETTING_KEY, "true");
            }
            assertAcked(
                clusterAdmin().preparePutRepository(newRepositoryName).setType("fs").setSettings(settings).setVerify(randomBoolean())
            );
            restoreRepositoryName = backupRepositoryName.equals(repositoryName) ? newRepositoryName : backupRepositoryName;
        } else {
            restoreRepositoryName = backupRepositoryName;
        }

        logger.info("--> starting to restore snapshot-2");
        assertThat(
            client().admin()
                .cluster()
                .prepareRestoreSnapshot(restoreRepositoryName, snapshotTwo.getName())
                .setIndices(restoredIndexName)
                .get()
                .status(),
            equalTo(RestStatus.ACCEPTED)
        );
        ensureGreen(restoredIndexName);
        logger.info("--> finished restoring snapshot-2");

        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
    }

    private void assertTotalHits(String indexName, TotalHits originalAllHits, TotalHits originalBarHits) throws Exception {
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

        for (int i = 0; i < threads.length; i++) {
            threads[i].join();

            final TotalHits allTotalHits = allHits.get(i);
            final TotalHits barTotalHits = barHits.get(i);

            logger.info("--> thread #{} has [{}] hits in total, of which [{}] match the query", i, allTotalHits, barTotalHits);
            assertThat(allTotalHits, equalTo(originalAllHits));
            assertThat(barTotalHits, equalTo(originalBarHits));
        }
    }

    private void assertRecoveryStats(String indexName, boolean preWarmEnabled) throws Exception {
        int shardCount = getNumShards(indexName).totalNumShards;
        assertBusy(() -> {
            final RecoveryResponse recoveryResponse = client().admin().indices().prepareRecoveries(indexName).get();
            assertThat(recoveryResponse.toString(), recoveryResponse.shardRecoveryStates().get(indexName).size(), equalTo(shardCount));

            for (List<RecoveryState> recoveryStates : recoveryResponse.shardRecoveryStates().values()) {
                for (RecoveryState recoveryState : recoveryStates) {
                    ByteSizeValue cacheSize = getCacheSizeForNode(recoveryState.getTargetNode().getName());
                    boolean unboundedCache = cacheSize.equals(new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES));
                    RecoveryState.Index index = recoveryState.getIndex();
                    assertThat(
                        Strings.toString(recoveryState, true, true),
                        index.recoveredFileCount(),
                        preWarmEnabled && unboundedCache ? equalTo(index.totalRecoverFiles()) : greaterThanOrEqualTo(0)
                    );

                    // Since the cache size is variable, the pre-warm phase might fail as some of the files can be evicted
                    // while a part is pre-fetched, in that case the recovery state stage is left as FINALIZE.
                    assertThat(
                        recoveryState.getStage(),
                        unboundedCache
                            ? equalTo(RecoveryState.Stage.DONE)
                            : anyOf(equalTo(RecoveryState.Stage.DONE), equalTo(RecoveryState.Stage.FINALIZE))
                    );
                }
            }
        }, 30L, TimeUnit.SECONDS);
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
                            max(indexInputStats.getOptimizedBytesRead().getCount(), indexInputStats.getDirectBytesRead().getCount()),
                            greaterThan(0L)
                        );
                        assertThat(
                            "Expected no cache read or write for " + fileName + " of shard " + shardRouting,
                            max(indexInputStats.getCachedBytesRead().getCount(), indexInputStats.getCachedBytesWritten().getCount()),
                            equalTo(0L)
                        );
                    } else if (nodeIdsWithLargeEnoughCache.contains(stats.getShardRouting().currentNodeId())) {
                        assertThat(
                            "Expected at least 1 cache read or write for " + fileName + " of shard " + shardRouting,
                            max(
                                indexInputStats.getCachedBytesRead().getCount(),
                                indexInputStats.getCachedBytesWritten().getCount(),
                                indexInputStats.getIndexCacheBytesRead().getCount()
                            ),
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
                            max(
                                indexInputStats.getCachedBytesRead().getCount(),
                                indexInputStats.getCachedBytesWritten().getCount(),
                                indexInputStats.getOptimizedBytesRead().getCount(),
                                indexInputStats.getDirectBytesRead().getCount(),
                                indexInputStats.getIndexCacheBytesRead().getCount()
                            ),
                            greaterThan(0L)
                        );
                    }
                }
            }
        }
    }

    private static long max(long... values) {
        return Arrays.stream(values).max().orElseThrow(() -> new AssertionError("no values"));
    }

    private ByteSizeValue getCacheSizeForNode(String nodeName) {
        return CacheService.SNAPSHOT_CACHE_SIZE_SETTING.get(internalCluster().getInstance(Environment.class, nodeName).settings());
    }
}
