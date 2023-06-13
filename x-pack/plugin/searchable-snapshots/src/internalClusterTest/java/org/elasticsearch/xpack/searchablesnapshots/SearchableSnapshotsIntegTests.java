/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStats;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.IndexStats;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.RestoreInProgress;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.AllocateUnassignedDecision;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.core.searchablesnapshots.SearchableSnapshotShardStats;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsResponse;

import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING;
import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.MAX_RESTORE_BYTES_PER_SEC;
import static org.elasticsearch.repositories.blobstore.BlobStoreRepository.READONLY_SETTING_KEY;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_RECOVERY_STATE_FACTORY_KEY;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class SearchableSnapshotsIntegTests extends BaseSearchableSnapshotsIntegTestCase {

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/95987")
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
        final Settings.Builder originalIndexSettings = Settings.builder().put(INDEX_SOFT_DELETES_SETTING.getKey(), true);
        if (randomBoolean()) {
            originalIndexSettings.put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom("false", "true", "checksum"));
        }
        assertAcked(prepareCreate(indexName, originalIndexSettings));
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
            .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), cacheEnabled);
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
                ByteSizeValue.ofBytes(randomLongBetween(10, 100_000))
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
            expectedDataTiersPreference = String.join(
                ",",
                randomSubsetOf(
                    DataTier.ALL_DATA_TIERS.stream().filter(tier -> tier.equals(DataTier.DATA_FROZEN) == false).collect(Collectors.toSet())
                )
            );
            indexSettingsBuilder.put(DataTier.TIER_PREFERENCE, expectedDataTiersPreference);
        } else {
            expectedDataTiersPreference = MountSearchableSnapshotRequest.Storage.FULL_COPY.defaultDataTiersPreference();
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
        assertThat(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings), equalTo(SEARCHABLE_SNAPSHOT_STORE_TYPE));
        assertThat(IndexModule.INDEX_RECOVERY_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_RECOVERY_STATE_FACTORY_KEY));
        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.exists(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.exists(settings));
        assertThat(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).toString(), equalTo("false"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), equalTo(expectedReplicas));
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(settings), equalTo(expectedDataTiersPreference));
        assertThat(IndexSettings.INDEX_CHECK_ON_STARTUP.get(settings), equalTo("false"));

        checkSoftDeletesNotEagerlyLoaded(restoredIndexName);
        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, preWarmEnabled);
        assertSearchableSnapshotStats(restoredIndexName, cacheEnabled, nonCachedExtensions);

        ensureGreen(restoredIndexName);
        assertBusy(() -> assertShardFolders(restoredIndexName, true), 30, TimeUnit.SECONDS);

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
            client().admin().cluster().prepareState().get().getState().nodes().getDataNodes().values()
        );

        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(
                    IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey(),
                    dataNode.getName()
                ),
            restoredIndexName
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

        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey()),
            restoredIndexName
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

        Settings.Builder indexSettingsBuilder = Settings.builder().put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true);
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

        Settings.Builder indexSettings = indexSettingsNoReplicas(between(1, 3)).put(INDEX_SOFT_DELETES_SETTING.getKey(), true);

        assertAcked(prepareCreate(indexName, indexSettings));

        final int nbDocs = between(10, 50);
        indexRandom(
            true,
            false,
            IntStream.range(0, nbDocs)
                .mapToObj(i -> client().prepareIndex(indexName).setSource("foo", randomAlphaOfLength(1048)))
                .collect(Collectors.toList())
        );
        refresh(indexName);
        forceMerge();

        final Settings.Builder repositorySettings = Settings.builder().put("location", randomRepoPath());
        final boolean useRateLimits = randomBoolean();
        if (useRateLimits) {
            // we compute the min across all the max shard (minimax) sizes by node in order to
            // trigger the rate limiter in all nodes. We could just use the min shard size
            // but that would make this test too slow.
            // Setting rate limit at a 90% of minimax value to increase probability to engage rate limiting mechanism
            // which is defined in RateLimitingInputStream (see details in https://github.com/elastic/elasticsearch/pull/96444)
            long minimax = getMaxShardSizeByNodeInBytes(indexName).values().stream().min(Long::compareTo).get();
            long rateLimitInBytes = (long) (0.9 * minimax);
            repositorySettings.put(MAX_RESTORE_BYTES_PER_SEC.getKey(), ByteSizeValue.ofBytes(rateLimitInBytes));
        } else {
            repositorySettings.put(MAX_RESTORE_BYTES_PER_SEC.getKey(), ByteSizeValue.ZERO);
            // Disable rate limiter which is defined in RecoverySettings
            // BlobStoreRepository#maybeRateLimitRestores uses two rate limiters and a single callback listener
            // which is asserted in this test. Both rate limiter should be switched off
            updateClusterSettings(Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), ByteSizeValue.ZERO));
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
            Settings.EMPTY,
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

        if (useRateLimits == false) {
            // compensate setting change from above
            updateClusterSettings(Settings.builder().putNull(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey()));
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
                .put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true);
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
        assertThat(snapshotTwoStatus.getStats().getTotalFileCount(), equalTo(0));
        assertThat(snapshotTwoStatus.getStats().getIncrementalFileCount(), equalTo(0));
        assertThat(snapshotTwoStatus.getStats().getProcessedFileCount(), equalTo(0));
    }

    public void testSnapshotMountedIndexWithTimestampsRecordsTimestampRangeInIndexMetadata() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final int numShards = between(1, 3);

        boolean indexed = randomBoolean();
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
                        .field("index", indexed)
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

        if (indexed) {
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
        } else {
            assertThat(timestampRange, sameInstance(IndexLongFieldRange.UNKNOWN));
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
                assertThat(stats.getIncrementalFileCount(), equalTo(0));
                assertThat(stats.getProcessedFileCount(), equalTo(0));
                assertThat(stats.getTotalFileCount(), equalTo(0));
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

        internalCluster().fullRestart();
        ensureGreen(restoredIndexName);
        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);

        final IllegalArgumentException remountException = expectThrows(IllegalArgumentException.class, () -> {
            try {
                mountSnapshot(
                    restoreRepositoryName,
                    snapshotTwo.getName(),
                    restoredIndexName,
                    randomAlphaOfLength(10).toLowerCase(Locale.ROOT),
                    Settings.EMPTY
                );
            } catch (Exception e) {
                final Throwable cause = ExceptionsHelper.unwrap(e, IllegalArgumentException.class);
                throw cause == null ? e : cause;
            }
        });
        assertThat(
            remountException.getMessage(),
            allOf(
                containsString("is a snapshot of a searchable snapshot index backed by index"),
                containsString(repositoryName),
                containsString(snapshotOne.getName()),
                containsString(indexName),
                containsString(restoreRepositoryName),
                containsString(snapshotTwo.getName()),
                containsString(restoredIndexName),
                containsString("cannot be mounted; did you mean to restore it instead?")
            )
        );
    }

    public void testSnapshotOfSearchableSnapshotCanBeRestoredBeforeRepositoryRegistered() throws Exception {
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

        // Take snapshot containing the actual data to one repository
        final String dataRepoName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createRepository(dataRepoName, "fs");

        final SnapshotId dataSnapshot = createSnapshot(dataRepoName, "data-snapshot", List.of(indexName)).snapshotId();
        assertAcked(client().admin().indices().prepareDelete(indexName));

        final String restoredIndexName = randomValueOtherThan(indexName, () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
        mountSnapshot(dataRepoName, dataSnapshot.getName(), indexName, restoredIndexName, Settings.EMPTY);
        ensureGreen(restoredIndexName);

        if (randomBoolean()) {
            logger.info("--> closing index before snapshot");
            assertAcked(client().admin().indices().prepareClose(restoredIndexName));
        }

        // Back up the cluster to a different repo
        final String backupRepoName = randomValueOtherThan(dataRepoName, () -> randomAlphaOfLength(10).toLowerCase(Locale.ROOT));
        createRepository(backupRepoName, "fs");
        final SnapshotId backupSnapshot = createSnapshot(backupRepoName, "backup-snapshot", List.of(restoredIndexName)).snapshotId();

        // Clear out data & the repo that contains it
        final RepositoryMetadata dataRepoMetadata = client().admin()
            .cluster()
            .prepareGetRepositories(dataRepoName)
            .get()
            .repositories()
            .get(0);
        assertAcked(client().admin().indices().prepareDelete(restoredIndexName));
        assertAcked(client().admin().cluster().prepareDeleteRepository(dataRepoName));

        // Restore the backup snapshot
        assertThat(
            client().admin()
                .cluster()
                .prepareRestoreSnapshot(backupRepoName, backupSnapshot.getName())
                .setIndices(restoredIndexName)
                .get()
                .status(),
            equalTo(RestStatus.ACCEPTED)
        );

        assertBusy(() -> {
            final ClusterAllocationExplanation clusterAllocationExplanation = client().admin()
                .cluster()
                .prepareAllocationExplain()
                .setIndex(restoredIndexName)
                .setShard(0)
                .setPrimary(true)
                .get()
                .getExplanation();

            final String description = Strings.toString(clusterAllocationExplanation);
            final AllocateUnassignedDecision allocateDecision = clusterAllocationExplanation.getShardAllocationDecision()
                .getAllocateDecision();
            assertTrue(description, allocateDecision.isDecisionTaken());
            assertThat(description, allocateDecision.getAllocationDecision(), equalTo(AllocationDecision.NO));
            for (NodeAllocationResult nodeAllocationResult : allocateDecision.getNodeDecisions()) {
                for (Decision decision : nodeAllocationResult.getCanAllocateDecision().getDecisions()) {
                    final String explanation = decision.getExplanation();
                    if (explanation.contains("this index is backed by a searchable snapshot")
                        && explanation.contains("no such repository is registered")
                        && explanation.contains("the required repository was originally named [" + dataRepoName + "]")) {
                        return;
                    }
                }
            }

            fail(description);
        });

        assertBusy(() -> {
            final RestoreInProgress restoreInProgress = client().admin()
                .cluster()
                .prepareState()
                .clear()
                .setCustoms(true)
                .get()
                .getState()
                .custom(RestoreInProgress.TYPE, RestoreInProgress.EMPTY);
            assertTrue(Strings.toString(restoreInProgress, true, true), restoreInProgress.isEmpty());
        });

        // Re-register the repository containing the actual data & verify that the shards are now allocated
        final String newRepositoryName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        final Settings.Builder settings = Settings.builder().put(dataRepoMetadata.settings());
        if (randomBoolean()) {
            settings.put(READONLY_SETTING_KEY, "true");
        }
        assertAcked(clusterAdmin().preparePutRepository(newRepositoryName).setType("fs").setSettings(settings));

        ensureGreen(restoredIndexName);
        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
    }

    public void testCheckOnStartupCanBeOverridden() throws Exception {
        final String suffix = getTestName().toLowerCase(Locale.ROOT);

        final String index = "index_" + suffix;
        final Settings.Builder indexSettings = Settings.builder();
        indexSettings.put(INDEX_SOFT_DELETES_SETTING.getKey(), true);

        final String checkOnStartup = randomFrom("false", "true", "checksum", null);
        if (checkOnStartup != null) {
            indexSettings.put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), checkOnStartup);
        }
        createAndPopulateIndex(index, indexSettings);

        final String repository = "repository_" + suffix;
        createRepository(repository, "fs");

        final String snapshot = "snapshot_" + suffix;
        createFullSnapshot(repository, snapshot);
        assertAcked(client().admin().indices().prepareDelete(index));

        {
            final String mountedIndex = mountSnapshot(repository, snapshot, index, Settings.EMPTY);
            assertThat(
                client().admin()
                    .indices()
                    .prepareGetSettings(mountedIndex)
                    .get()
                    .getIndexToSettings()
                    .get(mountedIndex)
                    .get(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey()),
                equalTo("false")
            );
            assertAcked(client().admin().indices().prepareDelete(mountedIndex));
        }
        {
            final String overridingCheckOnStartup = randomValueOtherThan(checkOnStartup, () -> randomFrom("false", "true", "checksum"));
            final String mountedIndex = mountSnapshot(
                repository,
                snapshot,
                index,
                Settings.builder().put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), overridingCheckOnStartup).build()
            );
            assertThat(
                client().admin()
                    .indices()
                    .prepareGetSettings(mountedIndex)
                    .get()
                    .getIndexToSettings()
                    .get(mountedIndex)
                    .get(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey()),
                equalTo(overridingCheckOnStartup)
            );
            assertAcked(client().admin().indices().prepareDelete(mountedIndex));
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
            .mapToLong(stat -> stat.getTotalSize().getBytes())
            .sum();
        assertThat("Expecting stats to exist for at least one Lucene file", totalSize, greaterThan(0L));

        for (SearchableSnapshotShardStats stats : statsResponse.getStats()) {
            final ShardRouting shardRouting = stats.getShardRouting();
            assertThat(stats.getShardRouting().getIndexName(), equalTo(indexName));
            if (shardRouting.started()) {
                for (SearchableSnapshotShardStats.CacheIndexInputStats indexInputStats : stats.getStats()) {
                    final String fileExt = indexInputStats.getFileExt();
                    assertThat(
                        "Unexpected open count for " + fileExt + " of shard " + shardRouting,
                        indexInputStats.getOpenCount(),
                        greaterThan(0L)
                    );
                    assertThat(
                        "Unexpected close count for " + fileExt + " of shard " + shardRouting,
                        indexInputStats.getCloseCount(),
                        lessThanOrEqualTo(indexInputStats.getOpenCount())
                    );
                    assertThat(
                        "Unexpected file length for " + fileExt + " of shard " + shardRouting,
                        indexInputStats.getTotalSize().getBytes(),
                        greaterThan(0L)
                    );
                    assertThat(
                        "Unexpected min. file length for " + fileExt + " of shard " + shardRouting,
                        indexInputStats.getMinSize().getBytes(),
                        greaterThan(0L)
                    );
                    assertThat(
                        "Unexpected max. file length for " + fileExt + " of shard " + shardRouting,
                        indexInputStats.getMaxSize().getBytes(),
                        greaterThan(0L)
                    );
                    assertThat(
                        "Unexpected average file length for " + fileExt + " of shard " + shardRouting,
                        indexInputStats.getAverageSize().getBytes(),
                        greaterThan(0L)
                    );
                    assertThat(
                        "Expected at least one Lucene read for " + fileExt + " of shard " + shardRouting,
                        indexInputStats.getLuceneBytesRead().getCount(),
                        greaterThan(0L)
                    );

                    if (cacheEnabled == false || nonCachedExtensions.contains(fileExt)) {
                        assertThat(
                            "Expected at least 1 optimized or direct read for " + fileExt + " of shard " + shardRouting,
                            max(indexInputStats.getOptimizedBytesRead().getCount(), indexInputStats.getDirectBytesRead().getCount()),
                            greaterThan(0L)
                        );
                        assertThat(
                            "Expected no cache read or write for " + fileExt + " of shard " + shardRouting,
                            max(indexInputStats.getCachedBytesRead().getCount(), indexInputStats.getCachedBytesWritten().getCount()),
                            equalTo(0L)
                        );
                    } else {
                        assertThat(
                            "Expected at least 1 cache read or write for " + fileExt + " of shard " + shardRouting,
                            max(
                                indexInputStats.getCachedBytesRead().getCount(),
                                indexInputStats.getCachedBytesWritten().getCount(),
                                indexInputStats.getIndexCacheBytesRead().getCount()
                            ),
                            greaterThan(0L)
                        );
                        assertThat(
                            "Expected no optimized read for " + fileExt + " of shard " + shardRouting,
                            indexInputStats.getOptimizedBytesRead().getCount(),
                            equalTo(0L)
                        );
                        assertThat(
                            "Expected no direct read for " + fileExt + " of shard " + shardRouting,
                            indexInputStats.getDirectBytesRead().getCount(),
                            equalTo(0L)
                        );
                    }
                }
            }
        }
    }

    private static long max(long... values) {
        return Arrays.stream(values).max().orElseThrow(() -> new AssertionError("no values"));
    }
}
