/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotIndexShardStatus;
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.cluster.routing.allocation.decider.DiskThresholdDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.cache.request.RequestCacheStats;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreStats;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesRequestCache;
import org.elasticsearch.indices.IndicesRequestCacheUtils;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.snapshots.SearchableSnapshotsSettings;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotAction;
import org.elasticsearch.xpack.core.searchablesnapshots.MountSearchableSnapshotRequest;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsAction;
import org.elasticsearch.xpack.searchablesnapshots.action.SearchableSnapshotsStatsRequest;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.IndexSettings.INDEX_SOFT_DELETES_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.dateHistogram;
import static org.elasticsearch.snapshots.SearchableSnapshotsSettings.SEARCHABLE_SNAPSHOT_STORE_TYPE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshots.SNAPSHOT_RECOVERY_STATE_FACTORY_KEY;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.sameInstance;

public class FrozenSearchableSnapshotsIntegTests extends BaseFrozenSearchableSnapshotsIntegTestCase {

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

        logger.info("--> restoring partial index [{}] with cache enabled", restoredIndexName);

        Settings.Builder indexSettingsBuilder = Settings.builder().put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true);
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
        final String indexCheckOnStartup;
        if (randomBoolean()) {
            indexCheckOnStartup = randomFrom("false", "true", "checksum");
            indexSettingsBuilder.put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), indexCheckOnStartup);
        } else {
            indexCheckOnStartup = "false";
        }
        final String expectedDataTiersPreference;
        expectedDataTiersPreference = MountSearchableSnapshotRequest.Storage.SHARED_CACHE.defaultDataTiersPreference();

        indexSettingsBuilder.put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.ZERO);
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
                    StoreStats store = shardStats.getStats().getStore();
                    assertThat(shardStats.getShardRouting().toString(), store.getReservedSize().getBytes(), equalTo(0L));
                    assertThat(shardStats.getShardRouting().toString(), store.getSize().getBytes(), equalTo(0L));
                }
                if (indicesStatsResponse.getShards().length > 0) {
                    assertThat(indicesStatsResponse.getTotal().getStore().getReservedSize().getBytes(), equalTo(0L));
                    assertThat(indicesStatsResponse.getTotal().getStore().getSize().getBytes(), equalTo(0L));
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

        final Map<Integer, SnapshotIndexShardStatus> snapshotShards = clusterAdmin().prepareSnapshotStatus(fsRepoName)
            .setSnapshots(snapshotInfo.snapshotId().getName())
            .get()
            .getSnapshots()
            .get(0)
            .getIndices()
            .get(indexName)
            .getShards();

        ensureGreen(restoredIndexName);

        final IndicesStatsResponse indicesStatsResponse = client().admin()
            .indices()
            .prepareStats(restoredIndexName)
            .clear()
            .setStore(true)
            .get();
        assertThat(indicesStatsResponse.getShards().length, greaterThan(0));
        long totalExpectedSize = 0;
        for (ShardStats shardStats : indicesStatsResponse.getShards()) {
            StoreStats store = shardStats.getStats().getStore();

            final ShardRouting shardRouting = shardStats.getShardRouting();
            assertThat(shardRouting.toString(), store.getReservedSize().getBytes(), equalTo(0L));
            assertThat(shardRouting.toString(), store.getSize().getBytes(), equalTo(0L));

            // the original shard size from the snapshot
            final long originalSize = snapshotShards.get(shardRouting.getId()).getStats().getTotalSize();
            totalExpectedSize += originalSize;

            // an extra segments_N file is created for bootstrapping new history and associating translog. We can extract the size of this
            // extra file but we have to unwrap the in-memory directory first.
            final Directory unwrappedDir = FilterDirectory.unwrap(
                internalCluster().getInstance(IndicesService.class, getDiscoveryNodes().resolveNode(shardRouting.currentNodeId()).getName())
                    .indexServiceSafe(shardRouting.index())
                    .getShard(shardRouting.getId())
                    .store()
                    .directory()
            );
            assertThat(shardRouting.toString(), unwrappedDir, notNullValue());
            assertThat(shardRouting.toString(), unwrappedDir, instanceOf(ByteBuffersDirectory.class));

            final ByteBuffersDirectory inMemoryDir = (ByteBuffersDirectory) unwrappedDir;
            assertThat(inMemoryDir.listAll(), arrayWithSize(1));

            assertThat(shardRouting.toString(), store.getTotalDataSetSize().getBytes(), equalTo(originalSize));
        }

        final StoreStats store = indicesStatsResponse.getTotal().getStore();
        assertThat(store.getTotalDataSetSize().getBytes(), equalTo(totalExpectedSize));

        statsWatcherRunning.set(false);
        statsWatcher.join();

        final Settings settings = client().admin()
            .indices()
            .prepareGetSettings(restoredIndexName)
            .get()
            .getIndexToSettings()
            .get(restoredIndexName);
        assertThat(SearchableSnapshots.SNAPSHOT_SNAPSHOT_NAME_SETTING.get(settings), equalTo(snapshotName));
        assertThat(IndexModule.INDEX_STORE_TYPE_SETTING.get(settings), equalTo(SEARCHABLE_SNAPSHOT_STORE_TYPE));
        assertThat(IndexModule.INDEX_RECOVERY_TYPE_SETTING.get(settings), equalTo(SNAPSHOT_RECOVERY_STATE_FACTORY_KEY));
        assertTrue(IndexMetadata.INDEX_BLOCKS_WRITE_SETTING.get(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_SNAPSHOT_ID_SETTING.exists(settings));
        assertTrue(SearchableSnapshots.SNAPSHOT_INDEX_ID_SETTING.exists(settings));
        assertThat(IndexMetadata.INDEX_AUTO_EXPAND_REPLICAS_SETTING.get(settings).toString(), equalTo("false"));
        assertThat(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.get(settings), equalTo(expectedReplicas));
        assertThat(DataTier.TIER_PREFERENCE_SETTING.get(settings), equalTo(expectedDataTiersPreference));
        assertTrue(SearchableSnapshotsSettings.SNAPSHOT_PARTIAL_SETTING.get(settings));
        assertTrue(DiskThresholdDecider.SETTING_IGNORE_DISK_WATERMARKS.get(settings));
        assertThat(IndexSettings.INDEX_CHECK_ON_STARTUP.get(settings), equalTo(indexCheckOnStartup));

        checkSoftDeletesNotEagerlyLoaded(restoredIndexName);
        assertTotalHits(restoredIndexName, originalAllHits, originalBarHits);
        assertRecoveryStats(restoredIndexName, false);
        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, true, nonCachedExtensions);
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
        assertRecoveryStats(restoredIndexName, false);
        // TODO: fix
        // assertSearchableSnapshotStats(restoredIndexName, false, nonCachedExtensions);

        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .putNull(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getConcreteSettingForNamespace("_name").getKey()),
            restoredIndexName
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
                        .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
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

    public void testRequestCacheOnFrozen() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test-index")
                .setMapping("f", "type=date")
                .setSettings(indexSettings(1, 0).put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), true))
                .get()
        );
        indexRandom(
            true,
            client().prepareIndex("test-index").setSource("f", "2014-03-10T00:00:00.000Z"),
            client().prepareIndex("test-index").setSource("f", "2014-05-13T00:00:00.000Z")
        );
        ensureSearchable("test-index");

        createRepository("repo", "fs", Settings.builder().put("location", randomRepoPath()));

        createFullSnapshot("repo", "snap");

        assertAcked(client().admin().indices().prepareDelete("test-index"));

        logger.info("--> restoring index [{}]", "test-index");

        Settings.Builder indexSettingsBuilder = Settings.builder().put(SearchableSnapshots.SNAPSHOT_CACHE_ENABLED_SETTING.getKey(), true);
        final MountSearchableSnapshotRequest req = new MountSearchableSnapshotRequest(
            "test-index",
            "repo",
            "snap",
            "test-index",
            indexSettingsBuilder.build(),
            Strings.EMPTY_ARRAY,
            true,
            MountSearchableSnapshotRequest.Storage.SHARED_CACHE
        );

        final RestoreSnapshotResponse restoreSnapshotResponse = client().execute(MountSearchableSnapshotAction.INSTANCE, req).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(0));
        ensureSearchable("test-index");

        // use a fixed client for the searches, as clients randomize timeouts, which leads to different cache entries
        Client client = client();

        final SearchResponse r1 = client.prepareSearch("test-index")
            .setSize(0)
            .setSearchType(SearchType.QUERY_THEN_FETCH)
            .addAggregation(
                dateHistogram("histo").field("f").timeZone(ZoneId.of("+01:00")).minDocCount(0).calendarInterval(DateHistogramInterval.MONTH)
            )
            .get();
        assertSearchResponse(r1);

        assertRequestCacheState(client(), "test-index", 0, 1);

        // The cached is actually used
        assertThat(
            client().admin()
                .indices()
                .prepareStats("test-index")
                .setRequestCache(true)
                .get()
                .getTotal()
                .getRequestCache()
                .getMemorySizeInBytes(),
            greaterThan(0L)
        );

        for (int i = 0; i < 10; ++i) {
            final SearchResponse r2 = client.prepareSearch("test-index")
                .setSize(0)
                .setSearchType(SearchType.QUERY_THEN_FETCH)
                .addAggregation(
                    dateHistogram("histo").field("f")
                        .timeZone(ZoneId.of("+01:00"))
                        .minDocCount(0)
                        .calendarInterval(DateHistogramInterval.MONTH)
                )
                .get();
            assertSearchResponse(r2);
            assertRequestCacheState(client(), "test-index", i + 1, 1);
            Histogram h1 = r1.getAggregations().get("histo");
            Histogram h2 = r2.getAggregations().get("histo");
            final List<? extends Histogram.Bucket> buckets1 = h1.getBuckets();
            final List<? extends Histogram.Bucket> buckets2 = h2.getBuckets();
            assertEquals(buckets1.size(), buckets2.size());
            for (int j = 0; j < buckets1.size(); ++j) {
                final Histogram.Bucket b1 = buckets1.get(j);
                final Histogram.Bucket b2 = buckets2.get(j);
                assertEquals(b1.getKey(), b2.getKey());
                assertEquals(b1.getDocCount(), b2.getDocCount());
            }
        }

        // shut down shard and check that cache entries are actually removed
        client().admin().indices().prepareClose("test-index").get();
        ensureGreen("test-index");

        for (IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            IndicesRequestCache indicesRequestCache = IndicesRequestCacheUtils.getRequestCache(indicesService);
            IndicesRequestCacheUtils.cleanCache(indicesRequestCache);
            for (String key : IndicesRequestCacheUtils.cachedKeys(indicesRequestCache)) {
                assertThat(key, not(containsString("test-index")));
            }
        }
    }

    private static void assertRequestCacheState(Client client, String index, long expectedHits, long expectedMisses) {
        RequestCacheStats requestCacheStats = client.admin()
            .indices()
            .prepareStats(index)
            .setRequestCache(true)
            .get()
            .getTotal()
            .getRequestCache();
        // Check the hit count and miss count together so if they are not
        // correct we can see both values
        assertEquals(
            Arrays.asList(expectedHits, expectedMisses, 0L),
            Arrays.asList(requestCacheStats.getHitCount(), requestCacheStats.getMissCount(), requestCacheStats.getEvictions())
        );
    }

}
