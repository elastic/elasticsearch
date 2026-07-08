/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.shutdown.PutShutdownNodeAction;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.core.TimeValue.MINUS_ONE;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class BoostedDataEvictionIT extends AbstractStatelessPluginIntegTestCase {

    private static final String TIMESTAMP_MAPPING = """
        {
            "properties": {
                "@timestamp": {
                    "type":"date"
                },
                "hostname": {
                    "type":"keyword",
                    "time_series_dimension": true
                }
            }
        }
        """;

    // non-boosted doc-value reads (sort forces reading all values per segment) overflow it.
    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(4); // TODO randomisation
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofKb(256);
    private static final long BOOST_WINDOW_MILLIS = TimeValue.timeValueDays(7).millis();
    private static final long ONE_DAY_MILLIS = TimeValue.timeValueDays(1).millis();
    // we avoid current timestamp to ease potential test failures reproduction
    private static final long BOOST_WINDOW_END = Instant.parse("2026-01-01T00:00:00Z").toEpochMilli();
    private final String BOOSTED_IDX = randomIdentifier();
    private final String NON_BOOSTED_IDX = randomIdentifier();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(SpyCacheStatelessPlugin.class);
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(ShutdownPlugin.class);
        return Collections.unmodifiableList(plugins);
    }

    @Before
    public void clearCacheServiceInvocations() {
        if (internalCluster().size() > 0) {
            for (String nodeName : internalCluster().getNodeNames()) {
                internalCluster().getInstance(PluginsService.class, nodeName)
                    .filterPlugins(SpyCacheStatelessPlugin.class)
                    .findFirst()
                    .ifPresent(plugin -> Mockito.clearInvocations(plugin.getStatelessSharedBlobCacheService()));
            }
        }
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // Disable all background warmers so nothing populates the cache between test steps
        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false)
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
            .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), false)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1));

    }

    public void testNonBoostedSearchesEvictBoostedData() {
        final Settings cacheSettings = Settings.builder()
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE)
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE)
            .build();
        final String masterAndIndexNodeName = startMasterAndIndexNode(cacheSettings);
        startSearchNode(cacheSettings);
        final Settings idxSettings = ESTestCase.indexSettings(1, 1)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false")
            .build();

        assertAcked(prepareCreate(BOOSTED_IDX).setSettings(idxSettings).setMapping(TIMESTAMP_MAPPING));
        assertAcked(prepareCreate(NON_BOOSTED_IDX).setSettings(idxSettings).setMapping(TIMESTAMP_MAPPING));
        ensureGreen(BOOSTED_IDX, NON_BOOSTED_IDX);

        // Fixed reference point + seeded random offset so failures are reproducible from the test seed,
        // and so the boost-window bounds can be asserted against compound-commit metadata below.
        final long boostWindowEndInMillis = BOOST_WINDOW_END + randomLongBetween(0, TimeValue.timeValueDays(365).millis());
        final long boostWindowStartInMillis = boostWindowEndInMillis - BOOST_WINDOW_MILLIS + ONE_DAY_MILLIS;
        final long nonBoostWindowEndInMillis = boostWindowEndInMillis - BOOST_WINDOW_MILLIS - 2 * ONE_DAY_MILLIS;
        final long nonBoostWindowStartInMillis = nonBoostWindowEndInMillis - 30L * ONE_DAY_MILLIS;
        // Non-boosted index is sized to exceed the cache: many segments ensure non-boosted searches
        // span more cache regions than the cache holds, so LFU eviction must displace every boosted region.
        indexDocuments(masterAndIndexNodeName, 10, NON_BOOSTED_IDX, 10_000, nonBoostWindowStartInMillis, nonBoostWindowEndInMillis);
        indexDocuments(masterAndIndexNodeName, 10, BOOSTED_IDX, 1_000, boostWindowStartInMillis, boostWindowEndInMillis);

        final StatelessSharedBlobCacheService cacheService = getCacheService();
        logger.debug(
            "cache regions after ingesting docs: boosted={}, non-boosted={}",
            cacheRegionsForIndex(cacheService, BOOSTED_IDX),
            cacheRegionsForIndex(cacheService, NON_BOOSTED_IDX)
        );

        // Step 1 — populate the cache with boosted data via a single on-demand search.
        // All boosted regions start at LFU frequency 1 (written once, not yet promoted).
        searchBoostedData(BOOSTED_IDX);

        final SharedBlobCacheService.Stats statsAfterBoostSearch = cacheService.getStats();
        logger.debug(
            "boosted cache regions after searching boosted docs: boosted={}, non-boosted={}",
            cacheRegionsForIndex(cacheService, BOOSTED_IDX),
            cacheRegionsForIndex(cacheService, NON_BOOSTED_IDX)
        );

        assertThat("boosted data should have been loaded into the cache", statsAfterBoostSearch.writeBytes(), greaterThan(0L));
        assertThat("boosted cache regions should be resident", cacheRegionsForIndex(cacheService, BOOSTED_IDX), greaterThan(0L));

        // Step 2 — drive non-boosted searches. Sorting by @timestamp forces reading all doc-value
        // data per segment, generating enough blob-cache reads to overflow the small cache.
        // Both boosted and non-boosted regions compete at the same LFU frequency (1); the older
        // boosted regions are evicted first under the LFU clock.
        searchNonBoostedData(NON_BOOSTED_IDX);

        logger.debug(
            "boosted cache regions after searching non-boosted docs: boosted={}, non-boosted={}",
            cacheRegionsForIndex(cacheService, BOOSTED_IDX),
            cacheRegionsForIndex(cacheService, NON_BOOSTED_IDX)
        );

        // TODO this is the current behavior we want to get rid off, as a part of caching infrastructure improvements
        assertThat(
            "boosted regions must have been fully evicted by non-boosted searches",
            cacheRegionsForIndex(cacheService, BOOSTED_IDX),
            equalTo(0L)
        );
    }

    public void testCacheDemotedToFrequencyZeroAfterSearchShardRelocation() throws Exception {
        final Settings cacheSettings = cacheBoostPreferenceTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNodeA = startSearchNode(cacheSettings);
        final String searchNodeB = startSearchNode(cacheSettings);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", searchNodeB).build());
        ensureGreen(indexName);

        indexAndSearch(indexName, randomIntBetween(10, 100));

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final StatelessSharedBlobCacheService cacheServiceA = getCacheService(searchNodeA);
        assertNonZeroFrequencies(cacheServiceA, shardId);

        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", searchNodeA), indexName);
        internalCluster().awaitNodesInclude(indexName, nodes -> nodes.contains(searchNodeA) == false && nodes.contains(searchNodeB));

        assertBusy(() -> verify(cacheServiceA, atLeastOnce()).demoteAllAsync(ArgumentMatchers.any(), ArgumentMatchers.any()));
        assertDemotedToFrequencyZero(cacheServiceA, shardId);
        verify(cacheServiceA, never()).forceEvictAsync(ArgumentMatchers.any());
    }

    public void testForceEvictAsyncOnIndexDelete() throws Exception {
        final Settings cacheSettings = cacheBoostPreferenceTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNode = startSearchNode(cacheSettings);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        indexAndSearch(indexName, randomIntBetween(10, 100));

        final StatelessSharedBlobCacheService cacheService = getCacheService(searchNode);
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        assertThat(cacheService.countCachedRegions(shardPredicate(shardId)), greaterThan(0L));

        assertAcked(indicesAdmin().prepareDelete(indexName));

        assertBusy(() -> assertThat(cacheService.countCachedRegions(shardPredicate(shardId)), equalTo(0L)));
    }

    public void testCacheNotDemotedWhenNodeIsShuttingDown() throws Exception {
        final Settings cacheSettings = cacheBoostPreferenceTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNodeA = startSearchNode(cacheSettings);
        final String searchNodeB = startSearchNode(cacheSettings);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        indexAndSearch(indexName, randomIntBetween(10, 100));

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final Set<String> nodesWithShard = internalCluster().nodesInclude(indexName);
        final String shutdownNode = nodesWithShard.stream()
            .filter(n -> n.equals(searchNodeA) || n.equals(searchNodeB))
            .findFirst()
            .orElseThrow(() -> new AssertionError("no search node has a shard for [" + indexName + "]"));

        final StatelessSharedBlobCacheService cacheService = getCacheService(shutdownNode);
        assertNonZeroFrequencies(cacheService, shardId);

        final Map<Integer, Integer> freqsBeforeShutdown = SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(
            cacheService,
            shardPredicate(shardId)
        );

        assertAcked(
            client().execute(
                PutShutdownNodeAction.INSTANCE,
                new PutShutdownNodeAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    getNodeId(shutdownNode),
                    SingleNodeShutdownMetadata.Type.SIGTERM,
                    "test shutdown to verify cache demotion is skipped",
                    null,
                    null,
                    TimeValue.timeValueMinutes(5)
                )
            )
        );

        internalCluster().awaitNodeVacated(indexName, shutdownNode);

        long regionCount = cacheService.countCachedRegions(shardPredicate(shardId));
        assertThat(regionCount, greaterThan(0L));
        assertThat(
            "cache regions should not be demoted when node is shutting down",
            SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheService, shardPredicate(shardId)),
            equalTo(freqsBeforeShutdown)
        );
        verify(cacheService, never()).demoteAllAsync(ArgumentMatchers.any(), ArgumentMatchers.any());
        verify(cacheService, never()).forceEvictAsync(ArgumentMatchers.any());
    }

    private static Settings cacheBoostPreferenceTestSettings() {
        return Settings.builder()
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(32))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(256))
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .build();
    }

    private void indexAndSearch(String indexName, int numDocs) {
        indexDocs(indexName, numDocs);
        flushAndRefresh(indexName);

        final int searches = randomIntBetween(10, 20);
        for (int i = 0; i < searches; i++) {
            assertResponse(
                prepareSearch(indexName).setSize(numDocs),
                response -> assertEquals(numDocs, response.getHits().getHits().length)
            );
        }
    }

    private static StatelessSharedBlobCacheService getCacheService(String nodeName) {
        final var statelessPlugin = internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(SpyCacheStatelessPlugin.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError("stateless plugin not found on node [" + nodeName + "]"));
        return statelessPlugin.getStatelessSharedBlobCacheService();
    }

    private static Predicate<FileCacheKey> shardPredicate(ShardId shardId) {
        return key -> key.shardId().equals(shardId);
    }

    private static void assertNonZeroFrequencies(StatelessSharedBlobCacheService cacheService, ShardId shardId) throws Exception {
        assertBusy(() -> {
            long regionCount = cacheService.countCachedRegions(shardPredicate(shardId));
            assertThat(regionCount, greaterThan(0L));
            int maxFreq = SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheService, shardPredicate(shardId))
                .keySet()
                .stream()
                .max(Integer::compareTo)
                .orElse(0);
            assertThat(maxFreq, greaterThan(0));
        });
    }

    private static void assertDemotedToFrequencyZero(StatelessSharedBlobCacheService cacheService, ShardId shardId) throws Exception {
        assertBusy(() -> {
            long regionCount = cacheService.countCachedRegions(shardPredicate(shardId));
            assertThat(regionCount, greaterThan(0L));
            assertThat(
                SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheService, shardPredicate(shardId)),
                equalTo(Map.of(0, (int) regionCount))
            );
        });
    }

    private long cacheRegionsForIndex(StatelessSharedBlobCacheService cacheService, String indexName) {
        return cacheService.countCachedRegions(key -> key.shardId().getIndexName().equals(indexName));
    }

    private static void searchNonBoostedData(String nonBoostedIdx) {
        for (int i = 0; i < randomIntBetween(2, 4); i++) {
            assertResponse(
                prepareSearch(nonBoostedIdx).setSize(5_000).addSort(DataStream.TIMESTAMP_FIELD_NAME, ASC),
                ElasticsearchAssertions::assertNoFailures
            );
        }
    }

    private static void searchBoostedData(String boostedIdx) {
        for (int i = 0; i < randomIntBetween(2, 4); i++) {
            assertResponse(prepareSearch(boostedIdx).setSize(1_000), ElasticsearchAssertions::assertNoFailures);
        }
    }

    private void indexDocuments(String nodeName, int numBatches, String indexName, int numDocs, long startInMillis, long endInMillis) {
        range(0, numBatches).forEach(i -> indexDocumentsWithTimestamp(indexName, numDocs, startInMillis, endInMillis));
        // Verify the @timestamp values we generated actually propagate down to the compound commit metadata
        // (StatelessCompoundCommit#timestampFieldValueRange) — that range is what a future boost feature on
        // the search node will consult, so this asserts the test's "boost window" label is real, not just doc source.
        assertTimestampRangePropagatedToCommits(nodeName, indexName, startInMillis, endInMillis);
        flush(indexName);
    }

    private void assertTimestampRangePropagatedToCommits(String nodeName, String indexName, long minBound, long maxBound) {
        final var shardId = findIndexShard(indexName).shardId();
        final var commitService = internalCluster().getInstance(StatelessCommitService.class, nodeName);
        final var virtualBcc = commitService.getCurrentVirtualBcc(shardId);
        assertThat("expected a pending virtual BCC for shard " + shardId, virtualBcc, notNullValue());
        final var pendingCommits = virtualBcc.getPendingCompoundCommits();
        assertThat("expected at least one pending compound commit", pendingCommits.size(), greaterThan(0));
        for (final var pendingCC : pendingCommits) {
            final var range = pendingCC.getStatelessCompoundCommit().timestampFieldValueRange();
            assertThat("compound commit must carry a @timestamp range", range, notNullValue());
            assertThat(range.minMillis(), greaterThanOrEqualTo(minBound));
            assertThat(range.maxMillis(), lessThanOrEqualTo(maxBound));
        }
    }

    private void indexDocumentsWithTimestamp(String indexName, int numDocs, long minTimestamp, long maxTimestamp) {
        var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        range(0, numDocs).mapToObj(
            i -> client().prepareIndex(indexName)
                .setSource(
                    DataStream.TIMESTAMP_FIELD_NAME,
                    randomLongBetween(minTimestamp, maxTimestamp),
                    "hostname",
                    "host-" + randomIntBetween(1, 5)
                )
        ).forEach(bulk::add);
        assertNoFailures(bulk.get());
    }

    private StatelessSharedBlobCacheService getCacheService() {
        final IndexShard boostedShard = findSearchShard(BOOSTED_IDX);
        return BlobStoreCacheDirectoryTestUtils.getCacheService(SearchDirectory.unwrapDirectory(boostedShard.store().directory()));
    }

    /**
     * Wraps the shared blob cache in a Mockito spy so tests can verify eviction and demotion calls without
     * replacing the real cache implementation.
     */
    public static class SpyCacheStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public SpyCacheStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected StatelessSharedBlobCacheService createSharedBlobCacheService(
            NodeEnvironment nodeEnvironment,
            Settings settings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics,
            ClusterService clusterService,
            IndicesService indicesService
        ) {
            final StatelessSharedBlobCacheService spy = Mockito.spy(
                super.createSharedBlobCacheService(nodeEnvironment, settings, threadPool, blobCacheMetrics, clusterService, indicesService)
            );
            return spy;
        }
    }
}
