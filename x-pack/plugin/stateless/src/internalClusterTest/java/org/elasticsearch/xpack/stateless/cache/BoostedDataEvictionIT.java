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
import org.elasticsearch.common.time.TimeProviderUtils;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
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
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;
import org.junit.Before;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_DECAY_INTERVAL_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.core.TimeValue.MINUS_ONE;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.cache.PinnedWindowEvictionPolicy.PINNED_WINDOW_DURATION_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
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
        final var searchNode = startSearchNode(cacheSettings);
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

        final StatelessSharedBlobCacheService cacheService = getCacheService(searchNode);
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

    public void testPinnedWindowEvictionPolicyProtectsPinnedData() {
        final Settings cacheSettings = Settings.builder()
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE)
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), false)
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(
                StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_EVICTION_POLICY_SEARCH_SETTING.getKey(),
                StatelessCacheEvictionPolicyType.PINNED_WINDOW
            )
            .put(PINNED_WINDOW_DURATION_SETTING.getKey(), TimeValue.timeValueHours(12))
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICTION_POLICY_DEGRADATION_THRESHOLD_SETTING.getKey(), "100%")
            .build();
        final var masterAndIndexNodeName = startMasterAndIndexNode(cacheSettings);
        final var searchNode = startSearchNode(cacheSettings);
        final Settings idxSettings = ESTestCase.indexSettings(1, 1)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE)
            .put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES)
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "hostname")
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false")
            .build();

        final var pinnedIdx = randomIdentifier("pinned-");
        final var unpinnedIdx = randomIdentifier("unpinned-");
        assertAcked(prepareCreate(pinnedIdx).setSettings(idxSettings).setMapping(TIMESTAMP_MAPPING));
        assertAcked(prepareCreate(unpinnedIdx).setSettings(idxSettings).setMapping(TIMESTAMP_MAPPING));
        ensureGreen(pinnedIdx, unpinnedIdx);

        // Stub absoluteTimeInMillis() on the mock timeProvider so that PinnedWindowEvictionPolicy sees a fixed "now",
        // making data timestamps independent of actual system time and fully reproducible.
        final var spyCachePlugin = findPlugin(searchNode, SpyCacheStatelessPlugin.class);
        spyCachePlugin.currentTimestamp.set(BOOST_WINDOW_END);
        // 12-hour pinned window: pinned data (< 6h old) is protected; unpinned data (> 14h old) is evictable. We use
        // these timestamps to leave some extra margins for both pinned and unpinned data so that they are not too close
        // to the time window boundaries which might lead to flaky tests.
        final long pinnedDataEndMillis = BOOST_WINDOW_END;
        final long pinnedDataStartMillis = BOOST_WINDOW_END - TimeValue.timeValueHours(6).millis();
        final long unpinnedDataEndMillis = BOOST_WINDOW_END - TimeValue.timeValueHours(14).millis();
        final long unpinnedDataStartMillis = BOOST_WINDOW_END - TimeValue.timeValueHours(38).millis();
        // Unpinned index is sized to exceed the cache, same as testNonBoostedSearchesEvictBoostedData.
        indexDocuments(masterAndIndexNodeName, 10, unpinnedIdx, 10_000, unpinnedDataStartMillis, unpinnedDataEndMillis);
        indexDocuments(masterAndIndexNodeName, 10, pinnedIdx, 1_000, pinnedDataStartMillis, pinnedDataEndMillis);

        final StatelessSharedBlobCacheService cacheService = getCacheService(searchNode);
        logger.info(
            "cache regions after ingesting docs: pinned={}, unpinned={}",
            cacheRegionsForIndex(cacheService, pinnedIdx),
            cacheRegionsForIndex(cacheService, unpinnedIdx)
        );

        // Step 1 — populate the cache with pinned data.
        searchData(pinnedIdx, 1_000, false);

        // Regions with MINIMAL_CACHE_TIMESTAMP (0) from metadata reads are not protected by the policy and may be evicted
        // when they have no active readers.
        final Predicate<FileCacheKey> isPinnedIdx = key -> key.shardId().getIndexName().equals(pinnedIdx);
        final long pinnedRegionsAfterPinnedSearch = cacheRegionsForIndex(cacheService, pinnedIdx) - countZeroTimestampRegions(
            cacheService,
            isPinnedIdx
        );
        logger.info(
            "cache regions after searching pinned data: pinned (positive-timestamp)={}, unpinned={}",
            pinnedRegionsAfterPinnedSearch,
            cacheRegionsForIndex(cacheService, unpinnedIdx)
        );
        assertThat("pinned data should have been loaded into the cache", pinnedRegionsAfterPinnedSearch, greaterThan(0L));

        // Step 2 — drive searches over unpinned data to overflow the cache.
        searchData(unpinnedIdx, 5_000, true);

        final long pinnedRegionsAfterUnpinnedSearch = cacheRegionsForIndex(cacheService, pinnedIdx) - countZeroTimestampRegions(
            cacheService,
            isPinnedIdx
        );

        // The unpinned index takes non-zero number of regions that are unprotected
        final long regionsForUnpinnedIdx = cacheRegionsForIndex(cacheService, unpinnedIdx);
        assertThat(regionsForUnpinnedIdx, greaterThan(0L));
        logger.info(
            "cache regions after searching unpinned data: pinned (positive-timestamp)={}, unpinned={}",
            pinnedRegionsAfterUnpinnedSearch,
            regionsForUnpinnedIdx
        );

        assertThat(
            "pinned regions must not be evicted: PinnedWindowEvictionPolicy protects regions with timestamps inside the window",
            pinnedRegionsAfterUnpinnedSearch,
            equalTo(pinnedRegionsAfterPinnedSearch)
        );
    }

    public void testCacheDemotedToFrequencyZeroAfterSearchShardRelocation() throws Exception {
        final Settings cacheSettings = demoteClosedShardRegionsTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNodeA = startSearchNode(cacheSettings);
        final String searchNodeB = startSearchNode(cacheSettings);
        final StatelessSharedBlobCacheService cacheServiceA = getCacheService(searchNodeA);

        final String indexName = randomIdentifier();
        final ShardId shardId = createIndexWithPopulatedCacheExcludingNode(indexName, searchNodeB, cacheServiceA);

        relocateSearchShardFromNodeToNode(indexName, searchNodeA, searchNodeB);

        assertBusy(() -> verify(cacheServiceA, atLeastOnce()).demoteAllAsync(ArgumentMatchers.any(), ArgumentMatchers.any()));
        assertDemotedToFrequencyZero(cacheServiceA, shardId);
        verify(cacheServiceA, never()).forceEvictAsync(ArgumentMatchers.any());
    }

    /// Verifies the [StatelessSharedBlobCacheService#STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING] escape hatch is a
    /// live no-op when flipped off, and takes effect again when flipped back on.
    public void testDemotionOfClosedShardRegionsCanBeFlippedDynamically() throws Exception {
        final Settings cacheSettings = demoteClosedShardRegionsTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNodeA = startSearchNode(cacheSettings);
        final String searchNodeB = startSearchNode(cacheSettings);
        final StatelessSharedBlobCacheService cacheServiceA = getCacheService(searchNodeA);

        // Flip the escape hatch off, then relocate a shard away. updateClusterSettings blocks until every node has acknowledged the
        // update, so node A's cache service has observed the new value before the relocation starts.
        final String indexNotToBeDemoted = randomIdentifier();
        final ShardId shardIdNotToBeDemoted = createIndexWithPopulatedCacheExcludingNode(indexNotToBeDemoted, searchNodeB, cacheServiceA);
        setDemoteClosedShardRegionsEnabledTo(false);
        final Map<Integer, Integer> shardNotToBeDemotedFreqs = SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(
            cacheServiceA,
            shardPredicate(shardIdNotToBeDemoted)
        );
        relocateSearchShardFromNodeToNode(indexNotToBeDemoted, searchNodeA, searchNodeB);
        awaitShardStoreClosed(searchNodeA, shardIdNotToBeDemoted);

        verify(cacheServiceA, never()).demoteAllAsync(ArgumentMatchers.eq(shardIdNotToBeDemoted), ArgumentMatchers.any());
        assertThat(
            "cache regions of the shard relocated while the setting was disabled must keep their frequencies",
            SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheServiceA, shardPredicate(shardIdNotToBeDemoted)),
            equalTo(shardNotToBeDemotedFreqs)
        );

        // Flip back on and relocate a second shard, which must be demoted.
        setDemoteClosedShardRegionsEnabledTo(true);
        final String indexToBeDemoted = randomIdentifier();
        final ShardId shardIdToBeDemoted = createIndexWithPopulatedCacheExcludingNode(indexToBeDemoted, searchNodeB, cacheServiceA);
        relocateSearchShardFromNodeToNode(indexToBeDemoted, searchNodeA, searchNodeB);
        awaitShardStoreClosed(searchNodeA, shardIdToBeDemoted);

        verify(cacheServiceA, atLeastOnce()).demoteAllAsync(ArgumentMatchers.eq(shardIdToBeDemoted), ArgumentMatchers.any());
        assertDemotedToFrequencyZero(cacheServiceA, shardIdToBeDemoted);
    }

    public void testForceEvictAsyncOnIndexDelete() throws Exception {
        final Settings cacheSettings = evictDeletedIndexRegionsTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNode = startSearchNode(cacheSettings);
        final StatelessSharedBlobCacheService cacheService = getCacheService(searchNode);

        final String indexName = randomIdentifier();
        final ShardId shardId = createIndexWithPopulatedCache(indexName, cacheService);

        assertAcked(indicesAdmin().prepareDelete(indexName));

        assertBusy(() -> assertThat(cacheService.countCachedRegions(shardPredicate(shardId)), equalTo(0L)));
    }

    /// Verifies the [StatelessSharedBlobCacheService#STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING] escape hatch is a
    /// live no-op when flipped off, and takes effect again when flipped back on.
    public void testEvictionOfDeletedIndexRegionsCanBeFlippedDynamically() throws Exception {
        final Settings cacheSettings = evictDeletedIndexRegionsTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNode = startSearchNode(cacheSettings);
        final StatelessSharedBlobCacheService cacheService = getCacheService(searchNode);

        // Flip the escape hatch off, then delete an index. updateClusterSettings blocks until every node has acknowledged the update, so
        // the search node's cache service has observed the new value before the deletion starts.
        final String indexNotToBeEvicted = randomIdentifier();
        final ShardId shardIdNotToBeEvicted = createIndexWithPopulatedCache(indexNotToBeEvicted, cacheService);
        setEvictDeletedIndexRegionsEnabledTo(false);
        final long regionsBeforeDelete = cacheService.countCachedRegions(shardPredicate(shardIdNotToBeEvicted));
        assertAcked(indicesAdmin().prepareDelete(indexNotToBeEvicted));
        // beforeIndexRemoved runs before the index's stores are closed, so once the store is gone the (disabled) gate has had its chance
        // to schedule an eviction. The retention assertion below is therefore deterministic rather than racing the async force-evict.
        awaitShardStoreClosed(searchNode, shardIdNotToBeEvicted);

        verify(cacheService, never()).forceEvictAsync(ArgumentMatchers.any());
        assertThat(
            "cache regions of the index deleted while the setting was disabled must be retained",
            cacheService.countCachedRegions(shardPredicate(shardIdNotToBeEvicted)),
            equalTo(regionsBeforeDelete)
        );

        // Flip back on and delete a second index, whose regions must be evicted.
        setEvictDeletedIndexRegionsEnabledTo(true);
        final String indexToBeEvicted = randomIdentifier();
        final ShardId shardIdToBeEvicted = createIndexWithPopulatedCache(indexToBeEvicted, cacheService);
        assertAcked(indicesAdmin().prepareDelete(indexToBeEvicted));

        assertBusy(() -> assertThat(cacheService.countCachedRegions(shardPredicate(shardIdToBeEvicted)), equalTo(0L)));
    }

    public void testCacheNotDemotedWhenNodeIsShuttingDown() throws Exception {
        final Settings cacheSettings = demoteClosedShardRegionsTestSettings();
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

    /// A cache small enough that the regions of a shard stay countable, but large enough that the test indices never compete for slots.
    private static Settings.Builder smallCacheSettings() {
        return Settings.builder()
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(32))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(256))
            .put(SHARED_CACHE_DECAY_INTERVAL_SETTING.getKey(), TimeValue.timeValueDays(1));
    }

    private static Settings.Builder maybeEnableCacheBoostPreference(Settings.Builder builder) {
        if (randomBoolean()) {
            builder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
                .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true);
        }
        return builder;
    }

    private static Settings demoteClosedShardRegionsTestSettings() {
        final var builder = maybeEnableCacheBoostPreference(smallCacheSettings());
        builder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING.getKey(), true);
        builder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING.getKey(), false);
        builder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING.getKey(), false);
        return builder.build();
    }

    private static Settings evictDeletedIndexRegionsTestSettings() {
        final var builder = maybeEnableCacheBoostPreference(smallCacheSettings());
        builder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING.getKey(), true);
        builder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING.getKey(), false);
        builder.put(StatelessSharedBlobCacheService.STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING.getKey(), false);
        return builder.build();
    }

    /// Creates a one-replica index whose search shard is kept off `excludedSearchNode`, then searches it so that the search shard on
    /// the other search node has cached regions at a non-zero access frequency. Returns the shard id.
    private ShardId createIndexWithPopulatedCacheExcludingNode(
        String indexName,
        String excludedSearchNode,
        StatelessSharedBlobCacheService cacheService
    ) throws Exception {
        createIndex(indexName, indexSettings(1, 1).put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", excludedSearchNode).build());
        ensureGreen(indexName);
        indexAndSearch(indexName, randomIntBetween(10, 100));
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        assertNonZeroFrequencies(cacheService, shardId);
        return shardId;
    }

    /// Creates a one-replica index and searches it so that its search shard has cached regions. Returns the shard id.
    private ShardId createIndexWithPopulatedCache(String indexName, StatelessSharedBlobCacheService cacheService) {
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        indexAndSearch(indexName, randomIntBetween(10, 100));
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        assertThat(cacheService.countCachedRegions(shardPredicate(shardId)), greaterThan(0L));
        return shardId;
    }

    /// Moves the search shard of `indexName` from `vacatedSearchNode` to `targetSearchNode` by excluding the former from the index's
    /// routing, returning once the cluster state routing table reflects the move. Doesn't guarantee shard store has actually closed.
    private static void relocateSearchShardFromNodeToNode(String indexName, String vacatedSearchNode, String targetSearchNode) {
        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", vacatedSearchNode), indexName);
        internalCluster().awaitNodesInclude(
            indexName,
            nodes -> nodes.contains(vacatedSearchNode) == false && nodes.contains(targetSearchNode)
        );
    }

    private static void awaitShardStoreClosed(String searchNode, ShardId shardId) throws Exception {
        final NodeEnvironment nodeEnvironment = internalCluster().getInstance(NodeEnvironment.class, searchNode);
        assertBusy(
            () -> assertThat(
                "store of " + shardId + " is still open on [" + searchNode + "]",
                nodeEnvironment.lockedShards(),
                not(hasItem(shardId))
            )
        );
    }

    private static void setDemoteClosedShardRegionsEnabledTo(boolean enabled) {
        updateClusterSettings(
            Settings.builder()
                .put(StatelessSharedBlobCacheService.STATELESS_CACHE_DEMOTE_CLOSED_SHARD_REGIONS_ENABLED_SETTING.getKey(), enabled)
        );
    }

    private static void setEvictDeletedIndexRegionsEnabledTo(boolean enabled) {
        updateClusterSettings(
            Settings.builder()
                .put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_DELETED_INDEX_REGIONS_ENABLED_SETTING.getKey(), enabled)
        );
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

    private static long countZeroTimestampRegions(StatelessSharedBlobCacheService cacheService, Predicate<FileCacheKey> predicate) {
        final long[] count = new long[1];
        cacheService.iterateCachedRegions((region, freq) -> {
            if (predicate.test(region.key()) && region.timestampMillis() == SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP) {
                count[0]++;
            }
        });
        return count[0];
    }

    private static void searchNonBoostedData(String nonBoostedIdx) {
        searchData(nonBoostedIdx, 5_000, true);
    }

    private static void searchBoostedData(String boostedIdx) {
        searchData(boostedIdx, 1_000, false);
    }

    private static void searchData(String indexName, int size, boolean sortByTimestamp) {
        for (int i = 0; i < randomIntBetween(2, 4); i++) {
            final var searchRequestBuilder = prepareSearch(indexName).setSize(size);
            if (sortByTimestamp) {
                searchRequestBuilder.addSort(DataStream.TIMESTAMP_FIELD_NAME, ASC);
            }
            assertResponse(searchRequestBuilder, ElasticsearchAssertions::assertNoFailures);
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

    /**
     * Wraps the shared blob cache in a Mockito spy so tests can verify eviction and demotion calls without
     * replacing the real cache implementation.
     */
    public static class SpyCacheStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        volatile AtomicLong currentTimestamp = new AtomicLong(0);

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
            IndicesService indicesService,
            PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricHolder
        ) {
            final var real = new StatelessSharedBlobCacheService(
                nodeEnvironment,
                settings,
                clusterService.getClusterSettings(),
                threadPool,
                blobCacheMetrics,
                StatelessSharedBlobCacheService.createEvictionPolicy(
                    settings,
                    clusterService,
                    indicesService,
                    TimeProviderUtils.create(currentTimestamp::get)
                ),
                System::nanoTime,
                threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL),
                metricHolder
            );
            final StatelessSharedBlobCacheService spy = Mockito.spy(real);
            // Mockito copies the real service's fields into the spy rather than delegating to it. Reference fields such as the LFU
            // cache still point at the same objects, but a field reassigned later does not: the settings watchers registered in the
            // constructor write the maintenance flags to `real`, leaving the spy stuck on its creation-time values. Read the flags
            // through `real` so the tests below see a dynamic update.
            Mockito.doAnswer(invocation -> real.isDemoteClosedShardRegionsEnabled()).when(spy).isDemoteClosedShardRegionsEnabled();
            Mockito.doAnswer(invocation -> real.isEvictObsoleteRegionsEnabled()).when(spy).isEvictObsoleteRegionsEnabled();
            Mockito.doAnswer(invocation -> real.isEvictDeletedIndexRegionsEnabled()).when(spy).isEvictDeletedIndexRegionsEnabled();
            return spy;
        }
    }
}
