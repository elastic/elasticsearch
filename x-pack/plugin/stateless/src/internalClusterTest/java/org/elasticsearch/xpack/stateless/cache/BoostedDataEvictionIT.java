/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.util.Collection;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.common.util.CollectionUtils.appendToCopy;
import static org.elasticsearch.core.TimeValue.MINUS_ONE;
import static org.elasticsearch.search.sort.SortOrder.ASC;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThan;

public class BoostedDataEvictionIT extends AbstractStatelessPluginIntegTestCase {

    private static final String TIMESTAMP_MAPPING = """
        {"properties":{"@timestamp":{"type":"date"}}}""";

    // non-boosted doc-value reads (sort forces reading all values per segment) overflow it.
    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(4); // TODO randomisation
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofKb(256);
    private static final long BOOST_WINDOW_MILLIS = TimeValue.timeValueDays(7).millis();
    private static final long ONE_DAY_MILLIS = TimeValue.timeValueDays(1).millis();
    private final String BOOSTED_IDX = randomIdentifier();
    private final String NON_BOOSTED_IDX = randomIdentifier();

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return appendToCopy(super.nodePlugins(), InternalSettingsPlugin.class);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // Disable all background warmers so nothing populates the cache between test steps
        return super.nodeSettings().put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false)
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
            .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), false);
    }

    public void testNonBoostedSearchesEvictBoostedData() {
        startNodes();
        createIndexes(BOOSTED_IDX, NON_BOOSTED_IDX);

        final long boostWindowEndInMillis = System.currentTimeMillis();
        final long boostWindowStartInMillis = boostWindowEndInMillis - BOOST_WINDOW_MILLIS + ONE_DAY_MILLIS;
        indexDocuments(10, BOOSTED_IDX, 1_000, boostWindowStartInMillis, boostWindowEndInMillis);

        final long preBoostWindowEndInMillis = boostWindowEndInMillis - BOOST_WINDOW_MILLIS - 2 * ONE_DAY_MILLIS;
        final long preBoostWindowStartInMillis = preBoostWindowEndInMillis - 30L * ONE_DAY_MILLIS;
        indexDocuments(5, NON_BOOSTED_IDX, 10_000, preBoostWindowStartInMillis, preBoostWindowEndInMillis);

        final StatelessSharedBlobCacheService cacheService = getCacheService();
        cacheService.forceEvict(key -> true);
        logger.info("cache after forceEvict: {}", cacheService.getStats());

        // Step 1 — populate the cache with boosted data via a single on-demand search.
        // All boosted regions start at LFU frequency 1 (written once, not yet promoted).
        searchBoostedData(BOOSTED_IDX);

        final SharedBlobCacheService.Stats statsAfterBoostSearch = cacheService.getStats();
        logger.info("cache after boosted search: {}", statsAfterBoostSearch);

        assertThat("boosted data should have been loaded into the cache", statsAfterBoostSearch.writeBytes(), greaterThan(0L));
        assertThat("boosted cache regions should be resident", statsAfterBoostSearch.numberOfRegions(), greaterThan(0));

        // Step 2 — drive non-boosted searches. Sorting by @timestamp forces reading all doc-value
        // data per segment, generating enough blob-cache reads to overflow the small cache.
        // Both boosted and non-boosted regions compete at the same LFU frequency (1); the older
        // boosted regions are evicted first under the LFU clock.
        final long evictCountBaseline = statsAfterBoostSearch.evictCount();
        searchNonBoostedData(NON_BOOSTED_IDX);

        logger.info("cache after non-boosted searches: {}", cacheService.getStats());
        assertThat(
            "non-boosted searches must have caused cache evictions",
            cacheService.getStats().evictCount(),
            greaterThan(evictCountBaseline)
        );

        // Step 3 — re-search the boosted index. Evicted boosted regions must be re-fetched from
        // the object store, proving that non-boosted traffic displaced the boosted cache data.
        final SharedBlobCacheService.Stats statsBeforeReSearch = cacheService.getStats();
        searchBoostedData(BOOSTED_IDX);

        logger.info("cache after boosted re-search: {}", cacheService.getStats());
        assertThat(
            "re-search of boosted index must trigger cache misses because boosted regions were evicted",
            cacheService.getStats().missCount(),
            greaterThan(statsBeforeReSearch.missCount())
        );
    }

    private static void searchNonBoostedData(String nonBoostedIdx) {
        for (int i = 0; i < randomIntBetween(2, 4); i++) {
            assertResponse(
                prepareSearch(nonBoostedIdx).setSize(1_000).addSort(DataStream.TIMESTAMP_FIELD_NAME, ASC),
                ElasticsearchAssertions::assertNoFailures
            );
        }
    }

    private static void searchBoostedData(String boostedIdx) {
        assertResponse(prepareSearch(boostedIdx).setSize(1_000), ElasticsearchAssertions::assertNoFailures);
    }

    private void startNodes() {
        Settings cacheSettings = Settings.builder()
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE)
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE)
            .build();
        startMasterAndIndexNode(cacheSettings);
        startSearchNode(cacheSettings);
    }

    private void createIndexes(String boostedIdx, String nonBoostedIdx) {
        final Settings idxSettings = ESTestCase.indexSettings(1, 1)
            .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE)
            .put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false")
            .build();

        assertAcked(prepareCreate(boostedIdx).setSettings(idxSettings).setMapping(TIMESTAMP_MAPPING));
        assertAcked(prepareCreate(nonBoostedIdx).setSettings(idxSettings).setMapping(TIMESTAMP_MAPPING));
        ensureGreen(boostedIdx, nonBoostedIdx);
    }

    private void indexDocuments(int numBatches, String indexName, int numDocs, long startInMillis, long endInMillis) {
        range(0, numBatches).forEach(i -> indexDocumentsWithTimestamp(indexName, numDocs, startInMillis, endInMillis));
        flush(indexName);
    }

    private void indexDocumentsWithTimestamp(String indexName, int numDocs, long minTimestamp, long maxTimestamp) {
        var bulk = client().prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        range(0, numDocs).mapToObj(
            i -> client().prepareIndex(indexName).setSource(DataStream.TIMESTAMP_FIELD_NAME, randomLongBetween(minTimestamp, maxTimestamp))
        ).forEach(bulk::add);
        assertNoFailures(bulk.get());
    }

    private StatelessSharedBlobCacheService getCacheService() {
        final IndexShard boostedShard = findSearchShard(BOOSTED_IDX);
        return BlobStoreCacheDirectoryTestUtils.getCacheService(SearchDirectory.unwrapDirectory(boostedShard.store().directory()));
    }

}
