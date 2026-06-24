/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.blobcache.shared.SharedBlobCacheServiceTestUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class CacheBoostPreferenceIT extends AbstractStatelessPluginIntegTestCase {

    public void testCacheAccessCountsResetAfterSearchShardRelocation() throws Exception {
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
        assertNonZeroAccessCounts(cacheServiceA, shardId);

        updateIndexSettings(Settings.builder().put(INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", searchNodeA), indexName);
        internalCluster().awaitNodesInclude(indexName, nodes -> nodes.contains(searchNodeA) == false && nodes.contains(searchNodeB));

        assertAccessCountsReset(cacheServiceA, shardId);
    }

    public void testCacheAccessCountsResetAfterIndexClose() throws Exception {
        final Settings cacheSettings = cacheBoostPreferenceTestSettings();
        startMasterAndIndexNode(cacheSettings);
        final String searchNode = startSearchNode(cacheSettings);
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        indexAndSearch(indexName, randomIntBetween(10, 100));

        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);
        final StatelessSharedBlobCacheService cacheService = getCacheService(searchNode);
        assertNonZeroAccessCounts(cacheService, shardId);

        assertAcked(indicesAdmin().close(new CloseIndexRequest(indexName)).actionGet());

        assertAccessCountsReset(cacheService, shardId);
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
            .filterPlugins(TestUtils.StatelessPluginWithTrialLicense.class)
            .findFirst()
            .orElseThrow(() -> new AssertionError("stateless plugin not found on node [" + nodeName + "]"));
        return statelessPlugin.getStatelessSharedBlobCacheService();
    }

    private static Predicate<FileCacheKey> shardPredicate(ShardId shardId) {
        return key -> key.shardId().equals(shardId);
    }

    private static void assertNonZeroAccessCounts(StatelessSharedBlobCacheService cacheService, ShardId shardId) throws Exception {
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

    private static void assertAccessCountsReset(StatelessSharedBlobCacheService cacheService, ShardId shardId) throws Exception {
        assertBusy(() -> {
            long regionCount = cacheService.countCachedRegions(shardPredicate(shardId));
            assertThat(regionCount, greaterThan(0L));
            assertThat(
                SharedBlobCacheServiceTestUtils.countCachedRegionsByFreq(cacheService, shardPredicate(shardId)),
                equalTo(Map.of(0, (int) regionCount))
            );
        });
    }

}
