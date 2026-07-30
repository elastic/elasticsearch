/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class CacheMissWaitTimeHeaderIT extends AbstractBlobCacheMetricsIntegTestCase {

    private static final ByteSizeValue CACHE_REGION_SIZE = ByteSizeValue.ofBytes(8L * SharedBytes.PAGE_SIZE);

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), CACHE_REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), CACHE_REGION_SIZE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(10L))
            .put(STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(BlobCacheMetricsIT.SynchronousWarmingPlugin.class);
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    public void testCacheMissWaitTimeHeader() throws InterruptedException {
        startMasterAndIndexNode();

        final String indexName = createIndexWithNoReplicas("cache-miss-header");
        populateIndex(indexName);
        flush(indexName);

        startSearchNode();
        ensureStableCluster(2);
        setReplicaCount(1, indexName);
        ensureGreen(indexName);

        clearShardCache(findSearchShard(indexName));

        Map<String, Long> coldCacheMetrics = searchAndCollectDirectoryMetrics(indexName);
        assertThat(coldCacheMetrics, hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER));
        assertThat(coldCacheMetrics.get(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER), greaterThan(0L));

        Map<String, Long> warmCacheMetrics = searchAndCollectDirectoryMetrics(indexName);
        assertThat(warmCacheMetrics, not(hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER)));
    }

    private Map<String, Long> searchAndCollectDirectoryMetrics(String indexName) throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest(indexName).searchType(SearchType.QUERY_THEN_FETCH)
            .source(new SearchSourceBuilder().query(matchAllQuery()).size(10_000));

        SetOnce<Map<String, Long>> metrics = new SetOnce<>();
        SetOnce<Exception> failure = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        client().search(searchRequest, new LatchedActionListener<>(ActionListener.assertOnce(ActionListener.wrap(searchResponse -> {
            metrics.set(parseDirectoryMetrics(searchResponse));
        }, failure::set)), latch));
        assertTrue("search did not complete in time", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("unexpected search failure", failure.get());
        }
        assertThat(metrics.get(), notNullValue());
        return metrics.get();
    }

    private static Map<String, Long> parseDirectoryMetrics(SearchResponse searchResponse) {
        Map<String, Long> parsed = new HashMap<>();
        for (Map.Entry<String, String> entry : searchResponse.getDirectoryMetrics().entries().entrySet()) {
            parsed.put(entry.getKey(), Long.parseLong(entry.getValue()));
        }
        return Map.copyOf(parsed);
    }

}
