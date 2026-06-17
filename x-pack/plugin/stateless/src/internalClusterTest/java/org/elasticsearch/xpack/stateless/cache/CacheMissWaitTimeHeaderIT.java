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
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class CacheMissWaitTimeHeaderIT extends AbstractStatelessPluginIntegTestCase {

    private static final String SEARCH_METRICS_HEADER = "X-Elasticsearch-Search-Metrics";

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

        Map<String, Long> coldCacheHeaders = searchAndParseMetricsHeaders(indexName);
        assertThat(coldCacheHeaders, hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAITS_HEADER));
        assertThat(coldCacheHeaders, hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER));
        assertThat(coldCacheHeaders, hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_BYTES_HEADER));
        assertThat(coldCacheHeaders.get(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAITS_HEADER), greaterThanOrEqualTo(1L));
        assertThat(coldCacheHeaders.get(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER), greaterThan(0L));
        assertThat(coldCacheHeaders.get(BlobStoreCacheDirectoryMetrics.CACHE_MISS_BYTES_HEADER), greaterThan(0L));

        Map<String, Long> warmCacheHeaders = searchAndParseMetricsHeaders(indexName);
        assertThat(warmCacheHeaders, not(hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAITS_HEADER)));
        assertThat(warmCacheHeaders, not(hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER)));
        assertThat(warmCacheHeaders, not(hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_BYTES_HEADER)));
    }

    private Map<String, Long> searchAndParseMetricsHeaders(String indexName) throws InterruptedException {
        SearchRequest searchRequest = new SearchRequest(indexName).searchType(SearchType.QUERY_THEN_FETCH)
            .source(new SearchSourceBuilder().query(matchAllQuery()).size(10_000));

        SetOnce<Map<String, Long>> headers = new SetOnce<>();
        SetOnce<Exception> failure = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        final Client client = client();
        client.search(searchRequest, new LatchedActionListener<>(ActionListener.assertOnce(ActionListener.wrap(searchResponse -> {
            headers.set(parseMetricsHeaders(client));
        }, failure::set)), latch));
        assertTrue("search did not complete in time", latch.await(30, TimeUnit.SECONDS));
        if (failure.get() != null) {
            throw new AssertionError("unexpected search failure", failure.get());
        }
        assertThat(headers.get(), notNullValue());
        return headers.get();
    }

    private static Map<String, Long> parseMetricsHeaders(Client client) {
        Map<String, List<String>> responseHeaders = client.threadPool().getThreadContext().getResponseHeaders();
        assertThat(responseHeaders, hasKey(SEARCH_METRICS_HEADER));
        List<String> values = responseHeaders.get(SEARCH_METRICS_HEADER);
        assertThat(values, notNullValue());
        assertThat("expected at least one metrics header value", values.isEmpty(), equalTo(false));

        Map<String, Long> parsed = new HashMap<>();
        for (String value : values) {
            Tuple<String, Long> entry = parseHeader(value);
            parsed.put(entry.v1(), entry.v2());
        }
        return Map.copyOf(parsed);
    }

    private static Tuple<String, Long> parseHeader(String headerValue) {
        int splitterPos = headerValue.indexOf('=');
        if (splitterPos < 0) {
            throw new IllegalArgumentException("invalid header entry [" + headerValue + "]");
        }
        String key = headerValue.substring(0, splitterPos).trim();
        long value = Long.parseLong(headerValue.substring(splitterPos + 1).trim());
        return Tuple.tuple(key, value);
    }

    private void clearShardCache(IndexShard indexShard) {
        BlobStoreCacheDirectory blobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
        getCacheService(blobStoreCacheDirectory).forceEvict(key -> true);
    }

    private String createIndexWithNoReplicas(String namePrefix) {
        final String indexName = namePrefix + "-" + randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            )
        );
        return indexName;
    }

    private void populateIndex(String indexName) {
        final int iters = randomIntBetween(2, 3);
        int docsCounter = 0;
        for (int i = 0; i < iters; i++) {
            int numDocs = randomIntBetween(100, 1_000);
            indexDocs(indexName, numDocs);
            refresh(indexName);
            docsCounter += numDocs;
        }
        logger.info("--> Wrote {} documents in {} segments to index {}", docsCounter, iters, indexName);
    }
}
