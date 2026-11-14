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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.StoreMetrics;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

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
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.CACHE_MISS_WAIT_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class CacheMissWaitTimeHeaderIT extends AbstractBlobCacheMetricsIntegTestCase {

    private static final ByteSizeValue CACHE_REGION_SIZE = ByteSizeValue.ofBytes(8L * SharedBytes.PAGE_SIZE);

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
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testCacheMissWaitTimeHeader() throws InterruptedException {
        assertCacheMissWaitTimeHeader("cache-miss-header", true);
    }

    /**
     * One shard is local to the coordinator and the other is on another search node, and both wait on cache misses. The metric must
     * account for the waits of both.
     */
    public void testRecordsWaitOfLocalAndRemoteShards() throws Exception {
        startMasterAndIndexNode();
        String indexName = "mixed-" + randomIdentifier();
        createIndex(indexName, indexSettings(2, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        populateIndex(indexName);
        flush(indexName);
        startSearchNode();
        startSearchNode();
        ensureStableCluster(3);
        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);
        IndexShard localShard = findSearchShard(index, 0);
        IndexShard remoteShard = findSearchShard(index, 1);
        String coordinatorId = localShard.routingEntry().currentNodeId();
        assertThat(remoteShard.routingEntry().currentNodeId(), not(equalTo(coordinatorId)));
        String coordinator = internalCluster().clusterService().state().nodes().get(coordinatorId).getName();
        clearShardCache(localShard);
        clearShardCache(remoteShard);
        resetTelemetry();
        assertResponse(client(coordinator).prepareSearch(indexName).setSize(10_000), response -> {
            // the waits of the local and the remote shard are merged into one entry, which is the one the metric is recorded from
            long totalWaitNanos = response.getDirectoryMetrics()
                .metrics(BlobStoreCacheDirectoryMetrics.NAME)
                .cast(BlobStoreCacheDirectoryMetrics.class)
                .getWaitTime();
            assertThat(totalWaitNanos, greaterThan(0L));
            List<Measurement> measurements = getTelemetryPlugin(coordinator).getDoubleHistogramMeasurement(CACHE_MISS_WAIT_HISTOGRAM_NAME);
            assertThat(measurements, hasSize(1));
            assertThat(measurements.get(0).getDouble(), equalTo(totalWaitNanos / (double) TimeUnit.MILLISECONDS.toNanos(1)));
        });
    }

    /**
     * Cold search asserts {@code cache_miss_wait_nanos > 0}; warm asserts it's absent.
     * {@code expectStoreBytesRead} gates store metric assertions.
     */
    protected void assertCacheMissWaitTimeHeader(String indexNamePrefix, boolean expectStoreBytesRead) throws InterruptedException {
        startMasterAndIndexNode();

        final String indexName = createIndexWithNoReplicas(indexNamePrefix);
        populateIndex(indexName);
        flush(indexName);

        startSearchNode();
        ensureStableCluster(2);
        setReplicaCount(1, indexName);
        ensureGreen(indexName);

        clearShardCache(findSearchShard(indexName));

        resetTelemetry();
        Map<String, Long> coldCacheMetrics = searchAndCollectDirectoryMetrics(indexName);
        assertThat(coldCacheMetrics, hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER));
        assertThat(coldCacheMetrics.get(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER), greaterThan(0L));
        // the same value reaches the APM metric, in milliseconds
        List<Measurement> coldWaitMeasurements = cacheMissWaitMeasurements();
        assertThat(coldWaitMeasurements, hasSize(1));
        assertThat(
            coldWaitMeasurements.get(0).getDouble(),
            equalTo(
                coldCacheMetrics.get(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER) / (double) TimeUnit.MILLISECONDS.toNanos(
                    1
                )
            )
        );
        if (expectStoreBytesRead) {
            assertThat(coldCacheMetrics, hasKey(StoreMetrics.BYTES_READ_METRIC_KEY));
            // the blob cache directory accounts its own reads, so this also covers the holder reaching its inputs
            assertThat(coldCacheMetrics.get(StoreMetrics.BYTES_READ_METRIC_KEY), greaterThan(0L));
        } else {
            assertThat(coldCacheMetrics, not(hasKey(StoreMetrics.BYTES_READ_METRIC_KEY)));
        }

        resetTelemetry();
        Map<String, Long> warmCacheMetrics = searchAndCollectDirectoryMetrics(indexName);
        assertThat(warmCacheMetrics, not(hasKey(BlobStoreCacheDirectoryMetrics.CACHE_MISS_WAIT_NANOS_HEADER)));
        assertThat(cacheMissWaitMeasurements(), empty());
        if (expectStoreBytesRead == false) {
            assertThat(warmCacheMetrics, not(hasKey(StoreMetrics.BYTES_READ_METRIC_KEY)));
        }
    }

    protected Map<String, Long> searchAndCollectDirectoryMetrics(String indexName) throws InterruptedException {
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

    /** The coordinator that records the metric is whichever node received the search, so look at every node. */
    private static List<Measurement> cacheMissWaitMeasurements() {
        List<Measurement> measurements = new ArrayList<>();
        for (String nodeName : internalCluster().getNodeNames()) {
            measurements.addAll(getTelemetryPlugin(nodeName).getDoubleHistogramMeasurement(CACHE_MISS_WAIT_HISTOGRAM_NAME));
        }
        return measurements;
    }

    private static void resetTelemetry() {
        for (String nodeName : internalCluster().getNodeNames()) {
            getTelemetryPlugin(nodeName).resetMeter();
        }
    }

    private static Map<String, Long> parseDirectoryMetrics(SearchResponse searchResponse) {
        Map<String, Long> parsed = new HashMap<>();
        for (Map.Entry<String, String> entry : searchResponse.getDirectoryMetrics().entries().entrySet()) {
            parsed.put(entry.getKey(), Long.parseLong(entry.getValue()));
        }
        return Map.copyOf(parsed);
    }

}
