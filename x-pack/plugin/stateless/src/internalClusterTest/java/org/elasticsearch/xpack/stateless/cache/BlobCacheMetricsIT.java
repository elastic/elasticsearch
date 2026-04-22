/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BlobCacheMetricsIT extends AbstractStatelessPluginIntegTestCase {

    private static final ByteSizeValue CACHE_REGION_SIZE = ByteSizeValue.ofBytes(8L * SharedBytes.PAGE_SIZE);

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // keep region small, so we don't need a massive cache
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), CACHE_REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), CACHE_REGION_SIZE)
            // prevent automatic uploading
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            // ensure we have a cache large enough to allow warming
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(10L))
            // prevent online prewarming since it creates unwanted noise for the cache miss metric checks
            .put(STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(SynchronousWarmingPlugin.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testSearchNodeMetrics() {
        startMasterAndIndexNode();

        // Create some indices
        final var flushedIndex = createIndexWithNoReplicas("flushed");
        final var notFlushedIndex = createIndexWithNoReplicas("notflushed");

        // Write docs to flushedIndex, flush
        populateIndex(flushedIndex);
        flush(flushedIndex);

        // Write docs to notFlushedIndex, do not flush
        populateIndex(notFlushedIndex);

        // Start a search node, and wait for the search shards to be allocated (this should trigger warming)
        final var searchNode = startSearchNode();
        ensureStableCluster(2);
        setReplicaCount(1, flushedIndex);
        setReplicaCount(1, notFlushedIndex);
        ensureGreen(flushedIndex, notFlushedIndex);

        // The flushed index should warm from the BlobStore
        assertMetricsArePresent(searchNode, BlobCacheMetrics.CachePopulationReason.Warming, CachePopulationSource.BlobStore);

        // The not-flushed index should warm from the indexing node
        assertMetricsArePresent(searchNode, BlobCacheMetrics.CachePopulationReason.Warming, CachePopulationSource.Peer);

        // clear the metrics
        getTestTelemetryPlugin(searchNode).resetMeter();

        // evict everything from the cache
        clearShardCache(findSearchShard(notFlushedIndex));
        clearShardCache(findSearchShard(flushedIndex));

        // assert appropriate cache-miss metrics are published when searching
        executeSearchAndAssertCacheMissMetrics(searchNode, notFlushedIndex, CachePopulationSource.Peer);
        executeSearchAndAssertCacheMissMetrics(searchNode, flushedIndex, CachePopulationSource.BlobStore);

        executeNoMissSearch(searchNode, notFlushedIndex);
        executeNoMissSearch(searchNode, flushedIndex);
    }

    private void clearShardCache(IndexShard indexShard) {
        BlobStoreCacheDirectory indexShardBlobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
        getCacheService(indexShardBlobStoreCacheDirectory).forceEvict((key) -> true);
    }

    private static void executeSearchAndAssertCacheMissMetrics(
        String searchNode,
        String indexName,
        CachePopulationSource expectedPopulationSource
    ) {
        TestTelemetryPlugin testTelemetryPlugin = getTestTelemetryPlugin(searchNode);
        testTelemetryPlugin.collect();
        long reads = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong();
        long misses = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong();
        assertThat(misses, lessThanOrEqualTo(reads));

        executeSearch(indexName);

        // Confirm we see cache-miss metrics on the search node
        assertMetricsArePresent(searchNode, BlobCacheMetrics.CachePopulationReason.CacheMiss, expectedPopulationSource);

        testTelemetryPlugin.collect();

        long newReads = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong();
        long newMisses = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong();
        double newRatio = testTelemetryPlugin.getDoubleGaugeMeasurement("es.blob_cache.miss.ratio").getLast().getDouble();

        assertThat(newReads, greaterThan(reads));
        assertThat(newMisses, greaterThan(misses));
        assertThat(newMisses, lessThanOrEqualTo(reads));
        assertThat(newRatio, greaterThan(0d));
    }

    private static void executeNoMissSearch(String searchNode, String indexName) {
        TestTelemetryPlugin testTelemetryPlugin = getTestTelemetryPlugin(searchNode);
        testTelemetryPlugin.collect();
        long reads = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong();
        long misses = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong();
        double ratio = testTelemetryPlugin.getDoubleGaugeMeasurement("es.blob_cache.miss.ratio").getLast().getDouble();
        assertThat(misses, lessThanOrEqualTo(reads));

        executeSearch(indexName);

        testTelemetryPlugin.collect();
        long newReads = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong();
        long newMisses = testTelemetryPlugin.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong();
        double newRatio = testTelemetryPlugin.getDoubleGaugeMeasurement("es.blob_cache.miss.ratio").getLast().getDouble();

        assertThat(newReads, greaterThan(reads));
        assertThat(newMisses, equalTo(misses));
        assertThat(newRatio, lessThan(ratio));
    }

    private static void executeSearch(String indexName) {
        // Execute a match-all query against the index (should trigger cache-misses)
        safeGet(prepareSearch(indexName).setQuery(matchAllQuery()).setSize(10_000).execute()).decRef();
    }

    public void testWarmingMetricsArePublishedOnIndexNode() {
        var originalIndexNode = startMasterAndIndexNode();

        // Create an index
        final String indexName = createIndexWithNoReplicas("index");
        ensureGreen(indexName);

        // Index a bunch of docs, flush
        populateIndex(indexName);
        flush(indexName);

        // Start a second index node
        String otherIndexNode = startIndexNode();
        ensureStableCluster(2);

        // Trigger relocation, wait till it's complete
        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", originalIndexNode), indexName);
        ensureGreen(indexName);

        // Confirm we see warming metrics on the newly assigned node
        assertMetricsArePresent(otherIndexNode, BlobCacheMetrics.CachePopulationReason.Warming, CachePopulationSource.BlobStore);
    }

    public void testBypassReadMetrics() {
        startMasterAndIndexNode();
        final var noCacheSearchNode = startSearchNode(
            Settings.builder().put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ZERO).build()
        );
        final var normalCacheSearchNode = startSearchNode();
        ensureStableCluster(3);

        final String byPassIndexName = randomIdentifier("bypass");
        createIndex(byPassIndexName, indexSettings(1, 1).put("index.routing.allocation.exclude._name", normalCacheSearchNode).build());
        populateIndex(byPassIndexName);

        final String regularIndexName = randomIdentifier("regular");
        createIndex(regularIndexName, indexSettings(1, 1).put("index.routing.allocation.exclude._name", noCacheSearchNode).build());
        populateIndex(regularIndexName);

        clearShardCache(findSearchShard(byPassIndexName));
        clearShardCache(findSearchShard(regularIndexName));

        flush(byPassIndexName, regularIndexName);
        executeSearch(byPassIndexName);
        executeSearch(regularIndexName);

        // No-cache node: all reads bypass the cache
        final var noCacheTelemetry = getTestTelemetryPlugin(noCacheSearchNode);
        noCacheTelemetry.collect();
        long noCacheBypassCount = noCacheTelemetry.getLongCounterMeasurement(BlobCacheMetrics.BLOB_CACHE_BYPASS_READ_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat(noCacheBypassCount, greaterThan(0L));
        // Bypass reads count as both reads and misses
        assertThat(noCacheTelemetry.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong(), equalTo(noCacheBypassCount));
        assertThat(noCacheTelemetry.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong(), equalTo(noCacheBypassCount));

        // Normal-cache node: reads and misses but no bypass reads
        final var normalCacheTelemetry = getTestTelemetryPlugin(normalCacheSearchNode);
        normalCacheTelemetry.collect();
        assertThat(normalCacheTelemetry.getLongGaugeMeasurement("es.blob_cache.read.total").getLast().getLong(), greaterThan(0L));
        assertThat(normalCacheTelemetry.getLongGaugeMeasurement("es.blob_cache.miss.total").getLast().getLong(), greaterThan(0L));
        long normalCacheBypassCount = normalCacheTelemetry.getLongCounterMeasurement(BlobCacheMetrics.BLOB_CACHE_BYPASS_READ_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat(normalCacheBypassCount, equalTo(0L));
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
                    // disable automatic refresh
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            )
        );
        return indexName;
    }

    private void populateIndex(String indexName) {
        // Use at least 2 segments so that index data extends beyond cache region 0. ShardWarmer skips
        // region 0 for SEARCH warming (it's assumed already loaded), so with a single small segment all
        // file locations land in region 0 and warming records no metrics.
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

    private static void assertMetricsArePresent(
        String nodeName,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource
    ) {
        final TestTelemetryPlugin telemetryPlugin = getTestTelemetryPlugin(nodeName);

        // There is at least one `population.throughput.histogram` measurement
        assertContainsMeasurement(
            telemetryPlugin.getDoubleHistogramMeasurement("es.blob_cache.population.throughput.histogram"),
            cachePopulationReason,
            cachePopulationSource
        );

        // There is at least one `population.bytes.total` measurement
        assertContainsMeasurement(
            telemetryPlugin.getLongCounterMeasurement("es.blob_cache.population.bytes.total"),
            cachePopulationReason,
            cachePopulationSource
        );

        // There is at least one `population.time.total` measurement
        assertContainsMeasurement(
            telemetryPlugin.getLongCounterMeasurement("es.blob_cache.population.time.total"),
            cachePopulationReason,
            cachePopulationSource
        );
    }

    private static TestTelemetryPlugin getTestTelemetryPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
    }

    private static void assertContainsMeasurement(
        List<Measurement> measurements,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource
    ) {
        assertTrue(
            "No " + cachePopulationReason + "/" + cachePopulationSource + " metrics found in " + measurements,
            measurements.stream().anyMatch(m -> isMatchingMeasurement(m, cachePopulationReason, cachePopulationSource))
        );
    }

    private static boolean isMatchingMeasurement(
        Measurement measurement,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource
    ) {
        Map<String, Object> attributes = measurement.attributes();
        return attributes.get(BlobCacheMetrics.CACHE_POPULATION_REASON_ATTRIBUTE_KEY) == cachePopulationReason.name()
            && attributes.get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY) == cachePopulationSource.name();
    }

    /**
     * Makes recovery warming synchronous so that warming completes before shard recovery finishes.
     * This prevents a race where CacheMiss reads from the search engine opening the shard populate the
     * cache before warming tasks run, causing warming to find no gaps and record no metrics.
     */
    public static class SynchronousWarmingPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public SynchronousWarmingPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            ClusterSettings clusterSettings,
            WarmingRatioProvider warmingRatioProvider
        ) {
            return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, clusterSettings, warmingRatioProvider) {
                @Override
                protected void warmCache(
                    Type type,
                    IndexShard indexShard,
                    StatelessCompoundCommit commit,
                    BlobStoreCacheDirectory directory,
                    @Nullable Map<BlobFile, Long> endOffsetsToWarm,
                    boolean preWarmForIdLookup,
                    ActionListener<Void> listener
                ) {
                    var subscribableListener = new SubscribableListener<Void>();
                    super.warmCache(type, indexShard, commit, directory, endOffsetsToWarm, preWarmForIdLookup, subscribableListener);
                    safeAwait(subscribableListener);
                    subscribableListener.addListener(listener);
                }
            };
        }
    }
}
