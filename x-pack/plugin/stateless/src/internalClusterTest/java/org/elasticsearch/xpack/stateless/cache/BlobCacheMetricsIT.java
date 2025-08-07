/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static co.elastic.elasticsearch.stateless.cache.StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED;
import static co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BlobCacheMetricsIT extends AbstractStatelessIntegTestCase {

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
        final int iters = randomIntBetween(1, 3);
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
}
