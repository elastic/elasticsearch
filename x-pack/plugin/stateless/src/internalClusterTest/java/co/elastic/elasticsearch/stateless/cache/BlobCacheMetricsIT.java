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
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils.getCacheService;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

public class BlobCacheMetricsIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // prevent automatic uploading
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1));
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
        assertMetricsArePresent(
            searchNode,
            findSearchShard(flushedIndex).shardId(),
            BlobCacheMetrics.CachePopulationReason.Warming,
            CachePopulationSource.BlobStore
        );

        // The not-flushed index should warm from the indexing node
        assertMetricsArePresent(
            searchNode,
            findSearchShard(notFlushedIndex).shardId(),
            BlobCacheMetrics.CachePopulationReason.Warming,
            CachePopulationSource.Peer
        );

        // clear the metrics
        getTestTelemetryPlugin(searchNode).resetMeter();

        // evict everything from the cache
        evictShardCache(findSearchShard(notFlushedIndex));
        evictShardCache(findSearchShard(flushedIndex));

        // assert appropriate cache-miss metrics are published when searching
        executeSearchAndAssertCacheMissMetrics(searchNode, notFlushedIndex, CachePopulationSource.Peer);
        executeSearchAndAssertCacheMissMetrics(searchNode, flushedIndex, CachePopulationSource.BlobStore);
    }

    private void evictShardCache(IndexShard indexShard) {
        BlobStoreCacheDirectory indexShardBlobStoreCacheDirectory = BlobStoreCacheDirectory.unwrapDirectory(indexShard.store().directory());
        getCacheService(indexShardBlobStoreCacheDirectory).forceEvict((key) -> true);
    }

    private static void executeSearchAndAssertCacheMissMetrics(
        String searchNode,
        String indexName,
        CachePopulationSource expectedPopulationSource
    ) {
        // Execute a match-all query against the index (should trigger cache-misses)
        safeGet(prepareSearch(indexName).setQuery(matchAllQuery()).setSize(10_000).execute()).decRef();

        // Confirm we see cache-miss metrics on the search node
        assertMetricsArePresent(
            searchNode,
            findSearchShard(indexName).shardId(),
            BlobCacheMetrics.CachePopulationReason.CacheMiss,
            expectedPopulationSource
        );
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
        assertMetricsArePresent(
            otherIndexNode,
            findIndexShard(indexName).shardId(),
            BlobCacheMetrics.CachePopulationReason.Warming,
            CachePopulationSource.BlobStore
        );
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
        for (int i = 0; i < iters; i++) {
            indexDocs(indexName, randomIntBetween(100, 1_000));
            refresh(indexName);
        }
    }

    private static void assertMetricsArePresent(
        String nodeName,
        ShardId shardId,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource
    ) {
        final TestTelemetryPlugin telemetryPlugin = getTestTelemetryPlugin(nodeName);

        // There is at least one `population.throughput.histogram` measurement
        assertContainsMeasurementForShard(
            telemetryPlugin.getDoubleHistogramMeasurement("es.blob_cache.population.throughput.histogram"),
            cachePopulationReason,
            cachePopulationSource,
            shardId
        );

        // There is at least one `population.bytes.total` measurement
        assertContainsMeasurementForShard(
            telemetryPlugin.getLongCounterMeasurement("es.blob_cache.population.bytes.total"),
            cachePopulationReason,
            cachePopulationSource,
            shardId
        );

        // There is at least one `population.time.total` measurement
        assertContainsMeasurementForShard(
            telemetryPlugin.getLongCounterMeasurement("es.blob_cache.population.time.total"),
            cachePopulationReason,
            cachePopulationSource,
            shardId
        );
    }

    private static TestTelemetryPlugin getTestTelemetryPlugin(String nodeName) {
        return internalCluster().getInstance(PluginsService.class, nodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
    }

    private static void assertContainsMeasurementForShard(
        List<Measurement> measurements,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource,
        ShardId shardId
    ) {
        assertTrue(
            "No " + cachePopulationReason + "/" + cachePopulationSource + " metrics found in " + measurements,
            measurements.stream().anyMatch(m -> isMeasurementForShard(m, cachePopulationReason, cachePopulationSource, shardId))
        );
    }

    private static boolean isMeasurementForShard(
        Measurement measurement,
        BlobCacheMetrics.CachePopulationReason cachePopulationReason,
        CachePopulationSource cachePopulationSource,
        ShardId shardId
    ) {
        Map<String, Object> attributes = measurement.attributes();
        return attributes.get(BlobCacheMetrics.CACHE_POPULATION_REASON_ATTRIBUTE_KEY) == cachePopulationReason.name()
            && attributes.get(BlobCacheMetrics.CACHE_POPULATION_SOURCE_ATTRIBUTE_KEY) == cachePopulationSource.name()
            && Objects.equals(attributes.get(BlobCacheMetrics.SHARD_ID_ATTRIBUTE_KEY), shardId.getId())
            && shardId.getIndexName().equals(attributes.get(BlobCacheMetrics.INDEX_ATTRIBUTE_KEY));
    }
}
