/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.TimeRangeBucket;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.shared.SharedBytes;
import org.elasticsearch.cluster.metadata.IndexMetadata;
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
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.cache.StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BlobCacheMetricsIT extends AbstractBlobCacheMetricsIntegTestCase {

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

    private static void executeSearchAndAssertCacheMissMetrics(
        String searchNode,
        String indexName,
        CachePopulationSource expectedPopulationSource
    ) {
        TestTelemetryPlugin testTelemetryPlugin = getTestTelemetryPlugin(searchNode);
        long reads = collectAndSumReadTotal(testTelemetryPlugin);
        long misses = collectAndSumMissTotal(testTelemetryPlugin);
        assertThat(misses, lessThanOrEqualTo(reads));

        executeSearch(indexName);

        // Confirm we see cache-miss metrics on the search node
        assertMetricsArePresent(searchNode, BlobCacheMetrics.CachePopulationReason.CacheMiss, expectedPopulationSource);

        long newReads = collectAndSumReadTotal(testTelemetryPlugin);
        long newMisses = collectAndSumMissTotal(testTelemetryPlugin);
        double newRatio = testTelemetryPlugin.getDoubleGaugeMeasurement("es.blob_cache.miss.ratio").getLast().getDouble();

        assertThat(newReads, greaterThan(reads));
        assertThat(newMisses, greaterThan(misses));
        assertThat(newMisses, lessThanOrEqualTo(reads));
        assertThat(newRatio, greaterThan(0d));
    }

    private static void executeNoMissSearch(String searchNode, String indexName) {
        TestTelemetryPlugin testTelemetryPlugin = getTestTelemetryPlugin(searchNode);
        long reads = collectAndSumReadTotal(testTelemetryPlugin);
        long misses = collectAndSumMissTotal(testTelemetryPlugin);
        double ratio = testTelemetryPlugin.getDoubleGaugeMeasurement("es.blob_cache.miss.ratio").getLast().getDouble();
        assertThat(misses, lessThanOrEqualTo(reads));

        executeSearch(indexName);

        long newReads = collectAndSumReadTotal(testTelemetryPlugin);
        long newMisses = collectAndSumMissTotal(testTelemetryPlugin);
        double newRatio = testTelemetryPlugin.getDoubleGaugeMeasurement("es.blob_cache.miss.ratio").getLast().getDouble();

        assertThat(newReads, greaterThan(reads));
        assertThat(newMisses, equalTo(misses));
        assertThat(newRatio, lessThan(ratio));
    }

    /**
     * Resets the meter, triggers a fresh collect, then sums {@code es.blob_cache.read.total} across all
     * {@link BlobCacheMetrics#REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY} buckets. Because the gauge now emits one
     * observation per bucket per collection interval, callers must SUM across buckets to obtain the node-level total.
     */
    private static long collectAndSumReadTotal(TestTelemetryPlugin plugin) {
        plugin.resetMeter();
        plugin.collect();
        return plugin.getLongGaugeMeasurement("es.blob_cache.read.total").stream().mapToLong(Measurement::getLong).sum();
    }

    /** Like {@link #collectAndSumReadTotal} but for {@code es.blob_cache.miss.total}. */
    private static long collectAndSumMissTotal(TestTelemetryPlugin plugin) {
        plugin.resetMeter();
        plugin.collect();
        return plugin.getLongGaugeMeasurement("es.blob_cache.miss.total").stream().mapToLong(Measurement::getLong).sum();
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
        long noCacheBypassCount = noCacheTelemetry.getLongCounterMeasurement(BlobCacheMetrics.BLOB_CACHE_BYPASS_READ_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat(noCacheBypassCount, greaterThan(0L));
        // Bypass reads count as both reads and misses; SUM across all timestamp buckets to get node-level totals.
        assertThat(collectAndSumReadTotal(noCacheTelemetry), equalTo(noCacheBypassCount));
        assertThat(collectAndSumMissTotal(noCacheTelemetry), equalTo(noCacheBypassCount));

        // Normal-cache node: reads and misses but no bypass reads
        final var normalCacheTelemetry = getTestTelemetryPlugin(normalCacheSearchNode);
        assertThat(collectAndSumReadTotal(normalCacheTelemetry), greaterThan(0L));
        assertThat(collectAndSumMissTotal(normalCacheTelemetry), greaterThan(0L));
        long normalCacheBypassCount = normalCacheTelemetry.getLongCounterMeasurement(BlobCacheMetrics.BLOB_CACHE_BYPASS_READ_TOTAL)
            .stream()
            .mapToLong(Measurement::getLong)
            .sum();
        assertThat(normalCacheBypassCount, equalTo(0L));
    }

    public void testSearchNodeOnlyPeriodicCacheMetrics() throws Exception {
        final var indexNode = startMasterAndIndexNode();
        final var searchNode = startSearchNode();

        // Ensure object not instantiated for indexing node so that we expect exception throwing
        expectThrows(Exception.class, () -> internalCluster().getInstance(StatelessSharedBlobCachePeriodicMetrics.class, indexNode));
        // It should be available on the search node
        internalCluster().getInstance(StatelessSharedBlobCachePeriodicMetrics.class, searchNode);

        updateClusterSettings(Settings.builder().put(StatelessSharedBlobCachePeriodicMetrics.METRICS_INTERVAL_SETTING.getKey(), "1s"));

        final var plugin = getTestTelemetryPlugin(searchNode);
        assertBusy(() -> {
            plugin.collect();
            final var gauge = plugin.getLongGaugeMeasurement(StatelessSharedBlobCachePeriodicMetrics.BLOB_CACHE_REGIONS_FILLED);
            assertNotNull(gauge);
            assertFalse(gauge.isEmpty());
        });
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
     * Verifies that cache read and miss counters are attributed to the correct
     * {@link TimeRangeBucket} label based on the {@code @timestamp} values in each index's
     * compound commit.
     *
     * <p>For each bucket a dedicated single-shard index is created whose documents carry a
     * {@code @timestamp} positioned in the middle of that bucket's age window. The compound
     * commit captures the {@code @timestamp} range; the search node stamps cache regions with
     * the range midpoint. After warming, the cache is evicted, a search is issued, and only the
     * matching bucket's read and miss counters should increase. A separate "other" index (no
     * {@code @timestamp} mapping) exercises the sentinel path where regions receive an
     * {@code UNKNOWN_TIMESTAMP} and therefore route to the {@code "other"} bucket.
     *
     * <p>Timing note: the FifteenMinutes bucket uses a 2-minute-old timestamp, leaving
     * 13 minutes of slack before the age crosses the 15-minute boundary. The full test is
     * expected to complete well within that window on any reasonable CI machine.
     */
    public void testTimestampAgeBuckets() {
        startMasterAndIndexNode();

        // Capture "now" once. Every per-bucket timestamp is derived from this reference so that
        // test-execution wall-clock drift cannot push a timestamp across a bucket boundary.
        final long nowMillis = System.currentTimeMillis();

        // For each TimeRangeBucket, create an index whose @timestamp sits at the midpoint of
        // that bucket's age window and index two segments' worth of data.
        record BucketCase(TimeRangeBucket bucket, String indexName) {}
        final List<BucketCase> cases = new ArrayList<>();
        for (TimeRangeBucket bucket : TimeRangeBucket.values()) {
            final String indexName = createTimestampedIndex(bucket.label().replace('_', '-'));
            final long docTimestampMillis = nowMillis - midpointAgeMillisForBucket(bucket);
            indexTimestampedSegments(indexName, docTimestampMillis);
            flush(indexName);
            cases.add(new BucketCase(bucket, indexName));
        }

        // "other" bucket: an index without @timestamp mapping — regions receive UNKNOWN_TIMESTAMP
        // sentinel (-1), which always routes to the "other" bucket regardless of node type.
        final String otherIndexName = "other-" + randomIdentifier();
        assertAcked(
            prepareCreate(
                otherIndexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            )
        );
        populateIndex(otherIndexName);
        flush(otherIndexName);

        // Start the search node. All indices were created with 1 replica, so the search node
        // recovers every shard immediately. SynchronousWarmingPlugin ensures warming completes
        // before recovery finishes, so by the time ensureGreen returns the cache is fully
        // pre-populated and there is no concurrency between background prefetch and our
        // evict-then-search assertions below.
        final String searchNode = startSearchNode();
        ensureStableCluster(2);
        final List<String> allIndices = new ArrayList<>();
        cases.forEach(c -> allIndices.add(c.indexName()));
        allIndices.add(otherIndexName);
        ensureGreen(allIndices.toArray(String[]::new));

        final TestTelemetryPlugin plugin = getTestTelemetryPlugin(searchNode);

        // For each time-range bucket: evict the search cache, capture current per-bucket counts,
        // issue a search, then assert both the read and miss counter for that bucket grew.
        for (final BucketCase bc : cases) {
            clearShardCache(findSearchShard(bc.indexName()));
            final long readsBefore = collectBucketTotal(plugin, "es.blob_cache.read.total", bc.bucket().label());
            final long missesBefore = collectBucketTotal(plugin, "es.blob_cache.miss.total", bc.bucket().label());

            executeSearch(bc.indexName());

            final long readsAfter = collectBucketTotal(plugin, "es.blob_cache.read.total", bc.bucket().label());
            final long missesAfter = collectBucketTotal(plugin, "es.blob_cache.miss.total", bc.bucket().label());
            assertThat(
                "read count for bucket '" + bc.bucket().label() + "' should increase after cache eviction + search",
                readsAfter,
                greaterThan(readsBefore)
            );
            assertThat(
                "miss count for bucket '" + bc.bucket().label() + "' should increase after cache eviction + search",
                missesAfter,
                greaterThan(missesBefore)
            );
        }

        // "other" bucket: evict + search and assert the "other" counter grew
        clearShardCache(findSearchShard(otherIndexName));
        final long otherReadsBefore = collectBucketTotal(plugin, "es.blob_cache.read.total", "other");
        final long otherMissesBefore = collectBucketTotal(plugin, "es.blob_cache.miss.total", "other");

        executeSearch(otherIndexName);

        final long otherReadsAfter = collectBucketTotal(plugin, "es.blob_cache.read.total", "other");
        final long otherMissesAfter = collectBucketTotal(plugin, "es.blob_cache.miss.total", "other");
        assertThat("read count for 'other' bucket should increase after eviction + search", otherReadsAfter, greaterThan(otherReadsBefore));
        assertThat(
            "miss count for 'other' bucket should increase after eviction + search",
            otherMissesAfter,
            greaterThan(otherMissesBefore)
        );
    }

    /**
     * Returns the age (in milliseconds) used as the {@code @timestamp} offset for a given bucket.
     * Each value is chosen to sit near the midpoint of the bucket's age window, giving ample slack
     * for test-execution time to pass without crossing a bucket boundary.
     */
    private static long midpointAgeMillisForBucket(TimeRangeBucket bucket) {
        return switch (bucket) {
            // (0, 15 min] — use 2 min; leaves 13 min slack before crossing the 15-min boundary
            case FifteenMinutes -> TimeUnit.MINUTES.toMillis(2);
            // (15 min, 1 hr] — use 37 min; comfortably between the two boundaries
            case OneHour -> TimeUnit.MINUTES.toMillis(37);
            // (1 hr, 12 hr] — use 6 hr; midpoint of the 11-hour window
            case TwelveHours -> TimeUnit.HOURS.toMillis(6);
            // (12 hr, 24 hr] — use 18 hr
            case OneDay -> TimeUnit.HOURS.toMillis(18);
            // (1 day, 3 days] — use 2 days
            case ThreeDays -> TimeUnit.DAYS.toMillis(2);
            // (3 days, 7 days] — use 5 days
            case SevenDays -> TimeUnit.DAYS.toMillis(5);
            // (7 days, 14 days] — use 10 days
            case FourteenDays -> TimeUnit.DAYS.toMillis(10);
            // > 14 days — use 20 days
            case OlderThan14Days -> TimeUnit.DAYS.toMillis(20);
        };
    }

    /**
     * Creates a single-shard, one-replica index with a {@code @timestamp} date field mapping.
     * The replica starts unassigned (no search node yet) and is allocated when a search node joins,
     * triggering synchronous warming before recovery completes. Automatic refresh is disabled so
     * each explicit {@link #refresh} call produces exactly one Lucene segment.
     */
    private String createTimestampedIndex(String namePrefix) {
        final String indexName = namePrefix + "-" + randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
            ).setMapping("@timestamp", "type=date")
        );
        return indexName;
    }

    /**
     * Indexes two batches of small documents into {@code indexName}, each batch followed by a
     * refresh (producing two Lucene segments). All documents carry {@code @timestamp =
     * timestampMillis} so the compound commit records a {@code TimestampFieldValueRange} whose
     * midpoint equals {@code timestampMillis}. The search node will stamp cache regions for this
     * index with that midpoint, routing reads and misses to the corresponding
     * {@link TimeRangeBucket}.
     */
    private void indexTimestampedSegments(final String indexName, final long timestampMillis) {
        for (int i = 0; i < 2; i++) {
            indexDocs(
                indexName,
                randomIntBetween(100, 300),
                UnaryOperator.identity(),
                null,
                () -> Map.of("@timestamp", timestampMillis, "field", randomAlphaOfLength(32))
            );
            refresh(indexName);
        }
    }

    /**
     * Resets the telemetry meter, triggers a fresh gauge collection, then returns the cumulative
     * total for the given metric filtered to the specified {@code bucketLabel}
     * ({@link BlobCacheMetrics#REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY} attribute value).
     */
    private static long collectBucketTotal(final TestTelemetryPlugin plugin, final String metricName, final String bucketLabel) {
        plugin.resetMeter();
        plugin.collect();
        return plugin.getLongGaugeMeasurement(metricName)
            .stream()
            .filter(m -> bucketLabel.equals(m.attributes().get(BlobCacheMetrics.REGION_TIMESTAMP_AGE_ATTRIBUTE_KEY)))
            .mapToLong(Measurement::getLong)
            .sum();
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
                    @Nullable Map<BlobFile, WarmTarget> endTargetsToWarm,
                    boolean preWarmForIdLookup,
                    ActionListener<Void> listener
                ) {
                    var subscribableListener = new SubscribableListener<Void>();
                    super.warmCache(type, indexShard, commit, directory, endTargetsToWarm, preWarmForIdLookup, subscribableListener);
                    safeAwait(subscribableListener);
                    subscribableListener.addListener(listener);
                }
            };
        }
    }
}
