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
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitCleaner;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.TestStatelessCommitService;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;
import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.elasticsearch.action.search.OnlinePrewarmingService;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.BlobCacheMetrics.CachePopulationReason;
import org.elasticsearch.blobcache.CachePopulationSource;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.PluginComponentBinding;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPoolStats;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static co.elastic.elasticsearch.stateless.cache.StatelessOnlinePrewarmingService.SEGMENT_PREWARMING_EXECUTION_WAITING_TIME_HISTOGRAM_NAME;
import static co.elastic.elasticsearch.stateless.cache.StatelessOnlinePrewarmingService.SHARD_TOOK_DURATION_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessOnlinePrewarmingIT extends AbstractStatelessIntegTestCase {

    public static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(16);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofMb(2);

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // we randomise the upload max size to test the production like case where the upload max size is slightly smaller than the
        // region size and the case where mutiple CCs are batched in a BCC exceeding the size of the region.
        ByteSizeValue uploadMaxSize = randomBoolean() ? ByteSizeValue.ofKb(14) : ByteSizeValue.ofMb(1);
        logger.info("-> upload max size: [{}]", uploadMaxSize);
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), true)
            // prefetching new commits will warm up new data so we want it disabled to avoid racing with prewarming
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
            .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), uploadMaxSize);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestCacheStatelessNoRecoveryPrewarming.class);
        plugins.add(MockRepository.Plugin.class);
        plugins.add(InternalSettingsPlugin.class);
        plugins.add(ShutdownPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        return plugins;
    }

    public void testShardPrewarming() throws Exception {
        startMasterOnlyNode();

        var cacheSettings = Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE.getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE)
            .build();
        startMasterAndIndexNode(cacheSettings);
        startSearchNode(cacheSettings);
        final String indexName = randomIdentifier();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                // disable merges so we can have some control over the segments structure
                .put(MergePolicyConfig.INDEX_MERGE_ENABLED, "false")
                .build()
        );
        ensureGreen(indexName);
        ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, DiscoveryNodeRole.SEARCH_ROLE);
        // this is the executor Lucene uses to fetch data from the object store in the cache in an on-demand manner
        // (e.g. when a reader is opened or when a search operation is executed)
        String shardReadThreadPool = Stateless.SHARD_READ_THREAD_POOL;
        // let's get the number of completed tasks before we start indexing so when we wait for the downloads to finish
        // we can assert that the number of completed tasks is higher, to make sure downloads actually occurred
        long preRefreshCompletedDownloadTasks = getNumberOfCompletedTasks(threadPool, shardReadThreadPool);
        long preRefreshCompletedRefreshTasks = getNumberOfCompletedTasks(threadPool, ThreadPool.Names.REFRESH);
        for (int i = 0; i < 20; i++) {
            indexDocs(indexName, 1000);
            if (i % 2 == 0) {
                // note that we open a reader on the search side when we refresh. opening a reader will read some
                // segments and warm them up in the cache so we need to wait for the reads triggered by the refresh to complete before
                // we can assert on the warmed bytes
                refresh(indexName);
            }
        }
        flush(indexName);
        assertNoRunningAndQueueTasks(threadPool, ThreadPool.Names.REFRESH, preRefreshCompletedRefreshTasks);
        assertNoRunningAndQueueTasks(threadPool, shardReadThreadPool, preRefreshCompletedDownloadTasks);

        IndexShard indexShard = findSearchShard(indexName);
        var searchDirectory = SearchDirectory.unwrapDirectory(indexShard.store().directory());
        StatelessSharedBlobCacheService cacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);
        // clear the cache to make sure prewarming doesn't race with readers opening
        cacheService.forceEvict(key -> true);
        SharedBlobCacheService.Stats statsBeforePrewarming = cacheService.getStats();

        long bytesWarmedBeforePrewarming = searchDirectory.totalBytesWarmed();
        StatelessOnlinePrewarmingService onlinePrewarmingService = (StatelessOnlinePrewarmingService) internalCluster().getInstance(
            OnlinePrewarmingService.class,
            DiscoveryNodeRole.SEARCH_ROLE
        );
        PlainActionFuture<Void> prewarmingFuture = new PlainActionFuture<>();
        onlinePrewarmingService.prewarm(indexShard, prewarmingFuture);
        prewarmingFuture.get(10, TimeUnit.SECONDS);
        SharedBlobCacheService.Stats statsAfterFirstPrewarm = cacheService.getStats();
        logger.info("-> stats before prewarming: [{}]", statsBeforePrewarming);
        logger.info(
            "-> before first prewarming: warmed bytes in search directory [{}] and written bytes in cache stats [{}]",
            bytesWarmedBeforePrewarming,
            statsBeforePrewarming.writeBytes()
        );
        logger.info("-> stats after prewarming: [{}]", statsAfterFirstPrewarm);
        long bytesWarmedAfterFirstPrewarming = searchDirectory.totalBytesWarmed();
        logger.info(
            "-> after first prewarming: warmed bytes in search directory [{}] and written bytes in cache stats [{}]",
            bytesWarmedAfterFirstPrewarming,
            statsAfterFirstPrewarm.writeBytes()
        );
        assertThat(statsAfterFirstPrewarm.writeCount(), greaterThan(statsBeforePrewarming.writeCount()));
        assertThat(statsAfterFirstPrewarm.writeBytes() - statsBeforePrewarming.writeBytes(), greaterThan(0L));

        // prewarming does not count as reads and misses
        assertThat(statsAfterFirstPrewarm.missCount(), equalTo(statsBeforePrewarming.missCount()));
        assertThat(statsAfterFirstPrewarm.readCount(), equalTo(statsBeforePrewarming.readCount()));

        assertThat(bytesWarmedAfterFirstPrewarming - bytesWarmedBeforePrewarming, is(greaterThan(0L)));

        TestTelemetryPlugin testTelemetryPlugin = internalCluster().getInstance(PluginsService.class, DiscoveryNodeRole.SEARCH_ROLE)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        List<Measurement> shardTookTimes = testTelemetryPlugin.getLongHistogramMeasurement(SHARD_TOOK_DURATION_HISTOGRAM_NAME);
        List<Measurement> segmentsPrewarmingWaitTimes = testTelemetryPlugin.getLongHistogramMeasurement(
            SEGMENT_PREWARMING_EXECUTION_WAITING_TIME_HISTOGRAM_NAME
        );
        // we should be recording telemetry for online prewarming
        assertThat(shardTookTimes.size(), is(greaterThan(0)));
        assertThat(segmentsPrewarmingWaitTimes.size(), is(greaterThan(0)));

        // second prewarming call should be a no-op as we prewarmed everything already
        PlainActionFuture<Void> secondPrewarmingFuture = new PlainActionFuture<>();
        onlinePrewarmingService.prewarm(indexShard, secondPrewarmingFuture);
        secondPrewarmingFuture.get(10, TimeUnit.SECONDS);

        long bytesWarmedAfterSecondPrewarming = searchDirectory.totalBytesWarmed();
        // no more bytes warmed as the shard was already prewarmed and no more writes have been executed
        assertThat(bytesWarmedAfterSecondPrewarming, is(bytesWarmedAfterFirstPrewarming));

        long downloadTasksAfterPrewarming = getNumberOfCompletedTasks(threadPool, shardReadThreadPool);
        long refreshTasksAfterPrewarming = getNumberOfCompletedTasks(threadPool, ThreadPool.Names.REFRESH);
        // let's create some more segments and trigger prewarming via a search operation
        for (int i = 0; i < 5; i++) {
            indexDocs(indexName, 10_000);
            refresh(indexName);
        }
        flush(indexName);
        assertNoRunningAndQueueTasks(threadPool, ThreadPool.Names.REFRESH, refreshTasksAfterPrewarming);
        assertNoRunningAndQueueTasks(threadPool, shardReadThreadPool, downloadTasksAfterPrewarming);

        logger.info("-> searching index after additional indexing");
        // clear the cache to make sure prewarming doesn't race with readers opening
        cacheService.forceEvict(key -> true);
        // trigger online prewarming via search operation
        assertResponse(prepareSearch(indexName), response -> assertThat(response.getHits().getTotalHits().value(), is(10_000L)));
        // we expect more bytes to have been warmed for the new segments
        assertBusy(() -> assertThat(searchDirectory.totalBytesWarmed() - bytesWarmedAfterSecondPrewarming, is(greaterThan(0L))));

        logger.info("-> checking telemetry after search prewarming");
        // clear the metrics collected so far
        testTelemetryPlugin.resetMeter();

        // evict everything from the cache
        cacheService.forceEvict(key -> true);
        long bytesWarmedBeforeSearchRequest = searchDirectory.totalBytesWarmed();
        // assert appropriate cache-miss metrics are published when searching
        assertResponse(prepareSearch(indexName), response -> assertThat(response.getHits().getTotalHits().value(), is(10_000L)));
        // wait for some prewarming to complete (it executes in parallel with the search operation)
        assertBusy(() -> assertThat(searchDirectory.totalBytesWarmed() - bytesWarmedAfterSecondPrewarming, is(greaterThan(0L))));
        // There is at least one `population.throughput.histogram` measurement
        CachePopulationReason cachePopulationReason = CachePopulationReason.OnlinePrewarming;
        CachePopulationSource cachePopulationSource = CachePopulationSource.BlobStore;
        assertContainsMeasurement(
            testTelemetryPlugin.getDoubleHistogramMeasurement("es.blob_cache.population.throughput.histogram"),
            cachePopulationReason,
            cachePopulationSource
        );

        // There is at least one `population.bytes.total` measurement
        assertContainsMeasurement(
            testTelemetryPlugin.getLongCounterMeasurement("es.blob_cache.population.bytes.total"),
            cachePopulationReason,
            cachePopulationSource
        );

        // There is at least one `population.time.total` measurement
        assertContainsMeasurement(
            testTelemetryPlugin.getLongCounterMeasurement("es.blob_cache.population.time.total"),
            cachePopulationReason,
            cachePopulationSource
        );
    }

    private static long getNumberOfCompletedTasks(ThreadPool threadPool, String shardReadThreadPool) {
        final ThreadPoolStats.Stats stats = threadPool.stats()
            .stats()
            .stream()
            .filter(s -> s.name().equals(shardReadThreadPool))
            .findFirst()
            .orElseThrow();
        return stats.completed();
    }

    private static void assertNoRunningAndQueueTasks(ThreadPool threadPool, String executorName, long previouslyObservedCompletedTasks)
        throws Exception {
        assertBusy(() -> {
            final ThreadPoolStats.Stats stats = threadPool.stats()
                .stats()
                .stream()
                .filter(s -> s.name().equals(executorName))
                .findFirst()
                .orElse(null);
            assertThat(stats, is(notNullValue()));
            assertThat(stats.completed(), greaterThan(previouslyObservedCompletedTasks));
            assertThat(stats.active() + stats.queue(), is(0));
        });
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

    public static final class TestCacheStatelessNoRecoveryPrewarming extends Stateless {

        public TestCacheStatelessNoRecoveryPrewarming(Settings settings) {
            super(settings);
        }

        @Override
        public Collection<Object> createComponents(Plugin.PluginServices services) {
            final Collection<Object> components = super.createComponents(services);
            components.add(
                new PluginComponentBinding<>(
                    StatelessCommitService.class,
                    components.stream().filter(c -> c instanceof TestStatelessCommitService).findFirst().orElseThrow()
                )
            );
            return components;
        }

        @Override
        protected StatelessCommitService createStatelessCommitService(
            Settings settings,
            ObjectStoreService objectStoreService,
            ClusterService clusterService,
            IndicesService indicesService,
            Client client,
            StatelessCommitCleaner commitCleaner,
            StatelessSharedBlobCacheService cacheService,
            SharedBlobCacheWarmingService cacheWarmingService,
            TelemetryProvider telemetryProvider
        ) {
            return new TestStatelessCommitService(
                settings,
                objectStoreService,
                clusterService,
                indicesService,
                client,
                commitCleaner,
                cacheService,
                cacheWarmingService,
                telemetryProvider
            );
        }

        @Override
        protected SharedBlobCacheWarmingService createSharedBlobCacheWarmingService(
            StatelessSharedBlobCacheService cacheService,
            ThreadPool threadPool,
            TelemetryProvider telemetryProvider,
            Settings settings
        ) {
            // no-op the warming on shard recovery so we can manually fetch ranges into the cache on the search tier
            return new SharedBlobCacheWarmingService(cacheService, threadPool, telemetryProvider, settings) {
                @Override
                public void warmCacheForShardRecovery(
                    Type type,
                    IndexShard indexShard,
                    StatelessCompoundCommit commit,
                    BlobStoreCacheDirectory directory
                ) {}
            };
        }
    }
}
