/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.test.ESIntegTestCase.assertBusy;
import static org.elasticsearch.test.ESTestCase.assertTrue;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

/**
 * Search-only shard recovery with a 4kb shared-cache region so the compound commit spans many regions. Concurrent searches run while
 * (1) a shard relocates to another search node, or (2) the index grows from one replica to two and the new replica allocates on a
 * search node that previously had no copy (another active copy exists elsewhere, so recovery uses the non-relocation warming path).
 * Telemetry on the target is zeroed while search threads are still blocked on a {@link CyclicBarrier} (so no concurrent search runs
 * before the baseline). After recovery finishes and the search threads stop, we assert
 * {@code es.blob_cache.miss_that_triggered_read.total} summed for the {@code search} executor only is zero on the target. Other
 * executors (e.g. prewarm) can still populate the cache from the object store during recovery; this assertion targets misses on the
 * search request path concurrent with recovery.
 * <p>
 * {@link SearchShardCacheWarmingITPlugin} registers {@link DefaultWarmingRatioProviderFactory#SEARCH_RECOVERY_WARMING_RATIO_SETTING}
 * (not registered on {@link org.elasticsearch.xpack.stateless.StatelessPlugin}). This test sets it to {@code 1.0} so the full compound
 * commit is warmed during recovery BCC warming.
 * Use sufficient test heap for several nodes, e.g. {@code -Dtests.heap.size=1g}.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class SearchShardCacheWarmingIT extends AbstractStatelessPluginIntegTestCase {

    private static final int NUM_DOCS = 800;
    private static final int FIELD_LENGTH = 350;

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(SearchShardCacheWarmingITPlugin.class);
        plugins.add(TestTelemetryPlugin.class);
        plugins.add(MockRepository.Plugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        Settings.Builder builder = super.nodeSettings().put(
            ObjectStoreService.TYPE_SETTING.getKey(),
            ObjectStoreService.ObjectStoreType.MOCK
        )
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(DefaultWarmingRatioProviderFactory.SEARCH_RECOVERY_WARMING_RATIO_SETTING.getKey(), 1.0d)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
        // only set if set by super, to verify default is on.
        if (builder.keys().contains(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey())) {
            builder.put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), true);
        }
        return builder;
    }

    /** Applied only on search nodes (master nodes cannot set shared cache size). */
    private static Settings searchNodeCacheSettings() {
        return Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(32).getStringRep())
            // Minimal region size so the compound commit spans many regions and multi-region warming is exercised.
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4).getStringRep())
            .build();
    }

    /**
     * Same as {@link #searchNodeCacheSettings()} but with a tiny non-relocation recovery warming timeout so the race against
     * {@link SharedBlobCacheWarmingService#searchRecoveryWarmingListener} reliably completes before warming when prewarm is stalled.
     */
    private static Settings searchNodeCacheSettingsWithFastNonRelocationWarmingTimeout() {
        return Settings.builder()
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(32).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4).getStringRep())
            .put(SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofKb(4).getStringRep())
            // this also enables blocking warmCache, see SearchShardCacheWarmingITPlugin.DelayWarmCacheUntilShardStartedService
            .put(
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING.getKey(),
                TimeValue.timeValueMillis(1)
            )
            .build();
    }

    public void testConcurrentSearchWhileRelocatingDoesNotMissBlobCacheOnTarget() throws Exception {
        TwoSearchNodes nodes = startTwoSearchNodeCluster();
        String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                indexSettings(1, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", nodes.searchNodeB)
            )
        );
        ensureGreen(indexName);
        indexSearchHeavyDocumentsAndMerge(indexName);
        final TestTelemetryPlugin telemetryOnB = getTelemetryPlugin(nodes.searchNodeB);
        ConcurrentSearchLoad searchLoad = startConcurrentSearchLoad(indexName);

        telemetryOnB.resetMeter();
        searchLoad.unblockSearchWorkers();
        updateIndexSettings(
            Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", nodes.searchNodeA),
            indexName
        );

        assertBusy(() -> {
            assertThat(internalCluster().nodesInclude(indexName), not(hasItem(nodes.searchNodeA)));
            assertThat(internalCluster().nodesInclude(indexName), hasItem(nodes.searchNodeB));
        });

        ensureGreen(indexName);

        searchLoad.stopAndShutdown();

        assertDocCountAndTelemetryMissesZero(
            indexName,
            telemetryOnB,
            "No blob-cache read misses on the search executor on the relocation target during recovery + concurrent search "
                + "(recovery_warming_ratio 1.0)"
        );
    }

    /**
     * One primary and one replica, then {@code number_of_replicas} raised to 2 while clearing allocation exclude so a third copy can
     * allocate on a search node that had no shard. That node is the telemetry target: recovery is not a relocation, but another active
     * copy exists, so {@link SharedBlobCacheWarmingService#searchRecoveryTimeout} awaits with the non-relocation timeout.
     */
    public void testConcurrentSearchWhileAddingSecondReplicaDoesNotMissBlobCacheOnTarget() throws Exception {
        TwoSearchNodes nodes = startTwoSearchNodeCluster();
        String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                indexSettings(1, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", nodes.searchNodeB)
            )
        );
        ensureGreen(indexName);
        indexSearchHeavyDocumentsAndMerge(indexName);
        final TestTelemetryPlugin telemetryOnB = getTelemetryPlugin(nodes.searchNodeB);
        ConcurrentSearchLoad searchLoad = startConcurrentSearchLoad(indexName);

        telemetryOnB.resetMeter();
        searchLoad.unblockSearchWorkers();
        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"),
            indexName
        );

        assertBusy(() -> assertThat(internalCluster().nodesInclude(indexName), hasItem(nodes.searchNodeB)));

        ensureGreen(indexName);

        searchLoad.stopAndShutdown();

        assertDocCountAndTelemetryMissesZero(
            indexName,
            telemetryOnB,
            "No blob-cache read misses on the search executor on the new replica's node during recovery + concurrent search "
                + "(recovery_warming_ratio 1.0)"
        );
    }

    /**
     * With {@link SearchCommitPrefetcherDynamicSettings#STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT} enabled, search recovery
     * warming races against {@link SharedBlobCacheWarmingService#SEARCH_RECOVERY_WARMING_TIMEOUT_NON_RELOCATION_SETTING}. {@link
     * SearchShardCacheWarmingITPlugin} defers {@link SharedBlobCacheWarmingService#warmCache} body until {@link IndexShardState#STARTED} on
     * the internal-files path so warming can stay “behind” recovery without starving the prewarm pool; the timeout must fire and the shard
     * must still reach {@code STARTED}.
     */
    public void testSearchRecoveryContinuesWhenWarmingTimesOutWithDeferredWarmCache() throws Exception {
        TwoSearchNodes nodes = startTwoSearchNodeCluster(searchNodeCacheSettingsWithFastNonRelocationWarmingTimeout());
        String indexName = randomIdentifier();
        assertAcked(
            prepareCreate(
                indexName,
                indexSettings(1, 1).put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", nodes.searchNodeB)
            )
        );
        ensureGreen(indexName);
        indexSearchHeavyDocumentsAndMerge(indexName);

        final TestTelemetryPlugin telemetryOnB = getTelemetryPlugin(nodes.searchNodeB);
        telemetryOnB.resetMeter();

        updateIndexSettings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2)
                .putNull(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name"),
            indexName
        );

        ensureGreen(indexName);
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(0), NUM_DOCS);
        telemetryOnB.collect();
        assertThat(
            telemetryOnB.getLongCounterMeasurement("es.blob_cache.miss_that_triggered_read.total")
                .stream()
                .mapToLong(Measurement::getLong)
                .sum(),
            greaterThan(0L)
        );

    }

    /** Master, index node, and two search nodes with shared cache settings. */
    private TwoSearchNodes startTwoSearchNodeCluster() {
        return startTwoSearchNodeCluster(searchNodeCacheSettings());
    }

    private TwoSearchNodes startTwoSearchNodeCluster(Settings searchCacheSettings) {
        startMasterAndIndexNode();
        String searchNodeA = startSearchNode(searchCacheSettings);
        String searchNodeB = startSearchNode(searchCacheSettings);
        ensureStableCluster(3);
        return new TwoSearchNodes(searchNodeA, searchNodeB);
    }

    private record TwoSearchNodes(String searchNodeA, String searchNodeB) {}

    /**
     * Fixed pool of threads issuing concurrent searches until {@link #stopAndShutdown()}. The test thread calls
     * {@link #unblockSearchWorkers()} after baselining telemetry so workers and the test proceed together.
     */
    private static final class ConcurrentSearchLoad {
        private final CyclicBarrier startBarrier;
        private final AtomicBoolean stopSearching;
        private final ExecutorService executor;
        private final List<Future<?>> futures;

        ConcurrentSearchLoad(CyclicBarrier startBarrier, AtomicBoolean stopSearching, ExecutorService executor, List<Future<?>> futures) {
            this.startBarrier = startBarrier;
            this.stopSearching = stopSearching;
            this.executor = executor;
            this.futures = futures;
        }

        /** Unblocks search worker threads and returns once they have all passed the start barrier (with this caller as the extra party). */
        void unblockSearchWorkers() throws Exception {
            startBarrier.await();
        }

        void stopAndShutdown() throws Exception {
            stopSearching.set(true);
            for (Future<?> f : futures) {
                safeGet(f);
            }
            executor.shutdown();
            assertTrue(executor.awaitTermination(SAFE_AWAIT_TIMEOUT.seconds(), TimeUnit.SECONDS));
        }
    }

    private ConcurrentSearchLoad startConcurrentSearchLoad(String indexName) {
        int concurrentSearchers = between(1, 4);
        CyclicBarrier startBarrier = new CyclicBarrier(concurrentSearchers + 1);
        AtomicBoolean stopSearching = new AtomicBoolean(false);
        ExecutorService executor = Executors.newFixedThreadPool(concurrentSearchers);
        List<Future<?>> futures = new ArrayList<>(concurrentSearchers);
        for (int t = 0; t < concurrentSearchers; t++) {
            futures.add(executor.submit(() -> {
                startBarrier.await();
                while (stopSearching.get() == false) {
                    try {
                        SearchResponse response = client().prepareSearch(indexName)
                            .setQuery(QueryBuilders.matchAllQuery())
                            .setSize(50)
                            .setAllowPartialSearchResults(false)
                            .get();
                        try {
                            // response body unused; release refcounted transport buffers.
                        } finally {
                            response.decRef();
                        }
                    } catch (Exception e) {
                        // During recovery the search shard may briefly be unavailable; keep polling.
                    }
                }
                return null;
            }));
        }
        return new ConcurrentSearchLoad(startBarrier, stopSearching, executor, futures);
    }

    /** Indexes {@link #NUM_DOCS} documents and merges to one segment for a predictable compound-commit layout. */
    private void indexSearchHeavyDocumentsAndMerge(String indexName) throws Exception {
        BulkRequestBuilder bulk = client().prepareBulk();
        for (int i = 0; i < NUM_DOCS; i++) {
            bulk.add(new IndexRequest(indexName).source("field", randomAlphaOfLength(FIELD_LENGTH)));
        }
        assertNoFailures(bulk.setRefreshPolicy(IMMEDIATE).get());
        flush(indexName);
        refresh(indexName);
        assertNoFailures(client().admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get());
        ensureGreen(indexName);
    }

    private void assertDocCountAndTelemetryMissesZero(String indexName, TestTelemetryPlugin telemetry, String message) {
        assertHitCount(client().prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setSize(0), NUM_DOCS);
        assertSearchExecutorBlobCacheMissesZero(telemetry, message);
    }

    private void assertSearchExecutorBlobCacheMissesZero(TestTelemetryPlugin telemetry, String message) {
        telemetry.collect();
        assertThat(message, missThatTriggeredReadTotalForSearchExecutor(telemetry), equalTo(0L));
    }

    private static long missThatTriggeredReadTotalForSearchExecutor(TestTelemetryPlugin telemetry) {
        return telemetry.getLongCounterMeasurement("es.blob_cache.miss_that_triggered_read.total")
            .stream()
            .filter(m -> ThreadPool.Names.SEARCH.equals(m.attributes().get(BlobCacheMetrics.ES_EXECUTOR_ATTRIBUTE_KEY)))
            .mapToLong(Measurement::getLong)
            .sum();
    }
}
