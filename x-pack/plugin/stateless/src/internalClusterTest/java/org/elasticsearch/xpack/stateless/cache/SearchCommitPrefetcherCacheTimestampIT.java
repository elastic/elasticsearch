/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;

public class SearchCommitPrefetcherCacheTimestampIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(CapturingTestPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Full control over how and when VBCCs are uploaded to the blob store.
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueHours(12))
            // Online prewarming could create traffic in the prewarm pool and stamp regions ahead of the prefetcher.
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false)
            // Enough room to cache the data.
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(32))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            // Always prefetch data from the indexing node when required.
            .put(SearchCommitPrefetcher.PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING.getKey(), ByteSizeValue.ofGb(20))
            // Foreground prefetch so the prefetcher populates the cache deterministically, before the on-demand refresh.
            .put(SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING.getKey(), false)
            // Prefetch from the indexing node on the first notification (before the refresh opens the new segments on-demand), so the
            // prefetcher is the first to populate (and thus stamp) the new BCC blob's regions with the commit timestamp.
            .put(SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING.getKey(), true)
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), true)
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_SEARCH_IDLE_TIME_SETTING.getKey(), TimeValue.THIRTY_SECONDS)
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true);
    }

    public void testPrefetchStampsCacheRegionsWithCommitTimestamp() throws Exception {
        long timestamp = randomLongBetween(1, MAX_MILLIS_BEFORE_9999);
        startMasterAndIndexNode();
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1))
                .setMapping("@timestamp", "type=date")
        );
        ensureGreen(indexName);

        // Break the idle barrier so prefetching is not skipped: a real search acquires a searcher and sets the last-searcher-access time.
        assertNoFailures(prepareSearch(indexName));

        var shardId = new ShardId(resolveIndex(indexName), 0);

        // Index documents all carrying the same @timestamp, so the new commit's timestamp range is [T, T] and its midpoint is exactly T.
        indexDocs(
            indexName,
            between(100, 1000),
            UnaryOperator.identity(),
            null,
            () -> Map.<String, Object>of("@timestamp", timestamp, "field", randomAlphaOfLength(10))
        );
        refresh(indexName);
        flush(indexName);

        var latestCommitGeneration = client().admin().indices().prepareStats(indexName).get().getAt(0).getCommitStats().getGeneration();
        var bccBlobName = BatchedCompoundCommit.blobNameFromGeneration(latestCommitGeneration);

        var cacheService = (CapturingCacheService) internalCluster().getInstance(
            StatelessPlugin.SharedBlobCacheServiceSupplier.class,
            searchNode
        ).get();
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();
        var cacheKey = new FileCacheKey(shardId, primaryTerm, bccBlobName);

        assertBusy(() -> {
            var stamps = cacheService.capturedTimestamps(cacheKey);
            assertThat(
                "prefetch should have populated at least one region of the new BCC blob with the commit timestamp",
                stamps,
                hasItem(timestamp)
            );
            assertThat(
                "regions of the new BCC blob must only be stamped with the commit timestamp or UNKNOWN_TIMESTAMP",
                stamps,
                everyItem(anyOf(equalTo(timestamp), equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP)))
            );
        });
    }

    public static final class CapturingTestPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        public CapturingTestPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected StatelessSharedBlobCacheService createSharedBlobCacheService(
            NodeEnvironment nodeEnvironment,
            Settings settings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics,
            ClusterService clusterService,
            IndicesService indicesService
        ) {
            return new CapturingCacheService(nodeEnvironment, settings, threadPool, blobCacheMetrics);
        }
    }

    static final class CapturingCacheService extends StatelessSharedBlobCacheService {

        private final TimestampCapturingEvictionPolicy capturingPolicy;

        CapturingCacheService(NodeEnvironment environment, Settings settings, ThreadPool threadPool, BlobCacheMetrics blobCacheMetrics) {
            this(environment, settings, threadPool, blobCacheMetrics, new TimestampCapturingEvictionPolicy());
        }

        private CapturingCacheService(
            NodeEnvironment environment,
            Settings settings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics,
            TimestampCapturingEvictionPolicy capturingPolicy
        ) {
            super(
                environment,
                settings,
                threadPool,
                blobCacheMetrics,
                capturingPolicy,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            );
            this.capturingPolicy = capturingPolicy;
        }

        List<Long> capturedTimestamps(FileCacheKey cacheKey) {
            return capturingPolicy.capturedTimestamps(cacheKey);
        }
    }
}
