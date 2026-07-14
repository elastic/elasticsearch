/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class SearchShardRecoveryCacheTimestampBackfillIT extends AbstractStatelessPluginIntegTestCase {

    // A small region so a BCC blob spans several cache regions, exercising the multi-region metadata-read path.
    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(64);

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(CapturingTestPlugin.class);
        // Lets the index use index.merge.enabled=false so each flush's segment stays referenced across blobs (see below).
        plugins.add(InternalSettingsPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(disableIndexingDiskAndMemoryControllersNodeSettings())
            // Full control over how and when VBCCs are uploaded: with these bounds a VBCC is only uploaded when a flush forces it,
            // so each flush() below produces its own BCC blob (one compound commit per blob).
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(StatelessCommitService.STATELESS_UPLOAD_VBCC_MAX_AGE.getKey(), TimeValue.timeValueHours(12))
            // Keep the recovery-time metadata read the one that stamps (and backfills) the BCC regions: disable prewarming and any
            // prefetch so nothing stamps the regions with a real timestamp ahead of it.
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false)
            .put(SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING.getKey(), false)
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
            // Enough room to keep every region cached for the duration of the test (no eviction).
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(64))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE);
    }

    /** A single BCC blob's cache key and the data timestamp its regions should be backfilled with, read from the object store. */
    private record BlobInfo(FileCacheKey cacheKey, long dataTimestamp) {}

    /**
     * Indexes several flushes (each its own multi-region BCC blob, kept referenced by disabling merges) and verifies that a search
     * shard recovery backfills the cache-region timestamps of the BCC blobs it reads: every metadata read first stamps regions with
     * {@code UNKNOWN_TIMESTAMP} and the backfill then resolves them to the blob's real data timestamp.
     */
    public void testSearchShardRecoveryBackfillsMetadataReadRegions() throws Exception {
        var indexNode = startMasterAndIndexNode();
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    // Disable merges so each flush's segment is never merged away: the latest commit keeps referencing the segments
                    // written by every earlier flush, forcing recovery to read those earlier blobs too (the referenced-CCs path).
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            ).setMapping("@timestamp", "type=date")
        );

        int iterations = randomIntBetween(2, 4);
        for (int i = 0; i < iterations; i++) {
            long timestamp = randomLongBetween(1, MAX_MILLIS_BEFORE_9999);
            // Index enough sizeable documents (all sharing one @timestamp so the commit's range is [T, T], midpoint exactly T) that
            // the flush's segment - and therefore its BCC blob - spans several cache regions.
            indexDocs(
                indexName,
                between(500, 800),
                UnaryOperator.identity(),
                null,
                () -> Map.<String, Object>of("@timestamp", timestamp, "field", randomAlphaOfLength(1024))
            );
            refresh(indexName);
            flush(indexName);
        }

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();

        // Read each BCC blob's compound commit straight from the object store (this bypasses the shared cache, so it does not perturb
        // the search node's captured timestamps) to learn the data timestamp its regions should end up backfilled with.
        List<BlobInfo> blobs = new ArrayList<>();
        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var indexObjectStore = getObjectStoreService(indexNode);
        for (var blob : commitsContainer.listBlobs(operationPurpose).entrySet()) {
            var blobName = blob.getKey();
            if (StatelessCompoundCommit.startsWithBlobPrefix(blobName) == false) {
                continue;
            }
            var generation = StatelessCompoundCommit.parseGenerationFromBlobName(blobName);
            var iterator = indexObjectStore.readBatchedCompoundCommitFromStoreIncrementally(
                shardId,
                new PrimaryTermAndGeneration(primaryTerm, generation),
                blob.getValue()
            );
            var compoundCommit = iterator.next();
            // The timestamp the backfill resolves for the blob (see MetadataReadTimestampBackfill): the commit's midpoint, or
            // MINIMAL_TIMESTAMP when the commit has no timestamp range. Our docs all carry @timestamp, so this is the commit's value.
            long midpoint = BlobFileRanges.midpointMillisOrUnknownForCache(compoundCommit.getTimestampFieldValueRange());
            long dataTimestamp = midpoint != SharedBlobCacheService.UNKNOWN_TIMESTAMP ? midpoint : 1L;
            blobs.add(new BlobInfo(new FileCacheKey(shardId, primaryTerm, blobName), dataTimestamp));
        }
        assertThat("the test needs several referenced blobs to exercise the cross-blob backfill", blobs.size(), greaterThanOrEqualTo(2));
        var blobsByKey = blobs.stream().collect(Collectors.toMap(BlobInfo::cacheKey, b -> b));

        // Bring up the search node: the search shard recovers by reading the latest BCC and every referenced earlier BCC through the
        // cache, stamping their header regions UNKNOWN and then backfilling them.
        var searchNode = startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        var cacheService = (CapturingCacheService) internalCluster().getInstance(
            StatelessPlugin.SharedBlobCacheServiceSupplier.class,
            searchNode
        ).get();

        assertBusy(() -> {
            // The BCC blobs the recovery actually read (and thus stamped/backfilled): the latest one plus the referenced earlier ones.
            var readBlobKeys = cacheService.capturedKeys().stream().filter(blobsByKey::containsKey).collect(Collectors.toSet());
            assertThat(
                "recovery must read the latest BCC and at least one referenced earlier BCC",
                readBlobKeys.size(),
                greaterThanOrEqualTo(2)
            );

            for (var cacheKey : readBlobKeys) {
                var blob = blobsByKey.get(cacheKey);

                var captured = cacheService.capturedTimestamps(cacheKey);
                assertThat("recovery must have cached regions of blob " + cacheKey, captured, not(empty()));
                assertThat(
                    "a metadata read stamps regions with UNKNOWN_TIMESTAMP before they are backfilled",
                    captured,
                    hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP)
                );

                var live = cacheService.liveTimestamps(cacheKey);
                assertThat("recovery must leave live cache regions for blob " + cacheKey, live, not(empty()));
                assertThat("backfill must resolve the header region to the blob's data timestamp", live, hasItem(blob.dataTimestamp()));
            }
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
            return new CapturingCacheService(nodeEnvironment, settings, threadPool, blobCacheMetrics, indicesService);
        }
    }

    static final class CapturingCacheService extends StatelessSharedBlobCacheService {

        private final TimestampCapturingEvictionPolicy capturingPolicy;

        CapturingCacheService(
            NodeEnvironment environment,
            Settings settings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics,
            IndicesService indicesService
        ) {
            this(environment, settings, threadPool, blobCacheMetrics, indicesService, new TimestampCapturingEvictionPolicy());
        }

        private CapturingCacheService(
            NodeEnvironment environment,
            Settings settings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics,
            IndicesService indicesService,
            TimestampCapturingEvictionPolicy capturingPolicy
        ) {
            super(
                environment,
                settings,
                threadPool,
                blobCacheMetrics,
                capturingPolicy,
                indicesService,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            );
            this.capturingPolicy = capturingPolicy;
        }

        List<Long> capturedTimestamps(FileCacheKey cacheKey) {
            return capturingPolicy.capturedTimestamps(cacheKey);
        }

        List<Long> liveTimestamps(FileCacheKey cacheKey) {
            return capturingPolicy.liveTimestamps(cacheKey);
        }

        Set<FileCacheKey> capturedKeys() {
            return capturingPolicy.capturedKeys();
        }
    }
}
