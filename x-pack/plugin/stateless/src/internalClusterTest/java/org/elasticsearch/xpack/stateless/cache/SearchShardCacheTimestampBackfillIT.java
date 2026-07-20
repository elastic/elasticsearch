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
import org.elasticsearch.common.blobstore.BlobContainer;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class SearchShardCacheTimestampBackfillIT extends AbstractStatelessPluginIntegTestCase {

    // One page (minimum region size): small enough that modest docs span several regions without megabytes of indexed data.
    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(4);
    private static final int DOCS_PER_FLUSH = 40;
    private static final int DOC_FIELD_LENGTH = 64;

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
            // Keep the metadata read (during recovery or a new commit notification) the one that stamps (and backfills) the BCC regions:
            // disable prewarming and any prefetch so nothing stamps the regions with a real timestamp ahead of it.
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false)
            .put(SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING.getKey(), false)
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
            // Both recovery and new commit notifications backfill referenced BCC metadata-read regions only after parsing referenced CCs
            // via this path.
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            // Time-based caching (BACKFILL_IN_PROGRESS stamping + backfill) is only enabled when cache boost preference is on.
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            // Enough room to keep every region cached for the duration of the test (no eviction).
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(4))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), REGION_SIZE);
    }

    private void indexTimeBasedFlush(String indexName, long timestamp) throws Exception {
        indexDocs(
            indexName,
            DOCS_PER_FLUSH,
            UnaryOperator.identity(),
            null,
            () -> Map.<String, Object>of("@timestamp", timestamp, "field", randomAlphaOfLength(DOC_FIELD_LENGTH))
        );
        refresh(indexName);
        flush(indexName);
    }

    private void indexNonTimeBasedFlush(String indexName) throws Exception {
        indexDocs(indexName, DOCS_PER_FLUSH, UnaryOperator.identity(), null, () -> Map.of("field", randomAlphaOfLength(DOC_FIELD_LENGTH)));
        refresh(indexName);
        flush(indexName);
    }

    /**
     * Indexes several flushes (each its own multi-region BCC blob, kept referenced by disabling merges) and verifies that a search
     * shard recovery backfills the cache-region timestamps of the BCC blobs it reads: every metadata read first stamps regions with
     * {@code BACKFILL_IN_PROGRESS_TIMESTAMP} and the backfill then resolves them to the blob's real data timestamp.
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
            indexTimeBasedFlush(indexName, timestamp);
        }

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();

        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var blobs = readBlobInfosFromObjectStore(shardId, primaryTerm, indexNode, commitsContainer);
        assertThat("the test needs several referenced blobs to exercise the cross-blob backfill", blobs.size(), greaterThanOrEqualTo(2));
        var blobsByKey = blobs.stream().collect(Collectors.toMap(BlobInfo::cacheKey, b -> b));
        for (var blob : blobs) {
            assertThat(
                "every indexed doc carries @timestamp so backfill resolves to a real data timestamp",
                blob.dataTimestamp(),
                not(equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
            );
        }

        // Bring up the search node: the search shard recovers by reading the latest BCC and every referenced earlier BCC through the
        // cache, stamping their header regions BACKFILL_IN_PROGRESS and then backfilling them.
        var searchNode = startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        var cacheService = (CapturingCacheService) internalCluster().getInstance(
            StatelessPlugin.SharedBlobCacheServiceSupplier.class,
            searchNode
        ).get();

        assertBusy(() -> {
            var readBlobKeys = cacheService.capturedKeys().stream().filter(blobsByKey::containsKey).collect(Collectors.toSet());
            assertMetadataReadRegionsBackfilled(cacheService, blobsByKey, readBlobKeys);
        });
    }

    /**
     * Non-time-based indices (no {@code @timestamp} mapping) stamp metadata-read regions with {@code UNKNOWN_TIMESTAMP} and never
     * backfill them to a real data timestamp.
     */
    public void testSearchShardRecoveryLeavesNonTimeBasedRegionsUnknown() throws Exception {
        var indexNode = startMasterAndIndexNode();
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            ).setMapping("field", "type=keyword")
        );

        for (int i = 0; i < randomIntBetween(2, 4); i++) {
            indexNonTimeBasedFlush(indexName);
        }

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();

        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var blobKeys = listBccBlobKeys(shardId, primaryTerm, commitsContainer);
        assertThat("the test needs several referenced blobs", blobKeys.size(), greaterThanOrEqualTo(2));

        var searchNode = startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        var cacheService = (CapturingCacheService) internalCluster().getInstance(
            StatelessPlugin.SharedBlobCacheServiceSupplier.class,
            searchNode
        ).get();

        assertBusy(() -> {
            var readBlobKeys = blobKeys.stream()
                .filter(k -> cacheService.capturedTimestamps(k).isEmpty() == false)
                .collect(Collectors.toSet());
            assertThat("recovery must read at least two BCC blobs on the search node", readBlobKeys.size(), greaterThanOrEqualTo(2));

            for (var cacheKey : readBlobKeys) {
                var captured = cacheService.capturedTimestamps(cacheKey);
                assertThat("non-time-based metadata reads must have cached regions of blob " + cacheKey, captured, not(empty()));
                assertThat(
                    "non-time-based metadata reads must not stamp BACKFILL_IN_PROGRESS",
                    captured,
                    not(hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP))
                );
                assertThat(
                    "non-time-based metadata reads stamp UNKNOWN",
                    captured,
                    everyItem(equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
                );

                var live = cacheService.liveTimestamps(cacheKey);
                assertThat("non-time-based recovery must leave live cache regions for blob " + cacheKey, live, not(empty()));
                assertThat("non-time-based regions must stay UNKNOWN", live, everyItem(equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP)));
            }
        });
    }

    /**
     * With the search shard already recovered, indexes several flushes (each its own multi-region BCC blob) and verifies that each new
     * commit notification backfills the cache-region timestamps of the BCC blob it reads: the metadata read first stamps regions with
     * {@code BACKFILL_IN_PROGRESS_TIMESTAMP} and the backfill then resolves them to the blob's real data timestamp.
     */
    public void testNewCommitNotificationBackfillsMetadataReadRegions() throws Exception {
        var indexNode = startMasterAndIndexNode();
        var searchNode = startSearchNode();
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    // Disable merges so each flush's segment is never merged away and stays referenced by the latest commit.
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            ).setMapping("@timestamp", "type=date")
        );
        // The replica lands on the search node, which then receives a new commit notification for each flush below.
        ensureGreen(indexName);

        int iterations = randomIntBetween(2, 4);
        for (int i = 0; i < iterations; i++) {
            long timestamp = randomLongBetween(1, MAX_MILLIS_BEFORE_9999);
            indexTimeBasedFlush(indexName, timestamp);
        }

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();

        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var blobs = readBlobInfosFromObjectStore(shardId, primaryTerm, indexNode, commitsContainer);
        assertThat("the test needs several BCC blobs to exercise the per-notification backfill", blobs.size(), greaterThanOrEqualTo(2));
        var blobsByKey = blobs.stream().collect(Collectors.toMap(BlobInfo::cacheKey, b -> b));
        for (var blob : blobs) {
            assertThat(
                "every indexed doc carries @timestamp so backfill resolves to a real data timestamp",
                blob.dataTimestamp(),
                not(equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
            );
        }

        var cacheService = (CapturingCacheService) internalCluster().getInstance(
            StatelessPlugin.SharedBlobCacheServiceSupplier.class,
            searchNode
        ).get();

        assertBusy(() -> {
            var readBlobKeys = blobsByKey.keySet()
                .stream()
                .filter(k -> cacheService.capturedTimestamps(k).isEmpty() == false)
                .collect(Collectors.toSet());
            assertMetadataReadRegionsBackfilled(cacheService, blobsByKey, readBlobKeys);
        });
    }

    /** A single BCC blob's cache key and the data timestamp its regions should be backfilled with, read from the object store. */
    private record BlobInfo(FileCacheKey cacheKey, long dataTimestamp) {}

    private static List<FileCacheKey> listBccBlobKeys(ShardId shardId, long primaryTerm, BlobContainer commitsContainer)
        throws IOException {
        List<FileCacheKey> blobKeys = new ArrayList<>();
        for (var blob : commitsContainer.listBlobs(operationPurpose).entrySet()) {
            var blobName = blob.getKey();
            if (StatelessCompoundCommit.startsWithBlobPrefix(blobName)) {
                blobKeys.add(new FileCacheKey(shardId, primaryTerm, blobName));
            }
        }
        return blobKeys;
    }

    /**
     * Reads each BCC blob's CC header from the object store (bypassing the search node's shared cache) to learn the data timestamp its
     * cache regions should be backfilled with.
     */
    private List<BlobInfo> readBlobInfosFromObjectStore(ShardId shardId, long primaryTerm, String indexNode, BlobContainer commitsContainer)
        throws Exception {
        List<BlobInfo> blobs = new ArrayList<>();
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
            assertThat("Test assumes one CC per BCC", iterator.hasNext(), equalTo(false));
            long dataTimestamp = BlobFileRanges.midpointMillisOrUnknownForCache(compoundCommit.getTimestampFieldValueRange());
            blobs.add(new BlobInfo(new FileCacheKey(shardId, primaryTerm, blobName), dataTimestamp));
        }
        return blobs;
    }

    private static void assertMetadataReadRegionsBackfilled(
        CapturingCacheService cacheService,
        Map<FileCacheKey, BlobInfo> blobsByKey,
        Set<FileCacheKey> readBlobKeys
    ) {
        assertThat(readBlobKeys.size(), greaterThanOrEqualTo(2));

        for (var cacheKey : readBlobKeys) {
            var blob = blobsByKey.get(cacheKey);

            var captured = cacheService.capturedTimestamps(cacheKey);
            assertThat("metadata read must have cached regions of blob " + cacheKey, captured, not(empty()));
            assertThat(
                "a metadata read stamps regions with BACKFILL_IN_PROGRESS_TIMESTAMP before they are backfilled",
                captured,
                hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP)
            );

            var live = cacheService.liveTimestamps(cacheKey);
            assertThat("backfill must leave live cache regions for blob " + cacheKey, live, not(empty()));
            assertThat(
                "metadata read and warming must populate more than one cache region for blob " + cacheKey,
                live.size(),
                greaterThan(1)
            );
            assertThat("backfill must resolve regions to the blob's data timestamp", live, hasItem(blob.dataTimestamp()));
            assertThat(
                "backfill must leave no live region stamped BACKFILL_IN_PROGRESS_TIMESTAMP",
                live,
                not(hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP))
            );
        }
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

        List<Long> liveTimestamps(FileCacheKey cacheKey) {
            return capturingPolicy.liveTimestamps(cacheKey);
        }

        Set<FileCacheKey> capturedKeys() {
            return capturingPolicy.capturedKeys();
        }
    }
}
