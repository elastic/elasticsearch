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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
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

    private ByteSizeValue cacheRegionSize = REGION_SIZE;

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
            // Full control over how and when VBCCs are uploaded: with these bounds a VBCC is only uploaded when a flush forces it (never by
            // size or age), so each test decides how many compound commits a blob holds by how many refreshes it does between flushes.
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
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(16))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), cacheRegionSize)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), cacheRegionSize)
            .put(SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), cacheRegionSize);
    }

    /// Indexes several flushes (each its own multi-region BCC blob, kept referenced by disabling merges) and verifies that a search
    /// shard recovery backfills the cache-region timestamps of the BCC blobs it reads: every metadata read first stamps regions with
    /// `BACKFILL_IN_PROGRESS_TIMESTAMP` and the backfill then resolves them to the blob's real data timestamp.
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
            indexTimestampedDocs(indexName, randomLongBetween(1, MAX_MILLIS_BEFORE_9999));
            flush(indexName);
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

    /// A single BCC blob can hold several compound commits with mixed timestamp availability (some CCs carry `@timestamp`, some do not).
    /// When recovery reads such a blob, its metadata-read region (stamped `BACKFILL_IN_PROGRESS_TIMESTAMP`) is backfilled with a single
    /// per-blob value: the most-recent known midpoint across all the blob's CCs. CCs without a `@timestamp` are skipped, and the sentinel
    /// never survives as long as one CC carries a timestamp.
    public void testRecoveryBackfillsMultiCcBlobWithSingleMostRecentTimestamp() throws Exception {
        // Larger than any blob below, so each blob occupies exactly one cache region. Needed as capturing policy is per key, not region.
        cacheRegionSize = ByteSizeValue.ofMb(1);
        var indexNode = startMasterAndIndexNode();
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
                    // Keep the accumulation deterministic: a high translog flush threshold prevents a size-triggered (non-by-refresh) flush
                    // from uploading the VBCC mid-way, so only the explicit flush() below packs all the refreshed CCs into one blob.
                    .put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), ByteSizeValue.ofGb(1))
            ).setMapping("@timestamp", "type=date")
        );

        // Accumulate several compound commits into one VBCC (each refresh appends a CC without uploading) and only then flush() to upload
        // them together. The CCs carry mixed timestamp availability - a known timestamp, then no @timestamp (UNKNOWN), then another known
        // one - so a blob holding more than one of them exercises the mixed-availability fold.
        long higherTimestamp = randomLongBetween(MAX_MILLIS_BEFORE_9999 / 2 + 1, MAX_MILLIS_BEFORE_9999);
        long lowerTimestamp = randomLongBetween(1, MAX_MILLIS_BEFORE_9999 / 2);
        indexTimestampedDocs(indexName, higherTimestamp);
        indexUntimestampedDocs(indexName);
        indexTimestampedDocs(indexName, lowerTimestamp);
        flush(indexName);

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();

        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        // An async flush can occasionally freeze the VBCC mid-sequence, so the CCs may land in more than one blob; the blob with the most
        // CCs is the multi-CC blob that recovery reads and backfills. A single split of the ordered CCs still leaves a prefix/suffix with
        // both a known and an UNKNOWN timestamp, which is all this test needs.
        var multiCcBlob = readBlobInfosFromObjectStore(shardId, primaryTerm, indexNode, commitsContainer).stream()
            .max(Comparator.comparingInt(blob -> blob.ccMidpoints().size()))
            .orElseThrow();
        assertThat("the test must pack several CCs into one blob", multiCcBlob.ccMidpoints().size(), greaterThanOrEqualTo(2));
        var blobKey = multiCcBlob.cacheKey();

        assertThat(
            "the blob must mix a known and an UNKNOWN CC so the mixed-availability fold is exercised",
            multiCcBlob.ccMidpoints(),
            hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP)
        );
        final long expectedTimestamp = multiCcBlob.dataTimestamp();
        assertThat(
            "some CCs carry @timestamp so the fold must resolve to a real data timestamp, not UNKNOWN",
            expectedTimestamp,
            not(equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
        );

        var searchNode = startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        var cacheService = (CapturingCacheService) internalCluster().getInstance(
            StatelessPlugin.SharedBlobCacheServiceSupplier.class,
            searchNode
        ).get();

        assertBusy(() -> {
            var captured = cacheService.capturedTimestamps(blobKey);
            assertThat("recovery must have cached the multi-CC blob's region", captured, not(empty()));
            assertThat(
                "the metadata read stamps the region with BACKFILL_IN_PROGRESS_TIMESTAMP before it is backfilled",
                captured,
                hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP)
            );

            // The blob is a single region, so its only live timestamp must be the backfilled per-blob value: the most-recent known midpoint
            // across the blob's CCs.
            var live = cacheService.liveTimestamps(blobKey);
            assertThat("recovery must leave the multi-CC blob's region live", live, not(empty()));
            assertThat(
                "the blob's single region must be backfilled to the most-recent known midpoint",
                live,
                everyItem(equalTo(expectedTimestamp))
            );
        });
    }

    /// Non-time-based indices (no `@timestamp` mapping) stamp metadata-read regions with `UNKNOWN_TIMESTAMP` and never
    /// backfill them to a real data timestamp.
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
            indexUntimestampedDocs(indexName);
            flush(indexName);
        }

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();

        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var blobKeys = readBlobInfosFromObjectStore(shardId, primaryTerm, indexNode, commitsContainer).stream()
            .map(BlobInfo::cacheKey)
            .toList();
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

    /// The time-based counterpart of [#testSearchShardRecoveryLeavesNonTimeBasedRegionsUnknown]: a time-based index (with a `@timestamp`
    /// mapping) whose documents carry no `@timestamp` value. Every compound commit then has no timestamp range (its cache midpoint is
    /// UNKNOWN), but because the index is time-based recovery still stamps the metadata-read regions `BACKFILL_IN_PROGRESS_TIMESTAMP` and
    /// the backfill resolves them to `MINIMAL_CACHE_TIMESTAMP` - the floor used when no real data timestamp is available - rather than
    /// leaving them UNKNOWN.
    ///
    /// The cache region is sized so each blob is a single region (its metadata-read region), isolating the backfilled value from the
    /// UNKNOWN that individual data-file regions would otherwise carry in a multi-region blob.
    public void testSearchShardRecoveryBackfillsUntimestampedTimeBasedRegionsToMinimal() throws Exception {
        cacheRegionSize = ByteSizeValue.ofMb(1); // larger than any blob below, so each blob occupies exactly one cache region
        var indexNode = startMasterAndIndexNode();
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    // Disable merges so each flush's segment stays referenced by the latest commit, forcing recovery to read every blob.
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            ).setMapping("@timestamp", "type=date")
        );

        for (int i = 0; i < randomIntBetween(2, 4); i++) {
            indexUntimestampedDocs(indexName);
            flush(indexName);
        }

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();

        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);
        var blobs = readBlobInfosFromObjectStore(shardId, primaryTerm, indexNode, commitsContainer);
        assertThat("the test needs several referenced blobs", blobs.size(), greaterThanOrEqualTo(2));
        var blobKeys = blobs.stream().map(BlobInfo::cacheKey).collect(Collectors.toSet());
        for (var blob : blobs) {
            assertThat(
                "no indexed doc carries @timestamp so every CC's cache midpoint is UNKNOWN",
                blob.ccMidpoints(),
                everyItem(equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
            );
        }

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
                assertThat("time-based metadata reads must have cached the region of blob " + cacheKey, captured, not(empty()));
                assertThat(
                    "a time-based index stamps metadata reads BACKFILL_IN_PROGRESS even when the CCs have no @timestamp",
                    captured,
                    hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP)
                );

                var live = cacheService.liveTimestamps(cacheKey);
                assertThat("backfill must leave the region of blob " + cacheKey + " live", live, not(empty()));
                assertThat(
                    "with no data timestamp available the region is floored to MINIMAL_CACHE_TIMESTAMP, not left UNKNOWN",
                    live,
                    everyItem(equalTo(SharedBlobCacheService.MINIMAL_CACHE_TIMESTAMP))
                );
            }
        });
    }

    /// With the search shard already recovered, indexes several flushes (each its own multi-region BCC blob) and verifies that each new
    /// commit notification backfills the cache-region timestamps of the BCC blob it reads: the metadata read first stamps regions with
    /// `BACKFILL_IN_PROGRESS_TIMESTAMP` and the backfill then resolves them to the blob's real data timestamp.
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
            indexTimestampedDocs(indexName, randomLongBetween(1, MAX_MILLIS_BEFORE_9999));
            flush(indexName);
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

    /// A single BCC blob's cache key and the cache midpoint of each of its compound commits (UNKNOWN for a CC without a `@timestamp`
    /// range), read from the object store. [#dataTimestamp()] is the single value the blob's metadata regions are backfilled with.
    private record BlobInfo(FileCacheKey cacheKey, List<Long> ccMidpoints) {
        long dataTimestamp() {
            return ccMidpoints.stream()
                .filter(ts -> ts != SharedBlobCacheService.UNKNOWN_TIMESTAMP)
                .max(Long::compare)
                .orElse(SharedBlobCacheService.UNKNOWN_TIMESTAMP);
        }
    }

    private void indexTimestampedDocs(String indexName, long timestamp) throws Exception {
        indexDocs(
            indexName,
            DOCS_PER_FLUSH,
            UnaryOperator.identity(),
            null,
            () -> Map.<String, Object>of("@timestamp", timestamp, "field", randomAlphaOfLength(DOC_FIELD_LENGTH))
        );
        refresh(indexName);
    }

    private void indexUntimestampedDocs(String indexName) throws Exception {
        indexDocs(indexName, DOCS_PER_FLUSH, UnaryOperator.identity(), null, () -> Map.of("field", randomAlphaOfLength(DOC_FIELD_LENGTH)));
        refresh(indexName);
    }

    /// Reads every BCC blob's compound commits from the object store (bypassing the search node's shared cache) to learn the cache
    /// midpoints its regions should be backfilled with.
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
            List<Long> ccMidpoints = new ArrayList<>();
            while (iterator.hasNext()) {
                ccMidpoints.add(BlobFileRanges.midpointMillisOrUnknownForCache(iterator.next().getTimestampFieldValueRange()));
            }
            blobs.add(new BlobInfo(new FileCacheKey(shardId, primaryTerm, blobName), ccMidpoints));
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
