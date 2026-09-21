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
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.PluggableDirectoryMetricsHolder;
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
import static org.hamcrest.Matchers.lessThan;
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
            .put(StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_TIMESTAMP_BACKFILL_ENABLED_SETTING.getKey(), true)
            // Enough room to keep every region cached for the duration of the test (no eviction).
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(16))
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RECOVERY_RANGE_SIZE_SETTING.getKey(), REGION_SIZE);
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
                blob.maxDataTimestamp(),
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

    /// Packs three compound commits with *mixed* `@timestamp` values into a single BCC blob and verifies that a search-shard recovery
    /// folds that blob to a single timestamp: the most recent known cache midpoint across its compound commits. The commit without a
    /// `@timestamp` value (UNKNOWN midpoint) and the older timestamped commit must both inherit the most recent commit's timestamp, never
    /// dragging the blob's regions to UNKNOWN.
    public void testRecoveryBackfillsMultiCcBlobWithSingleMostRecentTimestamp() throws Exception {
        var indexNode = startMasterAndIndexNode(
            Settings.builder().put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 3).build()
        );
        var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put(MergePolicyConfig.INDEX_MERGE_ENABLED, false)
            ).setMapping("@timestamp", "type=date")
        );

        // An older timestamped commit, then an untimestamped one, then the most recent timestamped commit. No flush: the third refresh
        // trips the count cutoff and uploads all three as one blob.
        long high = randomLongBetween(2, MAX_MILLIS_BEFORE_9999);
        long low = randomLongBetween(1, high - 1);
        indexTimestampedDocs(indexName, low);
        indexUntimestampedDocs(indexName);
        indexTimestampedDocs(indexName, high);

        var shardId = new ShardId(resolveIndex(indexName), 0);
        var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();
        var commitsContainer = getShardCommitsContainerForCurrentPrimaryTerm(indexName, indexNode, 0);

        // The cutoff-triggered upload is async, so wait for the packed blob to appear before reading its layout.
        assertBusy(() -> {
            var currentBlobs = readBlobInfosFromObjectStore(shardId, primaryTerm, indexNode, commitsContainer);
            assertThat(
                "the count cutoff must pack all three compound commits into one blob, got " + currentBlobs,
                currentBlobs.stream().anyMatch(b -> b.ccMidpoints().size() == 3),
                equalTo(true)
            );
        });
        var multiCcBlob = readBlobInfosFromObjectStore(shardId, primaryTerm, indexNode, commitsContainer).stream()
            .filter(b -> b.ccMidpoints().size() == 3)
            .findFirst()
            .orElseThrow();

        // The blob genuinely mixes an untimestamped commit with two timestamped ones, and folds to the most recent (higher) midpoint.
        assertThat(
            "one commit has no @timestamp (UNKNOWN midpoint)",
            multiCcBlob.ccMidpoints(),
            hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP)
        );
        var knownMidpoints = multiCcBlob.ccMidpoints()
            .stream()
            .filter(ts -> ts != SharedBlobCacheService.UNKNOWN_TIMESTAMP)
            .sorted()
            .toList();
        assertThat("two commits carry a @timestamp", knownMidpoints.size(), equalTo(2));
        assertThat("there is an older known midpoint that must not win the fold", knownMidpoints.get(0), lessThan(knownMidpoints.get(1)));
        assertThat("the fold picks the single most recent known midpoint", multiCcBlob.maxDataTimestamp(), equalTo(knownMidpoints.get(1)));

        // Recover the search shard: it reads the blob's metadata (stamping BACKFILL_IN_PROGRESS) and backfills its regions.
        var searchNode = startSearchNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1), indexName);
        ensureGreen(indexName);

        var cacheService = (CapturingCacheService) internalCluster().getInstance(
            StatelessPlugin.SharedBlobCacheServiceSupplier.class,
            searchNode
        ).get();

        assertBusy(() -> {
            var captured = cacheService.capturedTimestamps(multiCcBlob.cacheKey());
            assertThat("recovery must read the multi-CC blob's metadata", captured, not(empty()));
            assertThat(
                "the metadata read stamps the regions BACKFILL_IN_PROGRESS before backfill",
                captured,
                hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP)
            );
            assertThat(
                "time-based recovery must never stamp a region UNKNOWN",
                captured,
                not(hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
            );

            var live = cacheService.liveTimestamps(multiCcBlob.cacheKey());
            assertThat("backfill must leave the blob's regions live", live, not(empty()));
            assertThat(
                "the untimestamped and older commits inherit the single most recent known timestamp",
                live,
                hasItem(multiCcBlob.maxDataTimestamp())
            );
            assertThat(
                "backfill must leave no region at the transient sentinel",
                live,
                not(hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP))
            );
            assertThat(
                "time-based backfill must leave no live region UNKNOWN",
                live,
                not(hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
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
    public void testSearchShardRecoveryBackfillsUntimestampedTimeBasedRegionsToMinimal() throws Exception {
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
                assertThat("time-based metadata reads must have cached regions of blob " + cacheKey, captured, not(empty()));
                assertThat(
                    "a time-based index stamps metadata reads BACKFILL_IN_PROGRESS even when the CCs have no @timestamp",
                    captured,
                    hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP)
                );
                assertThat(
                    "time-based recovery must never stamp a region UNKNOWN",
                    captured,
                    not(hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
                );

                var live = cacheService.liveTimestamps(cacheKey);
                assertThat("backfill must leave the regions of blob " + cacheKey + " live", live, not(empty()));
                assertThat(
                    "with no data timestamp available every region is floored to MINIMAL_CACHE_TIMESTAMP, not left UNKNOWN",
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
                blob.maxDataTimestamp(),
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

    private record BlobInfo(FileCacheKey cacheKey, List<Long> ccMidpoints) {
        long maxDataTimestamp() {
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
            assertThat(
                "time-based metadata reads and warming must never stamp a region UNKNOWN",
                captured,
                not(hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
            );

            var live = cacheService.liveTimestamps(cacheKey);
            assertThat("backfill must leave live cache regions for blob " + cacheKey, live, not(empty()));
            assertThat(
                "metadata read and warming must populate more than one cache region for blob " + cacheKey,
                live.size(),
                greaterThan(1)
            );
            assertThat("backfill must resolve regions to the blob's data timestamp", live, hasItem(blob.maxDataTimestamp()));
            assertThat(
                "backfill must leave no live region stamped BACKFILL_IN_PROGRESS_TIMESTAMP",
                live,
                not(hasItem(SharedBlobCacheService.BACKFILL_IN_PROGRESS_TIMESTAMP))
            );
            assertThat(
                "time-based backfill must leave no live region UNKNOWN",
                live,
                not(hasItem(SharedBlobCacheService.UNKNOWN_TIMESTAMP))
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
            IndicesService indicesService,
            PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricHolder
        ) {
            return new CapturingCacheService(
                nodeEnvironment,
                settings,
                clusterService.getClusterSettings(),
                threadPool,
                blobCacheMetrics,
                metricHolder
            );
        }
    }

    static final class CapturingCacheService extends StatelessSharedBlobCacheService {

        private final TimestampCapturingEvictionPolicy capturingPolicy;

        CapturingCacheService(
            NodeEnvironment environment,
            Settings settings,
            ClusterSettings clusterSettings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics,
            PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricHolder
        ) {
            this(
                environment,
                settings,
                clusterSettings,
                threadPool,
                blobCacheMetrics,
                metricHolder,
                new TimestampCapturingEvictionPolicy()
            );
        }

        private CapturingCacheService(
            NodeEnvironment environment,
            Settings settings,
            ClusterSettings clusterSettings,
            ThreadPool threadPool,
            BlobCacheMetrics blobCacheMetrics,
            PluggableDirectoryMetricsHolder<BlobStoreCacheDirectoryMetrics> metricHolder,
            TimestampCapturingEvictionPolicy capturingPolicy
        ) {
            super(
                environment,
                settings,
                clusterSettings,
                threadPool,
                blobCacheMetrics,
                capturingPolicy,
                System::nanoTime,
                threadPool.executor(StatelessPlugin.SHARD_READ_THREAD_POOL),
                metricHolder
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
