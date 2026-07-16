/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.blobcache.common.ByteRange;
import org.elasticsearch.blobcache.shared.DefaultEvictionPolicy;
import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.ThreadLocalDirectoryMetricHolder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher.FileTimestampResolver;
import org.elasticsearch.xpack.stateless.cache.reader.CacheBlobReader;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFile;
import org.elasticsearch.xpack.stateless.commits.BlobLocation;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.NewCommitNotification;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryMetrics;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;

import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.UNKNOWN_TIMESTAMP;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcher.getPendingRangesToPrefetch;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class SearchCommitPrefetcherTests extends ESTestCase {
    public void testInitialGetPendingRangesToPrefetch() {
        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                1,
                luceneFile(1, 10, 10),
                luceneFile(1, 20, 20)
            );

            assertThat(
                rangesToPrefetch,
                // Only expected to fetch up to the latest file
                equalTo(Map.of(blobFile(1), ByteRange.of(0, 40)))
            );
        }

        {
            var rangesToPrefetch = computeRangesToPrefetch(
                new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 1), 30),
                1,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(1, 40, 20)
            );

            assertThat(
                rangesToPrefetch,
                // Expect to prefetch the contiguous range between the latest prefetched offset and the new file (even if there's a hole)
                equalTo(Map.of(blobFile(1), ByteRange.of(30, 60)))
            );
        }

        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(2, 10, 20)
            );
            assertThat(
                rangesToPrefetch,
                // Since this is the first commit that we prefetch, and it uses files stored in two BCCs
                // we have to fetch ranges from both regions
                equalTo(Map.of(blobFile(1), ByteRange.of(0, 30), blobFile(2), ByteRange.of(0, 30)))
            );
        }
    }

    public void testGetPendingRangesWithMaxBCCToPrefetch() {
        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                1,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(2, 0, 20)
            );
            assertThat(rangesToPrefetch, equalTo(Map.of(blobFile(1), ByteRange.of(0, 30))));
        }

        {
            var rangesToPrefetch = computeRangesToPrefetch(
                SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(2, 0, 20)
            );
            assertThat(rangesToPrefetch, equalTo(Map.of(blobFile(1), ByteRange.of(0, 30), blobFile(2), ByteRange.of(0, 20))));
        }
    }

    public void testGetPendingRangesToPrefetchOnSubsequentCommits() {
        var bccPreFetchedOffset = SearchCommitPrefetcher.BCCPreFetchedOffset.ZERO;

        assertThat(
            computeRangesToPrefetch(bccPreFetchedOffset, 1, luceneFile(1, 0, 10), luceneFile(1, 10, 20)),
            // Only expected to fetch up to the latest file
            equalTo(Map.of(blobFile(1), ByteRange.of(0, 30)))
        );

        bccPreFetchedOffset = new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 1), 30);

        assertThat(
            computeRangesToPrefetch(bccPreFetchedOffset, 1, luceneFile(1, 0, 10), luceneFile(1, 10, 20), luceneFile(1, 30, 20)),
            // Previously we fetched up to offset 30, now we only need to fetch the latest file
            equalTo(Map.of(blobFile(1), ByteRange.of(30, 50)))
        );

        bccPreFetchedOffset = new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 1), 50);

        assertThat(
            computeRangesToPrefetch(
                bccPreFetchedOffset,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(1, 30, 20),
                luceneFile(1, 50, 100),
                luceneFile(2, 0, 150)
            ),
            // Somehow we missed fetching one file from the BCC with generation 1 and now there's a new generation 2,
            // so we have to prefetch the missing range from 1 and the new file
            equalTo(Map.of(blobFile(1), ByteRange.of(50, 150), blobFile(2), ByteRange.of(0, 150)))
        );

        bccPreFetchedOffset = new SearchCommitPrefetcher.BCCPreFetchedOffset(termAndGen(1, 2), 150);
        assertThat(
            computeRangesToPrefetch(
                bccPreFetchedOffset,
                2,
                luceneFile(1, 0, 10),
                luceneFile(1, 10, 20),
                luceneFile(1, 30, 20),
                luceneFile(1, 50, 100),
                luceneFile(2, 0, 150)
            ),
            is(equalTo(Map.of()))
        );
    }

    public void testComputeTimestampPerBlobAttributesInternalAndReferencedFiles() {
        final BlobFile referencedBlobA = blobFile(1);
        final BlobFile referencedBlobB = blobFile(2);
        final BlobFile referencedBlobC = blobFile(3);
        final BlobFile internalBlob = blobFile(4);

        final Map<String, BlobLocation> commitFiles = new HashMap<>();
        commitFiles.put("internal_1", new BlobLocation(internalBlob, 0, 10));
        commitFiles.put("internal_2", new BlobLocation(internalBlob, 10, 10));
        commitFiles.put("referenced_a", new BlobLocation(referencedBlobA, 0, 10));
        // Two referenced files in the same older blob to exercise the most-recent-wins merge.
        commitFiles.put("referenced_b1", new BlobLocation(referencedBlobB, 0, 10));
        commitFiles.put("referenced_b2", new BlobLocation(referencedBlobB, 10, 10));
        // Somehow the directory is missing timestamp for this file
        commitFiles.put("referenced_c", new BlobLocation(referencedBlobC, 0, 10));

        final var notificationRange = new StatelessCompoundCommit.TimestampFieldValueRange(1000L, 3000L); // midpoint 2000
        final StatelessCompoundCommit commit = compoundCommit(4, commitFiles, Set.of("internal_1", "internal_2"), notificationRange);

        final Map<String, Long> resolvedTimestamps = Map.of("referenced_a", 500L, "referenced_b1", 1500L, "referenced_b2", 2500L);
        final FileTimestampResolver resolver = fileName -> resolvedTimestamps.getOrDefault(fileName, UNKNOWN_TIMESTAMP);

        final Map<BlobFile, Long> timestampPerBlob = SearchCommitPrefetcher.computeTimestampPerBlob(
            commit,
            Set.of(internalBlob, referencedBlobA, referencedBlobB, referencedBlobC),
            resolver
        );

        assertThat("internal blob takes the notification commit midpoint", timestampPerBlob.get(internalBlob), equalTo(2000L));
        assertThat("referenced blob A takes the resolved timestamp", timestampPerBlob.get(referencedBlobA), equalTo(500L));
        assertThat(
            "referenced blob B keeps the most recent of its two referenced files",
            timestampPerBlob.get(referencedBlobB),
            equalTo(2500L)
        );
        assertThat(
            "referenced blob C has no known file timestamp, so it resolved to UNKNOWN_TIMESTAMP",
            timestampPerBlob.get(referencedBlobC),
            equalTo(UNKNOWN_TIMESTAMP)
        );
    }

    public void testComputeTimestampPerBlobKeepsUnknownWhenNotificationTimestampUnknown() {
        final BlobFile referencedBlob = blobFile(1);
        final Map<String, BlobLocation> commitFiles = new HashMap<>();
        // No internal files: the whole commit has no timestamp range, and the referenced file is unknown too.
        commitFiles.put("referenced", new BlobLocation(referencedBlob, 0, 10));

        final StatelessCompoundCommit commit = compoundCommit(4, commitFiles, Set.of(), null);
        final FileTimestampResolver resolver = fileName -> UNKNOWN_TIMESTAMP;

        final Map<BlobFile, Long> timestampPerBlob = SearchCommitPrefetcher.computeTimestampPerBlob(
            commit,
            Set.of(referencedBlob),
            resolver
        );

        assertThat(
            "with an unknown notification timestamp there is nothing to fall back to, so the blob stays unknown",
            timestampPerBlob.get(referencedBlob),
            equalTo(UNKNOWN_TIMESTAMP)
        );
    }

    public void testPrefetchPropagatesPerCcTimestamp() throws Exception {
        long generation = 4L;
        final BlobFile internalBlob = blobFile(generation);
        final Map<String, BlobLocation> commitFiles = new HashMap<>();
        commitFiles.put("internal_1", new BlobLocation(internalBlob, 0, 10));
        commitFiles.put("internal_2", new BlobLocation(internalBlob, 10, 10));
        final var notificationRange = new StatelessCompoundCommit.TimestampFieldValueRange(1000L, 3000L); // midpoint 2000
        final StatelessCompoundCommit commit = compoundCommit(
            generation,
            commitFiles,
            Set.of("internal_1", "internal_2"),
            notificationRange
        );
        final ShardId shardId = commit.shardId();
        // Mark the BCC as uploaded so the prefetcher proceeds to populate the cache for the current generation.
        final var notification = new NewCommitNotification(commit, generation, new PrimaryTermAndGeneration(1L, generation), 0L, "node");

        final Settings settings = Settings.builder()
            .put(NODE_NAME_SETTING.getKey(), "node")
            .put(SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(16))
            .put(SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING.getKey(), ByteSizeValue.ofMb(1))
            .put("path.home", createTempDir())
            .build();

        record CapturedRegion(FileCacheKey key, int region) {}
        final Map<CapturedRegion, Long> capturedTimestamps = new ConcurrentHashMap<>();
        final DeterministicTaskQueue taskQueue = new DeterministicTaskQueue();
        final ThreadPool threadPool = taskQueue.getThreadPool();

        try (
            NodeEnvironment environment = new NodeEnvironment(settings, TestEnvironment.newEnvironment(settings));
            StatelessSharedBlobCacheService cacheService = new StatelessSharedBlobCacheService(
                environment,
                settings,
                threadPool,
                BlobCacheMetrics.NOOP,
                new DefaultEvictionPolicy<FileCacheKey>(),
                System::nanoTime,
                new ThreadLocalDirectoryMetricHolder<>(BlobStoreCacheDirectoryMetrics::new)
            ) {
                @Override
                public void fetchRange(
                    FileCacheKey cacheKey,
                    int region,
                    ByteRange range,
                    long blobLength,
                    SharedBlobCacheService.RangeMissingHandler writer,
                    Executor fetchExecutor,
                    boolean force,
                    long timestampMillis,
                    ActionListener<Boolean> listener
                ) {
                    // Capture the timestamp the prefetcher passes per region instead of populating a real region.
                    capturedTimestamps.put(new CapturedRegion(cacheKey, region), timestampMillis);
                    listener.onResponse(false);
                }
            }
        ) {
            final ClusterSettings clusterSettings = new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(
                    ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                    SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING,
                    SearchCommitPrefetcher.PREFETCH_NON_UPLOADED_COMMITS_SETTING,
                    SearchCommitPrefetcherDynamicSettings.PREFETCH_SEARCH_IDLE_TIME_SETTING,
                    SearchCommitPrefetcher.BACKGROUND_PREFETCH_ENABLED_SETTING,
                    SearchCommitPrefetcher.PREFETCH_REQUEST_SIZE_LIMIT_INDEX_NODE_SETTING,
                    SearchCommitPrefetcher.FORCE_PREFETCH_SETTING,
                    SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT
                )
            );

            final CacheBlobReader cacheBlobReader = new CacheBlobReader() {
                @Override
                public ByteRange getRange(long position, int length, long remainingFileLength) {
                    return ByteRange.of(position, position + length);
                }

                @Override
                public void getRangeInputStream(long position, int length, ActionListener<InputStream> listener) {
                    throw new AssertionError("fetchRange is short-circuited; stream path must not be reached");
                }
            };

            final SearchCommitPrefetcher prefetcher = new SearchCommitPrefetcher(
                shardId,
                cacheService,
                blobFile -> cacheBlobReader,
                // Internal files take the notification commit's timestamp, so the resolver value is irrelevant here.
                fileName -> UNKNOWN_TIMESTAMP,
                threadPool,
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                clusterSettings,
                new SearchCommitPrefetcherDynamicSettings(clusterSettings)
            );

            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            prefetcher.maybePrefetchLatestCommit(notification, 0L, future);
            taskQueue.runAllRunnableTasks();
            safeGet(future);

            final FileCacheKey internalBlobKey = new FileCacheKey(shardId, internalBlob.primaryTerm(), internalBlob.blobName());
            final long expectedTimestamp = notificationRange.midpointMillis();
            final var internalBlobStamps = capturedTimestamps.entrySet()
                .stream()
                .filter(e -> e.getKey().key().equals(internalBlobKey))
                .map(Map.Entry::getValue)
                .toList();
            assertThat("prefetch should have fetched at least one region of the internal blob", internalBlobStamps, not(empty()));
            assertThat(
                "every prefetched region of the internal blob should be fetched with the notification commit midpoint",
                internalBlobStamps,
                everyItem(equalTo(expectedTimestamp))
            );
        }
    }

    private Map<BlobFile, ByteRange> computeRangesToPrefetch(
        SearchCommitPrefetcher.BCCPreFetchedOffset bccPreFetchedOffset,
        long maxBCCGenerationToPrefetch,
        BlobLocation... blobLocations
    ) {
        assertThat(blobLocations.length, greaterThan(0));
        return getPendingRangesToPrefetch(bccPreFetchedOffset, maxBCCGenerationToPrefetch, Arrays.asList(blobLocations));
    }

    private StatelessCompoundCommit compoundCommit(
        long generation,
        Map<String, BlobLocation> commitFiles,
        Set<String> internalFiles,
        StatelessCompoundCommit.TimestampFieldValueRange timestampRange
    ) {
        return new StatelessCompoundCommit(
            new ShardId("index", "_na_", 0),
            new PrimaryTermAndGeneration(1L, generation),
            1L,
            "_na_",
            commitFiles,
            commitFiles.values().stream().mapToLong(BlobLocation::fileLength).sum(),
            internalFiles,
            0L,
            InternalFilesReplicatedRanges.EMPTY,
            Map.of(),
            timestampRange
        );
    }

    private BlobLocation luceneFile(long generation, long offset, long length) {
        return new BlobLocation(blobFile(generation), offset, length);
    }

    private BlobFile blobFile(long generation) {
        return new BlobFile(BatchedCompoundCommit.blobNameFromGeneration(generation), termAndGen(1, generation));
    }

    private static PrimaryTermAndGeneration termAndGen(long term, long generation) {
        return new PrimaryTermAndGeneration(term, generation);
    }
}
