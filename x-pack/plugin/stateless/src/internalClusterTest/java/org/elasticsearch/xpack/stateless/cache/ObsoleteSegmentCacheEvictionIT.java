/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.blobcache.BlobCacheUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.range;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_RANGE_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_REGION_SIZE_SETTING;
import static org.elasticsearch.blobcache.shared.SharedBlobCacheService.SHARED_CACHE_SIZE_SETTING;
import static org.elasticsearch.core.TimeValue.MINUS_ONE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.elasticsearch.xpack.stateless.lucene.BlobStoreCacheDirectoryTestUtils.doOpenInputWithoutReplicatedRanges;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Verifies that cache regions belonging to obsolete segments are evicted on the search node.
 */
public class ObsoleteSegmentCacheEvictionIT extends AbstractStatelessPluginIntegTestCase {

    private static final ByteSizeValue REGION_SIZE = ByteSizeValue.ofKb(4);
    private static final ByteSizeValue CACHE_SIZE = ByteSizeValue.ofMb(2);

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(
            StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING.getKey(),
            true
        )
            // Tests are checking the regions populated in the cache, so we disable the features that increase regions content or prefetch
            // regions in the background. Tests also expect specific BCCs so we disable background flushes too.
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(StatelessOnlinePrewarmingService.STATELESS_ONLINE_PREWARMING_ENABLED.getKey(), false)
            .put(SearchCommitPrefetcherDynamicSettings.PREFETCH_COMMITS_UPON_NOTIFICATIONS_ENABLED_SETTING.getKey(), false)
            .put(SharedBlobCacheWarmingService.SEARCH_OFFLINE_WARMING_ENABLED_SETTING.getKey(), false)
            // used to be required by `STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING`, now that force-evicting obsolete segments
            // setting is decoupled, it doesn't depend on this, so randomize.
            .put(SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), randomBoolean())
            .put(StatelessCommitService.STATELESS_COMMIT_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1) // each refresh creates a new BCC
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_SIZE.getKey(), ByteSizeValue.ofGb(1))
            .put(SHARED_CACHE_SIZE_SETTING.getKey(), CACHE_SIZE)
            .put(SHARED_CACHE_REGION_SIZE_SETTING.getKey(), REGION_SIZE)
            .put(SHARED_CACHE_RANGE_SIZE_SETTING.getKey(), REGION_SIZE);
    }

    /**
     * Creates multiple small segments, populates cache, then force-merges to produce a single segment.
     * After the search node picks up the post-merge commit, the cache regions for the now-obsolete pre-merge
     * segments should be evicted by {@code retainFilesAndEvict}.
     */
    public void testObsoleteSegmentRegionsAreEvicted() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final String indexName = "test-obsolete-eviction";
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE).build());
        ensureGreen(indexName);

        final var searchShard = findSearchShard(indexName);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
        final var searchCacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);

        int batches = randomIntBetween(2, 6);
        for (int batch = 0; batch < batches; batch++) {
            var bulkRequest = client().prepareBulk(indexName);
            range(0, randomIntBetween(10, 50)).mapToObj(
                n -> client().prepareIndex(indexName).setSource("value", n, "bytes", randomUnicodeOfLength(10))
            ).forEach(bulkRequest::add);

            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);

            refresh(indexName);
        }

        var blobsRegionsBeforeMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);

        assertThat(
            "all regions from pre-merge segments should be cached",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
            ),
            equalTo(blobsRegionsBeforeMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        // Force-merge to 1 segment
        forceMerge(true);
        refresh(indexName);

        var blobsRegionsAfterMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);

        assertThat(
            "post-merge blobs should be entirely new (no overlap with pre-merge blobs)",
            blobsRegionsAfterMerge.keySet().stream().noneMatch(blobsRegionsBeforeMerge::containsKey),
            equalTo(true)
        );

        assertBusy(() -> {
            assertThat(
                "cached regions should match exactly the regions from post-merge segments",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> blobsRegionsAfterMerge.containsKey(key.fileName())
                ),
                equalTo(blobsRegionsAfterMerge.values().stream().mapToLong(BitSet::cardinality).sum())
            );

            assertThat(
                "no cached region should belong to a pre-merge blob",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
                ),
                equalTo(0L)
            );
        });
    }

    /// Verifies the [StatelessSharedBlobCacheService#STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING] escape hatch
    /// is a live no-op when flipped off. With the setting enabled (the default from [nodeSettings()]) a force-merge
    /// evicts the obsolete pre-merge regions; once the setting is disabled dynamically, a subsequent force-merge leaves the
    /// now-obsolete regions cached.
    public void testObsoleteSegmentRegionEvictionCanBeDisabledDynamically() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE).build());
        ensureGreen(indexName);

        final var searchShard = findSearchShard(indexName);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
        final var searchCacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);

        // Phase 1: with eviction enabled (the class default), a force-merge evicts the obsolete pre-merge regions.
        indexBatchesAndRefresh(indexName);
        var regionsBeforeFirstMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        assertThat(
            "all regions from pre-merge segments should be cached",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> regionsBeforeFirstMerge.containsKey(key.fileName())
            ),
            equalTo(regionsBeforeFirstMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        forceMerge(true);
        refresh(indexName);
        var regionsAfterFirstMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        assertThat(
            "post-merge blobs should be entirely new (no overlap with pre-merge blobs)",
            regionsAfterFirstMerge.keySet().stream().noneMatch(regionsBeforeFirstMerge::containsKey),
            equalTo(true)
        );
        assertBusy(
            () -> assertThat(
                "obsolete pre-merge regions should be evicted while the setting is enabled",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> regionsBeforeFirstMerge.containsKey(key.fileName())
                ),
                equalTo(0L)
            )
        );

        // Flip the escape hatch off. updateClusterSettings blocks until the update is acknowledged by all nodes, so the search
        // node's cache service has observed the new value before we continue.
        updateClusterSettings(
            Settings.builder().put(StatelessSharedBlobCacheService.STATELESS_CACHE_EVICT_OBSOLETE_REGIONS_ENABLED_SETTING.getKey(), false)
        );

        // Phase 2: with eviction disabled, index a fresh set of segments, cache them, then force-merge again. The now-obsolete
        // pre-merge regions must be retained.
        indexBatchesAndRefresh(indexName);
        var regionsBeforeSecondMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        long cachedRegionsBeforeSecondMerge = searchCacheService.countCachedRegions(
            searchShard.shardId(),
            (key, region) -> regionsBeforeSecondMerge.containsKey(key.fileName())
        );
        assertThat(
            "all regions should be cached before the second merge",
            cachedRegionsBeforeSecondMerge,
            equalTo(regionsBeforeSecondMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        forceMerge(true);
        refresh(indexName);
        // SearchEngine calls retainFiles (where obsolete-region eviction is scheduled) before it fires the segment-generation
        // listener, so waiting for the search shard to process a later commit guarantees retainFiles has run - and therefore has
        // already incremented the eviction-task counter if the (now-disabled) gate had let it schedule anything.
        flushAndWaitForSearchShard(indexName, searchEngine);
        // Draining the counter to zero then guarantees any scheduled eviction has fully completed. With the setting disabled it is
        // already zero (nothing scheduled) so this returns immediately; were the escape hatch broken, this would wait for the async
        // force-evict to run and the retention assertion below would then fail deterministically, instead of racing the async task.
        assertBusy(() -> assertThat(BlobStoreCacheDirectoryTestUtils.pendingObsoleteRegionsEvictionTasks(searchDirectory), equalTo(0L)));

        var regionsAfterSecondMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        assertThat(
            "post-merge blobs should be entirely new (no overlap with pre-merge blobs)",
            regionsAfterSecondMerge.keySet().stream().noneMatch(regionsBeforeSecondMerge::containsKey),
            equalTo(true)
        );
        assertThat(
            "obsolete pre-merge regions must be retained while the setting is disabled",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> regionsBeforeSecondMerge.containsKey(key.fileName())
            ),
            equalTo(cachedRegionsBeforeSecondMerge)
        );
    }

    /**
     * Similar to {@link #testObsoleteSegmentRegionsAreEvicted} but with an open PIT (Point in Time) that holds
     * a reader on the pre-merge segments. The old segments' cache regions should be retained while the PIT is
     * open and evicted after it is closed and a subsequent commit is processed.
     */
    public void testObsoleteSegmentRegionsRetainedByPIT() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final String indexName = "test-obsolete-pit-eviction";
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE).build());
        ensureGreen(indexName);

        final var searchShard = findSearchShard(indexName);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
        final var searchCacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);

        int batches = randomIntBetween(2, 6);
        for (int batch = 0; batch < batches; batch++) {
            var bulkRequest = client().prepareBulk(indexName);
            range(0, randomIntBetween(10, 50)).mapToObj(
                n -> client().prepareIndex(indexName).setSource("value", n, "bytes", randomUnicodeOfLength(10))
            ).forEach(bulkRequest::add);

            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);

            refresh(indexName);
        }

        var blobsRegionsBeforeMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);

        assertThat(
            "all regions from pre-merge segments should be cached",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
            ),
            equalTo(blobsRegionsBeforeMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        // Open a PIT that holds a reader on the current (pre-merge) segments
        var openPitResponse = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(indexName).keepAlive(TimeValue.timeValueMinutes(5))
        ).actionGet();
        final var pitId = openPitResponse.getPointInTimeId();

        // Force-merge to 1 segment
        forceMerge(true);
        refresh(indexName);

        var blobsRegionsAfterMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);

        assertThat(
            "post-merge regions should be in cache",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> blobsRegionsAfterMerge.containsKey(key.fileName())
            ),
            equalTo(blobsRegionsAfterMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        assertThat(
            "pre-merge regions should be retained while PIT holds readers",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
            ),
            equalTo(blobsRegionsBeforeMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        // Close the PIT
        var closeResponse = client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pitId)).actionGet();
        assertThat(closeResponse.status(), equalTo(RestStatus.OK));

        // After PIT close, trigger a new commit so the search node processes retainFilesAndEvict without the PIT reader
        // TODO Fix this, it would be better to have immediate release/eviction after a reader is closed
        flush(indexName);
        refresh(indexName);

        var blobsRegionsAfterClosePIT = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);

        assertThat(
            "post-merge blobs should be entirely new (no overlap with pre-merge blobs)",
            blobsRegionsAfterClosePIT.keySet().stream().noneMatch(blobsRegionsBeforeMerge::containsKey),
            equalTo(true)
        );

        assertBusy(() -> {
            assertThat(
                "cached regions should match exactly the regions from post-merge segments",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> blobsRegionsAfterClosePIT.containsKey(key.fileName())
                ),
                equalTo(blobsRegionsAfterClosePIT.values().stream().mapToLong(BitSet::cardinality).sum())
            );

            assertThat(
                "no cached region should belong to a pre-merge blob",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
                ),
                equalTo(0L)
            );
        });
    }

    /**
     * Tests that cache regions for generational doc values files are evicted along with their parent
     * segment's regions when a force-merge eliminates the segment.
     */
    public void testGenerationalFileRegionsEvictedAfterMerge() throws Exception {
        startMasterAndIndexNode();
        startSearchNode();

        final String indexName = "test-gen-merge-eviction";
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE).build());
        ensureGreen(indexName);

        final var searchShard = findSearchShard(indexName);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
        final var searchCacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);

        final var docIds = new HashSet<String>();

        int batches = randomIntBetween(2, 6);
        for (int batch = 0; batch < batches; batch++) {
            var bulkRequest = client().prepareBulk(indexName);
            range(0, randomIntBetween(10, 50)).mapToObj(
                n -> client().prepareIndex(indexName).setSource("value", n, "bytes", randomUnicodeOfLength(10))
            ).forEach(bulkRequest::add);

            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);

            for (var result : bulkResponse.getItems()) {
                assertThat(result.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));
                docIds.add(result.getId());
            }

            refresh(indexName);
        }

        // Delete some docs to create generational doc values files (all generation files are in the same blob)
        var deleteBulk = client().prepareBulk(indexName);
        randomSubsetOf(randomIntBetween(1, docIds.size() - 1), docIds).stream()
            .map(id -> client().prepareDelete(indexName, id))
            .forEach(deleteBulk::add);
        assertNoFailures(deleteBulk.get());
        refresh(indexName);

        var blobsRegionsBeforeMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        assertThat(Set.of(searchDirectory.listAll()).stream().filter(StatelessCompoundCommit::isGenerationalFile).count(), greaterThan(0L));

        assertThat(
            "all regions from pre-merge segments (including generational files) should be cached",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
            ),
            equalTo(blobsRegionsBeforeMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        // Force-merge to 1 segment
        forceMerge(true);
        refresh(indexName);

        var blobsRegionsAfterMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);

        assertThat(
            "post-merge blobs should be entirely new (no overlap with pre-merge blobs)",
            blobsRegionsAfterMerge.keySet().stream().noneMatch(blobsRegionsBeforeMerge::containsKey),
            equalTo(true)
        );

        assertBusy(() -> {
            assertThat(
                "cached regions should match exactly the regions from post-merge segments",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> blobsRegionsAfterMerge.containsKey(key.fileName())
                ),
                equalTo(blobsRegionsAfterMerge.values().stream().mapToLong(BitSet::cardinality).sum())
            );

            assertThat(
                "no cached region should belong to a pre-merge blob (including generational file blobs)",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
                ),
                equalTo(0L)
            );
        });
    }

    /**
     * Tests that cache regions for obsolete generational doc values files are evicted when the doc values
     * generation rolls over (new deletions create a new generation that replaces the old one).
     */
    public void testFullyDeletedSegmentsEvicted() throws Exception {
        final var indexNode = startMasterAndIndexNode(
            Settings.builder().put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE).build()
        );
        startSearchNode();

        final String indexName = "test-fully-deleted";
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE).build());
        ensureGreen(indexName);

        final var searchShard = findSearchShard(indexName);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
        final var searchCacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);

        final int nbSegments = randomIntBetween(1, 5);
        final var bytes = randomUnicodeOfLength(BlobCacheUtils.toIntBytes(REGION_SIZE.getBytes()));

        record DocPerSegment(String docId, String blobName, long minOffset, long maxOffset) {}
        final var docPerSegments = new ArrayList<DocPerSegment>();
        long offset = 0L;

        // Index 1 doc and immediately refresh (so each doc is flushed in its own segment on disk)
        for (int i = 0; i < nbSegments; i++) {
            var bulkRequest = client().prepareBulk(indexName).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            bulkRequest.add(client().prepareIndex(indexName).setSource("whatever", bytes));

            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);

            var docResult = bulkResponse.getItems()[0];
            assertThat(docResult.getResponse().getResult(), equalTo(DocWriteResponse.Result.CREATED));

            var virtualBcc = internalCluster().getInstance(StatelessCommitService.class, indexNode)
                .getCurrentVirtualBcc(searchShard.shardId());
            assertThat(virtualBcc, notNullValue());

            docPerSegments.add(new DocPerSegment(docResult.getId(), virtualBcc.getBlobName(), offset, virtualBcc.getTotalSizeInBytes()));
            offset = virtualBcc.getTotalSizeInBytes();
        }

        flushAndWaitForSearchShard(indexName, searchEngine);

        // At this stage we have multiple segments within a single BCC
        final var blobsRegionsBeforeUpdates = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        assertThat(blobsRegionsBeforeUpdates.size(), equalTo(1));

        assertThat(
            "all regions should be cached",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> blobsRegionsBeforeUpdates.containsKey(key.fileName())
            ),
            equalTo(blobsRegionsBeforeUpdates.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        // Update random docs: we have 1 doc per segment before the updates, and all updates are bulked together, so we'll end up with
        // a new segment and as many generational files as updated doc.
        final var updatedDocs = randomNonEmptySubsetOf(docPerSegments);

        var bulkRequest = client().prepareBulk(indexName);
        for (var updatedDoc : updatedDocs) {
            bulkRequest.add(client().prepareUpdate(indexName, updatedDoc.docId()).setDoc("whatever", bytes, "updated", true));
        }

        var bulkResponse = bulkRequest.get();
        assertNoFailures(bulkResponse);

        for (var docResult : bulkResponse.getItems()) {
            assertThat(docResult.getResponse().getResult(), equalTo(DocWriteResponse.Result.UPDATED));
        }

        refresh(indexName);
        if (randomBoolean()) {
            flushAndWaitForSearchShard(indexName, searchEngine);
        }

        final var blobsRegionsAfterUpdates = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        if (updatedDocs.size() == nbSegments) {
            // If all docs were updated, then all segments have been replaced and we only have 1 BCC with the last segment
            assertThat(blobsRegionsAfterUpdates.size(), equalTo(1));
            assertThat(blobsRegionsAfterUpdates.keySet().stream().noneMatch(blobsRegionsBeforeUpdates::containsKey), equalTo(true));

            assertBusy(() -> {
                assertThat(
                    "cached regions should match exactly the regions from post-update segment",
                    searchCacheService.countCachedRegions(
                        searchShard.shardId(),
                        (key, region) -> blobsRegionsAfterUpdates.containsKey(key.fileName())
                    ),
                    equalTo(blobsRegionsAfterUpdates.values().stream().mapToLong(BitSet::cardinality).sum())
                );

                assertThat(
                    "no cached region should belong to a pre-update blob",
                    searchCacheService.countCachedRegions(
                        searchShard.shardId(),
                        (key, region) -> blobsRegionsBeforeUpdates.containsKey(key.fileName())
                    ),
                    equalTo(0L)
                );
            });

        } else {
            // Otherwise we have 2 BCCs, including the one before updates
            assertThat(blobsRegionsAfterUpdates.size(), equalTo(2));
            assertThat(blobsRegionsAfterUpdates.keySet().containsAll(blobsRegionsBeforeUpdates.keySet()), equalTo(true));

            // Compute which regions from the pre-update blob should still be cached based on non-updated segments
            final var updatedDocIds = updatedDocs.stream().map(DocPerSegment::docId).collect(Collectors.toSet());
            final var retainedRegions = new BitSet();
            final var removedRegions = new BitSet();
            for (var doc : docPerSegments) {
                int startRegion = searchCacheService.getRegion(doc.minOffset());
                int endRegion = searchCacheService.getEndingRegion(doc.maxOffset()) + 1;
                if (updatedDocIds.contains(doc.docId())) {
                    removedRegions.set(startRegion, endRegion);
                } else {
                    retainedRegions.set(startRegion, endRegion);
                }
            }

            // Regions exclusively belonging to updated segments (not shared with retained ones)
            var expectedRemovedRegions = (BitSet) removedRegions.clone();
            expectedRemovedRegions.andNot(retainedRegions);

            assertBusy(() -> {
                assertThat(
                    "Regions of pre-update blob should be only those containing non-updated docs",
                    searchCacheService.countCachedRegions(searchShard.shardId(), (key, region) -> {
                        if (blobsRegionsBeforeUpdates.containsKey(key.fileName())) {
                            assertThat(retainedRegions.get(region), equalTo(true));
                            assertThat(expectedRemovedRegions.get(region), equalTo(false));
                            return true;
                        }
                        return false;
                    }),
                    equalTo((long) blobsRegionsAfterUpdates.get(docPerSegments.get(0).blobName()).cardinality())
                );
            });
        }
    }

    /**
     * Creates multiple segments within a single BCC, then force-merges them. After the merge, the old segment regions within the same BCC
     * blob should be evicted while the new segment's regions are retained.
     */
    public void testObsoleteSegmentRegionsEvictedWithinSameBCC() throws Exception {
        final var extraNodeSettings = Settings.builder()
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), Integer.MAX_VALUE)
            .build();

        startMasterAndIndexNode(extraNodeSettings);
        startSearchNode(extraNodeSettings);

        final String indexName = "test-obsolete-same-bcc-eviction";
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), MINUS_ONE).build());
        ensureGreen(indexName);

        final var searchShard = findSearchShard(indexName);
        final var searchEngine = getShardEngine(searchShard, SearchEngine.class);
        final var searchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
        final var searchCacheService = BlobStoreCacheDirectoryTestUtils.getCacheService(searchDirectory);

        final var bytes = randomUnicodeOfLength(BlobCacheUtils.toIntBytes(REGION_SIZE.getBytes()));

        int batches = randomIntBetween(2, 6);
        for (int batch = 0; batch < batches; batch++) {
            var bulkRequest = client().prepareBulk(indexName);
            range(0, randomIntBetween(1, 15)).mapToObj(n -> client().prepareIndex(indexName).setSource("value", n, "bytes", bytes))
                .forEach(bulkRequest::add);

            var bulkResponse = bulkRequest.get();
            assertNoFailures(bulkResponse);

            refresh(indexName);
        }

        var blobsRegionsBeforeMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);

        assertThat("all segments should be in a single BCC blob", blobsRegionsBeforeMerge.size(), equalTo(1));
        assertThat(
            "all regions from pre-merge segments should be cached",
            searchCacheService.countCachedRegions(
                searchShard.shardId(),
                (key, region) -> blobsRegionsBeforeMerge.containsKey(key.fileName())
            ),
            equalTo(blobsRegionsBeforeMerge.values().stream().mapToLong(BitSet::cardinality).sum())
        );

        assertNoFailures(indicesAdmin().prepareForceMerge().setFlush(false).setMaxNumSegments(1).get());
        refresh(indexName);
        if (randomBoolean()) {
            flushAndWaitForSearchShard(indexName, searchEngine);
        }

        var blobsRegionsAfterMerge = readAllFilesAndCollectRegions(searchEngine, searchDirectory, searchCacheService);
        assertThat("all segments should be in a single BCC blob", blobsRegionsAfterMerge.size(), equalTo(1));

        assertBusy(() -> {
            assertThat(
                "cached regions should match exactly the regions from post-merge segments",
                searchCacheService.countCachedRegions(
                    searchShard.shardId(),
                    (key, region) -> blobsRegionsAfterMerge.containsKey(key.fileName())
                ),
                equalTo(blobsRegionsAfterMerge.values().stream().mapToLong(BitSet::cardinality).sum())
            );

            assertThat(
                "no cached region should belong exclusively to a pre-merge blob",
                searchCacheService.countCachedRegions(searchShard.shardId(), (key, region) -> {
                    if (blobsRegionsBeforeMerge.containsKey(key.fileName()) == false) {
                        return false;
                    }
                    BitSet afterMergeRegions = blobsRegionsAfterMerge.get(key.fileName());
                    return afterMergeRegions == null || afterMergeRegions.get(region) == false;
                }),
                equalTo(0L)
            );
        });
    }

    /**
     * Acquires an IndexCommit on the search engine, reads every file in the directory to populate
     * the cache, and returns the set of cache regions per blob.
     */
    private static HashMap<String, BitSet> readAllFilesAndCollectRegions(
        SearchEngine searchEngine,
        SearchDirectory searchDirectory,
        StatelessSharedBlobCacheService cacheService
    ) throws Exception {
        try (Engine.IndexCommitRef commitRef = searchEngine.acquireLastIndexCommit(false)) {
            assert commitRef != null;

            var regionsByBlob = new HashMap<String, BitSet>();
            for (var file : commitRef.getIndexCommit().getFileNames()) {
                // Reads the file at its original position in the blob
                try (var input = doOpenInputWithoutReplicatedRanges(searchDirectory, file, IOContext.READONCE)) {
                    CodecUtil.checksumEntireFile(input);
                }
                var blobFileRanges = searchDirectory.getBlobFileRangesForFile(file);
                assertNotNull(blobFileRanges);
                var blobLocation = blobFileRanges.blobLocation();
                assertNotNull(blobLocation);
                regionsByBlob.computeIfAbsent(blobLocation.blobName(), k -> new BitSet())
                    .set(
                        cacheService.getRegion(blobLocation.offset()),
                        cacheService.getEndingRegion(blobLocation.offset() + blobLocation.fileLength()) + 1
                    );
                if (blobFileRanges.hasReplicatedRanges()) {
                    // Reads the file again using replicated ranges
                    try (var input = searchDirectory.openInput(file, IOContext.READONCE)) {
                        CodecUtil.checksumEntireFile(input);
                    }
                    blobFileRanges.forEachReplicatedRange(
                        (offset, length) -> regionsByBlob.computeIfAbsent(blobLocation.blobName(), k -> new BitSet())
                            .set(cacheService.getRegion(offset), cacheService.getEndingRegion(offset + length) + 1)
                    );
                }
            }
            return regionsByBlob;
        }
    }

    private void indexBatchesAndRefresh(final String indexName) {
        final int batches = randomIntBetween(2, 6);
        for (int batch = 0; batch < batches; batch++) {
            var bulkRequest = client().prepareBulk(indexName);
            range(0, randomIntBetween(10, 50)).mapToObj(
                n -> client().prepareIndex(indexName).setSource("value", n, "bytes", randomUnicodeOfLength(10))
            ).forEach(bulkRequest::add);
            assertNoFailures(bulkRequest.get());
            refresh(indexName);
        }
    }

    private void flushAndWaitForSearchShard(final String indexName, final SearchEngine searchEngine) {
        assertThat(indexName, equalTo(searchEngine.config().getShardId().getIndexName()));
        final var future = new PlainActionFuture<Long>();

        var indexEngine = getShardEngine(findIndexShard(indexName), IndexEngine.class);
        searchEngine.addPrimaryTermAndGenerationListener(0L, indexEngine.getCurrentGeneration() + 1L, ActionListener.assertOnce(future));
        flush(indexName);
        safeGet(future);
    }
}
