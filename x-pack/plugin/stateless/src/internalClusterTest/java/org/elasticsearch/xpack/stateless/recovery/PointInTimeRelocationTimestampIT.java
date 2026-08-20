/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.BlobFileRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.TimestampFieldValueRange;
import org.elasticsearch.xpack.stateless.engine.SearchEngine;
import org.elasticsearch.xpack.stateless.lucene.SearchDirectory;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.OpenPITContextInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.common.time.DateUtils.MAX_MILLIS_BEFORE_9999;
import static org.elasticsearch.search.SearchService.PIT_RELOCATION_FEATURE_FLAG;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.commits.StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS;
import static org.elasticsearch.xpack.stateless.recovery.TransportStatelessUnpromotableRelocationAction.START_HANDOFF_ACTION_NAME;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

/// Integration tests verifying that PIT relocation correctly transfers `@timestamp` field value
/// range metadata in the wire payload, including generational files whose blob location changed between
/// when SearchDirectory first pinned them and the commit the PIT was opened at.
public class PointInTimeRelocationTimestampIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(PointInTimeRelocationIT.PITRelocationTestPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(SearchService.KEEPALIVE_INTERVAL_SETTING.getKey(), TimeValue.timeValueSeconds(1))
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            // With UPLOAD_MAX=1, every generational file is re-written as an internal file in each new BCC blob, giving it a fresh offset.
            // SearchDirectory's putIfAbsent keeps the old BCC's location, so overrideBlobFileRangesTimestamp sees a location mismatch.
            .put(STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 1)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    /// Verifies that the PIT wire payload carries the correct `@timestamp` range for every file
    /// in the commit after relocation.
    ///
    /// With `STATELESS_UPLOAD_MAX_AMOUNT_COMMITS=1` every flush or refresh creates its own
    /// single-commit BCC blob. Each file is stamped with the timestamp range of the BCC in
    /// which it was first written, and [SearchDirectory]
    /// preserves that range via `putIfAbsent`. The expected timestamp range for each file is
    /// captured directly from the source [SearchDirectory]
    /// right before PIT opens — this is the ground truth against which the wire payload is
    /// compared, which is precise regardless of how many BCCs exist or what timestamps they carry.
    public void testPitRelocationTransfersTimestamps() throws Exception {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        final var indexNode = startMasterAndIndexNode();
        final var searchNodeA = startSearchNode();
        final var searchNodeB = startSearchNode();

        final var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put("index.routing.allocation.exclude._name", searchNodeB)
            ).setMapping("@timestamp", "type=date")
        );
        ensureGreen(indexName);

        final var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        final var shardId = new ShardId(resolveIndex(indexName), 0);

        final int extraIndexingRounds = between(0, 2);
        final long timestamp = randomLongBetween(1, MAX_MILLIS_BEFORE_9999 - extraIndexingRounds);
        indexDocs(
            indexName,
            between(50, 200),
            UnaryOperator.identity(),
            null,
            () -> Map.of("@timestamp", timestamp, "field", randomAlphaOfLength(10))
        );
        flushAndAwaitSearchNodeCommit(indexName, commitService, shardId);

        // Randomly do a few more rounds of indexing + refresh to build additional segments
        // before the PIT is opened. Each refresh uploads its own BCC (UPLOAD_MAX=1), so files
        // from these rounds carry a different timestamp range than the initial flush files.
        for (int i = 0; i < extraIndexingRounds; i++) {
            final long extraRoundTimestamp = timestamp + i + 1;
            indexDocs(
                indexName,
                between(10, 50),
                UnaryOperator.identity(),
                null,
                () -> Map.of("@timestamp", extraRoundTimestamp, "field", randomAlphaOfLength(10))
            );
            refreshAndAwaitSearchNodeCommit(indexName, commitService, shardId);
        }

        // Capture the expected per-file timestamp ranges from the source SearchDirectory before the PIT opens.
        final Map<String, BlobFileRanges> expectedMetadata;
        {
            final IndexShard searchShard = findSearchShard(indexName);
            final var sourceSearchDirectory = SearchDirectory.unwrapDirectory(searchShard.store().directory());
            try (var commitRef = searchShard.acquireLastIndexCommit(false)) {
                expectedMetadata = sourceSearchDirectory.getBlobFileRangesForFiles(commitRef.getIndexCommit().getFileNames());
            }
        }

        final var pitId = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId);
        try {

            // Randomly force-merge to a single segment and flush. With extraIndexingRounds > 0 there are multiple Lucene segments,
            // so the force-merge is not a no-op and produces a newer commit (B) in the object store.
            // The search node applies commit B while the PIT remains pinned to commit A, which is the scenario this block exercises.
            if (randomBoolean() && extraIndexingRounds > 0) {
                final long priorGen = lastUploadedGeneration(commitService, shardId);
                indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get();
                flush(indexName);
                awaitSearchNodeCommit(indexName, priorGen + 1);
            }

            final var capturedInfos = relocateFromNodeAndCapturePITContextInfos(searchNodeA, indexName);
            assertThat(capturedInfos, hasSize(1));

            final Map<String, BlobFileRanges> pitMetadata = capturedInfos.getFirst().metadata();
            assertThat("metadata must be non-empty", pitMetadata, not(anEmptyMap()));
            // Every file in the wire payload must carry the exact timestamp range that the source
            // SearchDirectory recorded for it — the value captured in expectedMetadata above.
            assertThat(pitMetadata.keySet(), equalTo(expectedMetadata.keySet()));
            pitMetadata.forEach((fileName, ranges) -> {
                final BlobFileRanges expected = expectedMetadata.get(fileName);
                assertThat("source SearchDirectory must carry metadata for " + fileName, expected, notNullValue());
                assertThat(
                    "source SearchDirectory must carry a non-null timestamp range for " + fileName,
                    expected.timestampRange(),
                    notNullValue()
                );
                assertThat(
                    "wire timestamp range for " + fileName + " must match source SearchDirectory",
                    ranges.timestampRange(),
                    equalTo(expected.timestampRange())
                );
            });

            /// [SearchDirectory#mergeMetadata] (invoked via [SearchDirectory#mergePITReaderMetadata]) must merge every transferred
            /// range into the destination's own metadata.
            pitMetadata.forEach((fileName, wireRanges) -> {
                final BlobFileRanges mergedRanges = getSearchDirectoryBlobFileRanges(indexName, fileName);
                assertThat("destination SearchDirectory must know about transferred file: " + fileName, mergedRanges, notNullValue());
                assertThat(
                    "merged timestamp range for " + fileName + " must match the transferred range",
                    mergedRanges.timestampRange(),
                    equalTo(wireRanges.timestampRange())
                );
            });
        } finally {
            closeRelocatedPointInTime(pitId);
        }
    }

    /// Verifies that when a generational file's blob location differs between the
    /// [SearchDirectory]'s pinned entry and the commit the PIT was opened at,
    /// the wire payload stamps the file with the CC's own timestamp — not the old pinned one.
    ///
    /// Scenario (UPLOAD_MAX=1, every explicit flush → its own single-commit BCC blob):
    ///
    ///   1. Flush A: initial docs with `tsA` → segment `_0`, all files internal to
    ///     BCC_A. SearchDirectory entries for all files carry `tsA`.
    ///   2. Flush B: re-index one doc with `tsB`. Lucene soft-deletes the old version via a
    ///     generational live-docs file (e.g. `_0_1.liv`) and writes the new version into a
    ///     new segment `_1`. Because BCC_B is a single-commit VBCC, `_0_1.liv` is
    ///     written as an _internal_ file into BCC_B. SearchDirectory pins it at BCC_B's
    ///     offset with timestamp `tsB`.
    ///   3. Flush C: add one new doc with `tsC` → new segment `_2`. Commit C still
    ///     references `_0_1.liv` (segment `_0` has not been merged away). Because
    ///     CC_C is again the first (and only) commit in VBCC_C, `_0_1.liv` is re-copied
    ///     into BCC_C at a _new_ offset. SearchDirectory's `putIfAbsent` keeps the
    ///     BCC_B pin.
    ///   4. PIT opened at commit C. `overrideBlobFileRangesTimestamp` detects the location
    ///     mismatch for `_0_1.liv` (BCC_B in SearchDirectory vs BCC_C in CC_C) and stamps
    ///     the wire payload entry with CC_C's timestamp range `[tsC, tsC]`.
    public void testPitRelocationTransfersTimestampForGenerationalFileWithChangedBlobLocation() {
        assumeTrue("Requires pit relocation feature flag", PIT_RELOCATION_FEATURE_FLAG.isEnabled());
        final var indexNode = startMasterAndIndexNode();
        final var searchNodeA = startSearchNode();
        final var searchNodeB = startSearchNode();

        final var indexName = randomIdentifier();
        assertAcked(
            prepareCreate(indexName).setSettings(
                indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1)
                    .put("index.routing.allocation.exclude._name", searchNodeB)
            ).setMapping("@timestamp", "type=date")
        );
        ensureGreen(indexName);

        final var commitService = internalCluster().getInstance(StatelessCommitService.class, indexNode);
        final var shardId = new ShardId(resolveIndex(indexName), 0);

        final long tsA = randomLongBetween(1, MAX_MILLIS_BEFORE_9999 / 3);
        final long tsB = randomLongBetween(tsA + 1, MAX_MILLIS_BEFORE_9999 * 2 / 3);
        final long tsC = randomLongBetween(tsB + 1, MAX_MILLIS_BEFORE_9999);

        // Flush A: create segment _0 with initial docs.
        final var bulkResponse = indexDocs(
            indexName,
            randomIntBetween(50, 100),
            UnaryOperator.identity(),
            null,
            () -> Map.of("@timestamp", tsA, "field", randomAlphaOfLength(10))
        );
        final List<String> docIds = Stream.of(bulkResponse.getItems()).map(BulkItemResponse::getId).toList();
        flushAndAwaitSearchNodeCommit(indexName, commitService, shardId);

        // Flush B: re-index one doc from segment _0. Lucene creates a live-docs generational file
        // (e.g. _0_1.liv) to soft-delete the old version and writes the new version into segment _1.
        // Because BCC_B is a single-commit VBCC, _0_1.liv is written as an internal file of BCC_B.
        // SearchDirectory pins _0_1.liv to BCC_B's blob location with timestamp tsB.
        client().prepareIndex(indexName)
            .setId(randomFrom(docIds))
            .setSource(Map.of("@timestamp", tsB, "field", randomAlphaOfLength(10)))
            .get();
        flushAndAwaitSearchNodeCommit(indexName, commitService, shardId);

        // Flush C: add one new doc with tsC → new segment _2. _0_1.liv is still part of commit C
        // (segment _0 has not been merged away). Because CC_C is the first commit in VBCC_C,
        // _0_1.liv is treated as internal and re-copied into BCC_C at a new offset.
        // SearchDirectory's putIfAbsent keeps the original BCC_B pin.
        indexDocs(indexName, 1, UnaryOperator.identity(), null, () -> Map.of("@timestamp", tsC, "field", randomAlphaOfLength(10)));
        flushAndAwaitSearchNodeCommit(indexName, commitService, shardId);

        // Open PIT at commit C. overrideBlobFileRangesTimestamp detects the location mismatch for
        // _0_1.liv (BCC_B in SearchDirectory vs BCC_C in CC_C) and stamps it with CC_C's tsC.
        final var pitId = openPointInTime(indexName, TimeValue.timeValueMinutes(1)).getPointInTimeId();
        assertNotNull(pitId);
        try {
            final List<OpenPITContextInfo> capturedInfos = relocateFromNodeAndCapturePITContextInfos(searchNodeA, indexName);
            assertThat(capturedInfos, hasSize(1));
            final var info = capturedInfos.getFirst();

            // There must be at least one generational live-docs file in the metadata (created in flush B).
            final Map<String, BlobFileRanges> genFilesMetadata = info.metadata()
                .entrySet()
                .stream()
                .filter(e -> StatelessCompoundCommit.isGenerationalFile(e.getKey()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
            assertThat("expect at least one generational file after re-indexing a doc", genFilesMetadata, not(anEmptyMap()));
            assertThat(
                "expect all generational files should have the tsC timestamp",
                genFilesMetadata.values()
                    .stream()
                    .map(BlobFileRanges::timestampRange)
                    .allMatch(new TimestampFieldValueRange(tsC, tsC)::equals),
                is(true)
            );

            /// [SearchDirectory#mergeMetadata] must adopt CC_C's overridden range on the destination node too;
            /// otherwise the fix would only be visible on the wire and never reach the merged metadata that
            /// SearchDirectory actually uses to serve reads.
            final var genFilesWithTimestampC = genFilesMetadata.entrySet()
                .stream()
                .filter(e -> Objects.equals(e.getValue().timestampRange(), new TimestampFieldValueRange(tsC, tsC)))
                .toList();
            for (final var entry : genFilesWithTimestampC) {
                final BlobFileRanges mergedRanges = getSearchDirectoryBlobFileRanges(indexName, entry.getKey());
                assertThat("destination SearchDirectory must know about " + entry.getKey(), mergedRanges, notNullValue());
                assertThat(mergedRanges.timestampRange(), equalTo(new TimestampFieldValueRange(tsC, tsC)));
            }
        } finally {
            closeRelocatedPointInTime(pitId);
        }
    }

    private List<OpenPITContextInfo> relocateFromNodeAndCapturePITContextInfos(final String searchNodeA, final String indexName) {
        final var capturedInfos = new ArrayList<OpenPITContextInfo>();
        final var handoffLatch = new CountDownLatch(1);
        MockTransportService.getInstance(searchNodeA)
            .addRequestHandlingBehavior(
                START_HANDOFF_ACTION_NAME,
                (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                    @Override
                    public String getProfileName() {
                        return channel.getProfileName();
                    }

                    @Override
                    public void sendResponse(TransportResponse response) {
                        if (response instanceof TransportStatelessUnpromotableRelocationAction.RelocationHandoffResponse r) {
                            capturedInfos.addAll(r.getOpenPITContextInfos());
                        }
                        handoffLatch.countDown();
                        channel.sendResponse(response);
                    }

                    @Override
                    public void sendResponse(Exception exception) {
                        handoffLatch.countDown();
                        channel.sendResponse(exception);
                    }
                }, task)
            );

        updateIndexSettings(Settings.builder().put("index.routing.allocation.exclude._name", searchNodeA), indexName);
        safeAwait(handoffLatch);
        ensureGreen(indexName);
        return List.copyOf(capturedInfos);
    }

    /// Reads the merged [BlobFileRanges] for `fileName` directly from the destination search
    /// shard's [SearchDirectory], i.e. the state produced by [SearchDirectory#mergePITReaderMetadata] rather than
    /// what was merely sent over the wire during the relocation handoff.
    private static BlobFileRanges getSearchDirectoryBlobFileRanges(String indexName, String fileName) {
        return SearchDirectory.unwrapDirectory(findSearchShard(indexName).store().directory()).getBlobFileRangesForFile(fileName);
    }

    private void refreshAndAwaitSearchNodeCommit(String indexName, StatelessCommitService commitService, ShardId shardId) {
        final long priorGen = lastUploadedGeneration(commitService, shardId);
        refresh(indexName);
        awaitSearchNodeCommit(indexName, priorGen + 1);
    }

    private void flushAndAwaitSearchNodeCommit(String indexName, StatelessCommitService commitService, ShardId shardId) {
        final long priorGen = lastUploadedGeneration(commitService, shardId);
        flush(indexName);
        awaitSearchNodeCommit(indexName, priorGen + 1);
    }

    private static long lastUploadedGeneration(final StatelessCommitService commitService, final ShardId shardId) {
        final BatchedCompoundCommit prior = commitService.getLatestUploadedBcc(shardId);
        return prior != null ? prior.lastCompoundCommit().generation() : -1L;
    }

    private static void awaitSearchNodeCommit(final String indexName, final long gen) {
        final var primaryTerm = findIndexShard(indexName).getOperationPrimaryTerm();
        final var searchEngine = getShardEngine(findSearchShard(indexName), SearchEngine.class);
        final var listener = new SubscribableListener<Long>();
        searchEngine.addPrimaryTermAndGenerationListener(primaryTerm, gen, listener);
        safeAwait(listener);
    }

    private OpenPointInTimeResponse openPointInTime(String index, TimeValue keepAlive) {
        return client().execute(TransportOpenPointInTimeAction.TYPE, new OpenPointInTimeRequest(index).keepAlive(keepAlive)).actionGet();
    }

    private void closePointInTime(BytesReference readerId) {
        client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(readerId)).actionGet();
    }

    /// A PIT id encodes, per shard, the node it was last seen on; that mapping is only refreshed by a
    /// search response. Closing a PIT with the pre-relocation id after the shard has already relocated
    /// targets the old node, leaving the reader context created on the new node during the handoff
    /// unreleased. So we must search once to obtain the post-relocation id before closing.
    private void closeRelocatedPointInTime(BytesReference pitId) {
        final var updatedPitId = new AtomicReference<BytesReference>();
        assertResponse(prepareSearch().setPointInTime(new PointInTimeBuilder(pitId)), resp -> updatedPitId.set(resp.pointInTimeId()));
        closePointInTime(updatedPitId.get());
    }
}
