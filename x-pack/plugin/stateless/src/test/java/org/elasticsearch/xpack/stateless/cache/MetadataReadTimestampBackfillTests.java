/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.InternalFilesReplicatedRanges;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit.TimestampFieldValueRange;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;
import org.elasticsearch.xpack.stateless.lucene.FileCacheKey;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MetadataReadTimestampBackfillTests extends ESTestCase {

    private static final int REGION_SIZE = 100;

    private StatelessSharedBlobCacheService cacheService;
    private FileCacheKey cacheKey;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        cacheService = mock(StatelessSharedBlobCacheService.class);
        cacheKey = new FileCacheKey(new ShardId(new Index("index", "uuid"), 0), 1L, "blob");
        when(cacheService.getEndingRegion(anyLong())).thenAnswer(inv -> {
            final long pos = inv.getArgument(0);
            return (int) ((pos - (pos % REGION_SIZE == 0 ? 1 : 0)) / REGION_SIZE);
        });
    }

    private MetadataReadTimestampBackfill backfill(boolean indexHasTimestampField) {
        return new MetadataReadTimestampBackfill(cacheService, cacheKey, indexHasTimestampField);
    }

    public void testMergeCcKeepsMostRecent() {
        final var backfill = backfill(randomBoolean());
        backfill.mergeCc(0L, cc(1L, 50L, 50L, new TimestampFieldValueRange(9000L, 9000L)));
        backfill.mergeCc(100L, cc(2L, 50L, 50L, new TimestampFieldValueRange(1000L, 1000L))); // older, must not win
        assertThat(backfill.mostRecentTimestamp(), equalTo(9000L));
    }

    public void testMergeCcTracksDeepestCommit() {
        final var backfill = backfill(randomBoolean());
        assertThat(backfill.lastCommitOffset(), equalTo(-1L));
        backfill.mergeCc(0L, cc(1L, 50L, 50L, new TimestampFieldValueRange(1000L, 1000L)));
        assertThat(backfill.lastCommitOffset(), equalTo(0L));
        backfill.mergeCc(100L, cc(2L, 50L, 50L, new TimestampFieldValueRange(1000L, 1000L)));
        assertThat(backfill.lastCommitOffset(), equalTo(100L));
    }

    public void testMergeBccFoldsAllCommits() {
        final var bcc = new BatchedCompoundCommit(
            new PrimaryTermAndGeneration(1L, 1L),
            List.of(
                cc(1L, 100L, 10L, new TimestampFieldValueRange(1000L, 1000L)),
                cc(2L, 100L, 10L, new TimestampFieldValueRange(9000L, 9000L))
            )
        );

        final var backfill = backfill(randomBoolean());
        backfill.mergeCc(bcc);

        assertThat(backfill.mostRecentTimestamp(), equalTo(9000L));
    }

    public void testNullRangeKeepsUnknown() {
        final var backfill = backfill(randomBoolean());
        backfill.mergeCc(0L, cc(1L, 50L, 50L, null));
        assertThat(backfill.mostRecentTimestamp(), equalTo(SharedBlobCacheService.UNKNOWN_TIMESTAMP));
    }

    public void testRealRangeWinsOverNullRange() {
        final var backfill = backfill(randomBoolean());
        backfill.mergeCc(0L, cc(1L, 50L, 50L, null));
        backfill.mergeCc(100L, cc(2L, 50L, 50L, new TimestampFieldValueRange(5000L, 5000L)));
        assertThat(backfill.mostRecentTimestamp(), equalTo(5000L));
    }

    public void testFinalizeBackfillsMostRecentOverPopulatedRange() {
        final var backfill = backfill(randomBoolean());
        // Single commit at offset 0 with a header spanning regions 0..1.
        backfill.mergeCc(0L, cc(1L, 150L, 150L, new TimestampFieldValueRange(7000L, 7000L)));

        backfill.finalizeAndBackfill();

        verify(cacheService).backfillRegionTimestamps(eq(cacheKey), eq(0), eq(1), eq(7000L));
    }

    public void testFinalizeBoundsToLastCommitNotWholeBlob() {
        final var backfill = backfill(randomBoolean());
        // A small leading commit and a large last commit whose body is never read.
        backfill.mergeCc(0L, cc(1L, 50L, 50L, new TimestampFieldValueRange(1000L, 1000L)));
        backfill.mergeCc(100L, cc(2L, 1000L, 50L, new TimestampFieldValueRange(9000L, 9000L)));

        backfill.finalizeAndBackfill();

        // With assertions enabled the last commit's trailing (padding) region is covered: endingRegion(100 + 1000) = region 10.
        verify(cacheService).backfillRegionTimestamps(eq(cacheKey), eq(0), eq(10), eq(9000L));
    }

    public void testFinalizeUnresolvedTimeBasedIndexEvictsFirst() {
        final var backfill = backfill(true); // time-based index (has @timestamp)
        backfill.mergeCc(0L, cc(1L, 50L, 50L, null)); // region 0 only, no resolvable timestamp

        backfill.finalizeAndBackfill();

        // Unresolved timestamp for a time-based index: stamp the oldest possible timestamp so the regions are evicted first.
        verify(cacheService).backfillRegionTimestamps(eq(cacheKey), eq(0), eq(0), eq(1L));
    }

    public void testFinalizeUnresolvedNonTimeBasedIndexLeavesPinned() {
        final var backfill = backfill(false); // non-time-based index (no @timestamp)
        backfill.mergeCc(0L, cc(1L, 50L, 50L, null)); // region 0 only, no resolvable timestamp

        backfill.finalizeAndBackfill();

        // A non-time-based index leaves the regions with their UNKNOWN timestamp (pinned): no backfill happens.
        verify(cacheService, never()).backfillRegionTimestamps(any(), anyInt(), anyInt(), anyLong());
    }

    public void testFinalizeNothingReadIsNoop() {
        final var backfill = backfill(randomBoolean());
        backfill.finalizeAndBackfill();
        verify(cacheService, never()).backfillRegionTimestamps(any(), anyInt(), anyInt(), anyLong());
    }

    private static StatelessCompoundCommit cc(long generation, long sizeInBytes, long headerSizeInBytes, TimestampFieldValueRange range) {
        return new StatelessCompoundCommit(
            new ShardId(new Index("index", "uuid"), 0),
            new PrimaryTermAndGeneration(1L, generation),
            0L, // translogRecoveryStartFile (not the hollow sentinel)
            "node", // nodeEphemeralId (non-empty for a non-hollow commit)
            Map.of(), // commitFiles
            sizeInBytes,
            Set.of(), // internalFiles
            headerSizeInBytes,
            InternalFilesReplicatedRanges.EMPTY,
            Map.of(), // extraContent
            range
        );
    }
}
