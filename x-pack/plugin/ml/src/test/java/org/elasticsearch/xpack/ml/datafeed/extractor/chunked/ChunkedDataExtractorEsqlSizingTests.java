/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDatafeedQuery;

import java.util.Optional;
import java.util.OptionalLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class ChunkedDataExtractorEsqlSizingTests extends ESTestCase {

    private static final long GROUPING_INTERVAL_MILLIS = 1_000L;

    private static long representativeChunkEnd() {
        long end = GROUPING_INTERVAL_MILLIS;
        for (int i = 0; i < ChunkedDataExtractorEsqlSizing.MAX_ESQL_SHRINK_ATTEMPTS; i++) {
            end = Math.multiplyExact(end, ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR);
        }
        return end;
    }

    public void testTruncatedChunkShouldShrinkByHeadroomFactor() {
        long intervalEnd = representativeChunkEnd();
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        OptionalLong shrunkEnd = truncated(sizing, 0L, intervalEnd);
        assertThat(shrunkEnd.isPresent(), is(true));
        long expectedUnaligned = intervalEnd / ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR;
        assertThat(shrunkEnd.getAsLong(), lessThan(intervalEnd));
        assertThat(shrunkEnd.getAsLong(), equalTo(expectedUnaligned));
    }

    public void testShrinkBelowGroupingIntervalShouldClampToOneInterval() {
        long initialEnd = GROUPING_INTERVAL_MILLIS * ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR;
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        OptionalLong shrunkEnd = truncated(sizing, 0L, initialEnd);
        assertThat(shrunkEnd, equalTo(OptionalLong.of(GROUPING_INTERVAL_MILLIS)));
    }

    public void testTruncatedChunkAtOneGroupingIntervalShouldBeRecordedIncomplete() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        long end = GROUPING_INTERVAL_MILLIS;
        assertThat(truncated(sizing, 0L, end), equalTo(OptionalLong.empty()));
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, end))));
    }

    public void testShrinkAfterMaxAttemptsShouldBeRecordedIncomplete() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        long intervalEnd = representativeChunkEnd();
        for (int i = 0; i < ChunkedDataExtractorEsqlSizing.MAX_ESQL_SHRINK_ATTEMPTS; i++) {
            assertThat(truncated(sizing, 0L, intervalEnd).isPresent(), is(true));
        }
        assertThat(sizing.incompleteInterval(), equalTo(Optional.empty()));
        assertThat(truncated(sizing, 0L, intervalEnd), equalTo(OptionalLong.empty()));
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, intervalEnd))));
    }

    public void testNewChunkShouldResetShrinkAttempts() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        long intervalEnd = representativeChunkEnd();
        for (int i = 0; i < ChunkedDataExtractorEsqlSizing.MAX_ESQL_SHRINK_ATTEMPTS; i++) {
            truncated(sizing, 0L, intervalEnd);
        }
        sizing.onNewChunk();
        assertThat(truncated(sizing, 0L, intervalEnd).isPresent(), is(true));
    }

    public void testCancelledTruncatedChunkShouldNotShrinkAndBeRecordedIncomplete() {
        long intervalEnd = representativeChunkEnd();
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        OptionalLong shrunkEnd = sizing.onTruncated(0L, intervalEnd, true, new SearchInterval(0L, intervalEnd));
        assertThat(shrunkEnd, equalTo(OptionalLong.empty()));
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, intervalEnd))));
    }

    public void testSparseChunkAfterShrinkShouldKeepSpan() {
        long intervalEnd = representativeChunkEnd();
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(intervalEnd);
        OptionalLong shrunkEnd = truncated(sizing, 0L, intervalEnd);
        assertThat(shrunkEnd.isPresent(), is(true));
        assertThat(sizing.regrownSpan(shrunkEnd.getAsLong(), 500L), equalTo(shrunkEnd.getAsLong()));
    }

    public void testSparseChunkWithoutShrinkShouldRegrowTowardTargetRowCountCappedAtInitialSpan() {
        long initialSpan = representativeChunkEnd();
        long chunkSpan = initialSpan / ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR;
        ChunkedDataExtractorContext.TimeAligner aligner = ChunkedDataExtractorFactory.newIntervalTimeAligner(GROUPING_INTERVAL_MILLIS);
        long targetDocs = ChunkedDataExtractorEsqlSizing.DEFAULT_ESQL_CHUNK_DOCS;
        long scaled = (chunkSpan * targetDocs) / 500L;
        long expected = aligner.alignToCeil(Math.min(initialSpan, scaled));
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(initialSpan);
        assertThat(sizing.regrownSpan(chunkSpan, 500L), equalTo(expected));
    }

    public void testSparseChunkJustUnderTargetShouldNotDoubleSpan() {
        long initialSpan = representativeChunkEnd();
        ChunkedDataExtractorContext.TimeAligner aligner = ChunkedDataExtractorFactory.newIntervalTimeAligner(GROUPING_INTERVAL_MILLIS);
        long chunkSpan = initialSpan / ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR;
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(initialSpan);
        long rowCount = ChunkedDataExtractorEsqlSizing.DEFAULT_ESQL_CHUNK_DOCS - 1;
        long scaled = (chunkSpan * ChunkedDataExtractorEsqlSizing.DEFAULT_ESQL_CHUNK_DOCS) / rowCount;
        long expected = aligner.alignToCeil(Math.min(initialSpan, scaled));
        assertThat(sizing.regrownSpan(chunkSpan, rowCount), equalTo(expected));
        assertThat(sizing.regrownSpan(chunkSpan, rowCount), lessThan(chunkSpan * ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR));
    }

    public void testVerySparseChunkShouldRegrowUpToInitialSpan() {
        long initialSpan = representativeChunkEnd();
        long chunkSpan = initialSpan / ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR;
        ChunkedDataExtractorContext.TimeAligner aligner = ChunkedDataExtractorFactory.newIntervalTimeAligner(GROUPING_INTERVAL_MILLIS);
        long scaled = (chunkSpan * ChunkedDataExtractorEsqlSizing.DEFAULT_ESQL_CHUNK_DOCS) / 1_000L;
        long expected = aligner.alignToCeil(Math.min(initialSpan, scaled));
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(initialSpan);
        assertThat(sizing.regrownSpan(chunkSpan, 1_000L), equalTo(expected));
        assertThat(expected, equalTo(initialSpan));
    }

    public void testTwoTruncatedIntervalsShouldMergeIntoCoveringSpan() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        truncated(sizing, 1_000L, 2_000L);
        truncated(sizing, 0L, 1_000L);
        truncated(sizing, 2_000L, 3_000L);
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, 3_000L))));
    }

    public void testLargeIntervalShouldShrinkByHeadroomFactorAlignedDown() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        long intervalLength = Long.MAX_VALUE / 4L;
        OptionalLong shrunkEnd = truncated(sizing, 0L, intervalLength);
        long headroom = ChunkedDataExtractorEsqlSizing.CHUNK_HEADROOM_FACTOR;
        long expected = intervalLength / headroom - (intervalLength / headroom) % GROUPING_INTERVAL_MILLIS;
        assertThat(shrunkEnd, equalTo(OptionalLong.of(expected)));
    }

    public void testUserLimitShouldNotCountAsTruncated() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(false);
        assertThat(sizing.isTruncated(EsqlDatafeedQuery.INJECTED_ROW_LIMIT), is(false));
    }

    private static OptionalLong truncated(ChunkedDataExtractorEsqlSizing sizing, long start, long end) {
        return sizing.onTruncated(start, end, false, new SearchInterval(start, end));
    }

    private static ChunkedDataExtractorEsqlSizing esqlSizing(boolean injectsRowLimit) {
        ChunkedDataExtractorContext.TimeAligner timeAligner = ChunkedDataExtractorFactory.newIntervalTimeAligner(GROUPING_INTERVAL_MILLIS);
        ChunkedDataExtractorContext context = new ChunkedDataExtractorContext(
            "job",
            1000,
            0L,
            1_000_000L,
            null,
            timeAligner,
            false,
            null,
            true,
            injectsRowLimit
        );
        return new ChunkedDataExtractorEsqlSizing(context);
    }
}
