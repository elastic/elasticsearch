/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor;

import java.util.Optional;
import java.util.OptionalLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ChunkedDataExtractorEsqlSizingTests extends ESTestCase {

    private static final long GROUPING_INTERVAL_MILLIS = 1_000L;

    public void testTruncatedChunkShouldShrinkToOneFifthAlignedDown() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        OptionalLong shrunkEnd = truncated(sizing, 0L, 60_000L);
        assertThat(shrunkEnd, equalTo(OptionalLong.of(12_000L)));
    }

    public void testShrinkBelowGroupingIntervalShouldClampToOneInterval() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        OptionalLong shrunkEnd = truncated(sizing, 0L, 2_000L);
        assertThat(shrunkEnd, equalTo(OptionalLong.of(1_000L)));
    }

    public void testTruncatedChunkAtOneGroupingIntervalShouldBeRecordedIncomplete() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        assertThat(truncated(sizing, 0L, 1_000L), equalTo(OptionalLong.empty()));
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, 1_000L))));
    }

    public void testShrinkAfterMaxAttemptsShouldBeRecordedIncomplete() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        for (int i = 0; i < 5; i++) {
            assertThat(truncated(sizing, 0L, 10_000L).isPresent(), is(true));
        }
        assertThat(sizing.incompleteInterval(), equalTo(Optional.empty()));
        assertThat(truncated(sizing, 0L, 10_000L), equalTo(OptionalLong.empty()));
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, 10_000L))));
    }

    public void testNewChunkShouldResetShrinkAttempts() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        for (int i = 0; i < 5; i++) {
            truncated(sizing, 0L, 10_000L);
        }
        sizing.onNewChunk();
        assertThat(truncated(sizing, 0L, 10_000L).isPresent(), is(true));
    }

    public void testCancelledTruncatedChunkShouldNotShrinkAndBeRecordedIncomplete() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        OptionalLong shrunkEnd = sizing.onTruncated(0L, 60_000L, true, new SearchInterval(0L, 60_000L));
        assertThat(shrunkEnd, equalTo(OptionalLong.empty()));
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, 60_000L))));
    }

    public void testSparseChunkAfterShrinkShouldKeepSpan() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(60_000L);
        truncated(sizing, 0L, 60_000L);
        assertThat(sizing.regrownSpan(12_000L, 500L), equalTo(12_000L));
    }

    public void testSparseChunkWithoutShrinkShouldRegrowFivefoldCappedAtInitialSpan() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(60_000L);
        assertThat(sizing.regrownSpan(12_000L, 500L), equalTo(60_000L));
    }

    public void testTwoTruncatedIntervalsShouldMergeIntoCoveringSpan() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        truncated(sizing, 1_000L, 2_000L);
        truncated(sizing, 0L, 1_000L);
        truncated(sizing, 2_000L, 3_000L);
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, 3_000L))));
    }

    public void testLargeIntervalShouldShrinkToOneFifthAlignedDown() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        long intervalLength = Long.MAX_VALUE / 4L;
        long expectedEnd = intervalLength / 5L - (intervalLength / 5L) % GROUPING_INTERVAL_MILLIS;
        assertThat(truncated(sizing, 0L, intervalLength), equalTo(OptionalLong.of(expectedEnd)));
    }

    public void testUserLimitShouldNotCountAsTruncated() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(false);
        assertThat(sizing.isTruncated(EsqlDataExtractor.INJECTED_ROW_LIMIT), is(false));
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
