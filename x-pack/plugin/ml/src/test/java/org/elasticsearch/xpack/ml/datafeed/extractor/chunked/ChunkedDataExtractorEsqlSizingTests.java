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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ChunkedDataExtractorEsqlSizingTests extends ESTestCase {

    private static final long GROUPING_INTERVAL_MILLIS = 1_000L;

    public void testTruncatedChunkShouldShrinkToOneFifthAlignedDown() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(60_000L);
        long shrunkEnd = sizing.shrunkEnd(0L, 60_000L, EsqlDataExtractor.INJECTED_ROW_LIMIT);
        assertThat(shrunkEnd, equalTo(12_000L));
    }

    public void testShrinkBelowGroupingIntervalShouldClampToOneInterval() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        long shrunkEnd = sizing.shrunkEnd(0L, 2_000L, EsqlDataExtractor.INJECTED_ROW_LIMIT);
        assertThat(shrunkEnd, equalTo(1_000L));
    }

    public void testShrinkAfterMaxAttemptsShouldNotBeAllowed() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        for (int i = 0; i < 5; i++) {
            assertThat(sizing.canShrink(0L, 10_000L), is(true));
            sizing.onShrunk();
        }
        assertThat(sizing.canShrink(0L, 10_000L), is(false));
        sizing.onShrunk();
        assertThat(sizing.canShrink(0L, 10_000L), is(false));
    }

    public void testSparseChunkAfterShrinkShouldKeepSpan() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(60_000L);
        sizing.onShrunk();
        assertThat(sizing.regrownSpan(12_000L, 500L), equalTo(12_000L));
    }

    public void testSparseChunkWithoutShrinkShouldRegrowFivefoldCappedAtInitialSpan() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.captureInitialChunkSpan(60_000L);
        assertThat(sizing.regrownSpan(12_000L, 500L), equalTo(60_000L));
    }

    public void testTwoTruncatedIntervalsShouldMergeIntoCoveringSpan() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        sizing.recordIncomplete(new SearchInterval(1_000L, 2_000L));
        sizing.recordIncomplete(new SearchInterval(0L, 3_000L));
        assertThat(sizing.incompleteInterval(), equalTo(Optional.of(new SearchInterval(0L, 3_000L))));
    }

    public void testLargeIntervalScalingShouldNotOverflow() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(true);
        long start = 0L;
        long intervalLength = Long.MAX_VALUE / 4L;
        long end = start + intervalLength;
        long rowCount = EsqlDataExtractor.INJECTED_ROW_LIMIT;
        long targetLength = intervalLength / rowCount * ChunkedDataExtractorEsqlSizing.DEFAULT_ESQL_CHUNK_DOCS + intervalLength % rowCount
            * ChunkedDataExtractorEsqlSizing.DEFAULT_ESQL_CHUNK_DOCS / rowCount;
        long expectedEnd = ChunkedDataExtractorFactory.newIntervalTimeAligner(GROUPING_INTERVAL_MILLIS).alignToFloor(start + targetLength);
        assertThat(sizing.shrunkEnd(start, end, rowCount), equalTo(expectedEnd));
    }

    public void testUserLimitShouldNotCountAsTruncated() {
        ChunkedDataExtractorEsqlSizing sizing = esqlSizing(false);
        assertThat(sizing.isTruncated(EsqlDataExtractor.INJECTED_ROW_LIMIT), is(false));
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
