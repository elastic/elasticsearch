/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDatafeedQuery;

import java.util.Optional;
import java.util.OptionalLong;

/** ES|QL chunk shrink, regrow, and incomplete-interval state; touched only from the consuming thread. */
final class ChunkedDataExtractorEsqlSizing {

    /**
     * Burst headroom when sizing an initial ES|QL chunk: target {@link #DEFAULT_ESQL_CHUNK_DOCS} rows per chunk, which is
     * {@link EsqlDatafeedQuery#INJECTED_ROW_LIMIT} divided by this factor.
     */
    static final long CHUNK_HEADROOM_FACTOR = 2L;

    /**
     * Target rows per initial ES|QL chunk when chunk span is auto. ES|QL has no scroll; oversized chunks truncate at
     * {@link EsqlDatafeedQuery#INJECTED_ROW_LIMIT}.
     */
    static final long DEFAULT_ESQL_CHUNK_DOCS = EsqlDatafeedQuery.INJECTED_ROW_LIMIT / CHUNK_HEADROOM_FACTOR;

    /**
     * Maximum ES|QL shrink retries per chunk after a result hits {@link EsqlDatafeedQuery#INJECTED_ROW_LIMIT}; the interval shrinks by
     * at least {@link #CHUNK_HEADROOM_FACTOR} except where the one-grouping-interval floor applies.
     */
    static final int MAX_ESQL_SHRINK_ATTEMPTS = 6;

    private final ChunkedDataExtractorContext.TimeAligner timeAligner;
    private final boolean injectsEsqlRowLimit;
    private long initialChunkSpan;
    private int shrinkAttempts;
    @Nullable
    private SearchInterval incompleteSearchInterval;

    ChunkedDataExtractorEsqlSizing(ChunkedDataExtractorContext context) {
        this.timeAligner = context.timeAligner();
        this.injectsEsqlRowLimit = context.injectsEsqlRowLimit();
    }

    void captureInitialChunkSpan(long chunkSpan) {
        initialChunkSpan = chunkSpan;
    }

    boolean isTruncated(long rowCount) {
        return injectsEsqlRowLimit && rowCount >= EsqlDatafeedQuery.INJECTED_ROW_LIMIT;
    }

    /** Returns the shrunk end to retry the chunk with, or empty after recording {@code interval} as incomplete. */
    OptionalLong onTruncated(long currentStart, long currentEnd, boolean cancelled, SearchInterval interval) {
        // A cancelled extractor must not start new queries, so a truncated result is accepted as read.
        if (cancelled || canShrink(currentStart, currentEnd) == false) {
            recordIncomplete(interval);
            return OptionalLong.empty();
        }
        long newEnd = shrunkEnd(currentStart, currentEnd);
        shrinkAttempts++;
        return OptionalLong.of(newEnd);
    }

    private boolean canShrink(long currentStart, long currentEnd) {
        long intervalLength = currentEnd - currentStart;
        return shrinkAttempts < MAX_ESQL_SHRINK_ATTEMPTS && intervalLength > minChunkLength();
    }

    private long shrunkEnd(long currentStart, long currentEnd) {
        assert shrinkAttempts < MAX_ESQL_SHRINK_ATTEMPTS;
        long intervalLength = currentEnd - currentStart;
        // A truncated result has exactly INJECTED_ROW_LIMIT rows, so aiming for DEFAULT_ESQL_CHUNK_DOCS rows scales by 1 / headroom.
        long targetLength = Math.max(1L, intervalLength / CHUNK_HEADROOM_FACTOR);
        long shrunkEnd = timeAligner.alignToFloor(currentStart + targetLength);
        if (shrunkEnd <= currentStart) {
            shrunkEnd = currentStart + minChunkLength();
        }
        long newEnd = Math.min(shrunkEnd, currentEnd);
        assert currentStart < newEnd;
        return newEnd;
    }

    long alignChunkSpanAfterShrink(long chunkSpan, long currentStart, long currentEnd) {
        long shrunkLength = currentEnd - currentStart;
        return timeAligner.alignToCeil(Math.min(chunkSpan, shrunkLength));
    }

    long regrownSpan(long chunkSpan, long rowCount) {
        if (hasShrunkCurrentChunk()) {
            // The shrunk retry already proved a larger span truncates; regrowing here would waste shrink retries on the next chunk.
            return chunkSpan;
        }
        if (rowCount <= 0 || rowCount >= DEFAULT_ESQL_CHUNK_DOCS || initialChunkSpan <= 0) {
            return chunkSpan;
        }
        long scaledSpan = (chunkSpan * DEFAULT_ESQL_CHUNK_DOCS) / rowCount;
        return timeAligner.alignToCeil(Math.min(initialChunkSpan, scaledSpan));
    }

    private void recordIncomplete(SearchInterval interval) {
        if (incompleteSearchInterval == null) {
            incompleteSearchInterval = interval;
            return;
        }
        incompleteSearchInterval = new SearchInterval(
            Math.min(incompleteSearchInterval.startMs(), interval.startMs()),
            Math.max(incompleteSearchInterval.endMs(), interval.endMs())
        );
    }

    Optional<SearchInterval> incompleteInterval() {
        return Optional.ofNullable(incompleteSearchInterval);
    }

    void onNewChunk() {
        shrinkAttempts = 0;
    }

    private boolean hasShrunkCurrentChunk() {
        return shrinkAttempts > 0;
    }

    private long minChunkLength() {
        return timeAligner.alignToCeil(1L);
    }
}
