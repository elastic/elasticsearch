/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ml.datafeed.SearchInterval;
import org.elasticsearch.xpack.ml.datafeed.extractor.esql.EsqlDataExtractor;

import java.util.Optional;

/** ES|QL chunk shrink, regrow, and incomplete-interval state; touched only from the consuming thread. */
final class ChunkedDataExtractorEsqlSizing {

    /**
     * Burst headroom when sizing an initial ES|QL chunk: target {@link #DEFAULT_ESQL_CHUNK_DOCS} rows per chunk, which is
     * {@link EsqlDataExtractor#INJECTED_ROW_LIMIT} divided by this factor.
     */
    static final long CHUNK_HEADROOM_FACTOR = 5L;

    /**
     * Target rows per initial ES|QL chunk when chunk span is auto. ES|QL has no scroll; oversized chunks truncate at
     * {@link EsqlDataExtractor#INJECTED_ROW_LIMIT}.
     */
    static final long DEFAULT_ESQL_CHUNK_DOCS = EsqlDataExtractor.INJECTED_ROW_LIMIT / CHUNK_HEADROOM_FACTOR;

    /**
     * Maximum ES|QL shrink retries per chunk after a result hits {@link EsqlDataExtractor#INJECTED_ROW_LIMIT}; the interval shrinks by
     * at least {@link #CHUNK_HEADROOM_FACTOR} except where the one-grouping-interval floor applies.
     */
    private static final int MAX_ESQL_SHRINK_ATTEMPTS = 5;

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
        return injectsEsqlRowLimit && rowCount >= EsqlDataExtractor.INJECTED_ROW_LIMIT;
    }

    boolean canShrink(long currentStart, long currentEnd) {
        long intervalLength = currentEnd - currentStart;
        return shrinkAttempts < MAX_ESQL_SHRINK_ATTEMPTS && intervalLength > minChunkLength();
    }

    long shrunkEnd(long currentStart, long currentEnd, long rowCount) {
        assert rowCount >= EsqlDataExtractor.INJECTED_ROW_LIMIT;
        assert shrinkAttempts < MAX_ESQL_SHRINK_ATTEMPTS;
        long intervalLength = currentEnd - currentStart;
        // Quotient and remainder keep the scaled length from overflowing on large intervals.
        long targetLength = Math.max(
            1L,
            intervalLength / rowCount * DEFAULT_ESQL_CHUNK_DOCS + intervalLength % rowCount * DEFAULT_ESQL_CHUNK_DOCS / rowCount
        );
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
        if (rowCount < 0 || rowCount >= DEFAULT_ESQL_CHUNK_DOCS || initialChunkSpan <= 0) {
            return chunkSpan;
        }
        return timeAligner.alignToCeil(Math.min(initialChunkSpan, chunkSpan * CHUNK_HEADROOM_FACTOR));
    }

    void recordIncomplete(SearchInterval interval) {
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

    void onShrunk() {
        shrinkAttempts++;
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
