/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import java.util.HashMap;
import java.util.Map;

/**
 * Typed view of a single per-file stats contribution shipped back from a data node via
 * {@code DriverCompletionInfo.capturedSourceMetadata}.
 * <p>
 * Contributions travel over the wire as untyped {@code Map<String, Object>} blobs tagged with marker
 * keys ({@link ExternalStats#PARTIAL_CHUNK_KEY}, {@link ExternalStats#CHUNK_HAD_ERRORS_KEY}) and, for
 * partial chunks, a coverage range ({@link ExternalStats#COVERAGE_START_KEY} etc.). Each kind composes
 * differently and the reconciler routes them through an exhaustive {@code switch}, so a new kind is a
 * compile error until its merge semantics are written — the safeguard that the silent-fall-through
 * double-count/under-count regressions taught us to keep. The wire format stays the untyped map; this
 * type lives only on the coordinator, so there is no transport-version or BWC impact.
 */
sealed interface SourceStatsContribution {

    /** A complete read of the whole file; its row count already covers every row, so duplicates are deduplicated rather than summed. */
    record WholeFile(Map<String, Object> stats) implements SourceStatsContribution {}

    /**
     * One range of a parallel-parsed file (a streaming chunk, a record-aligned macro-split segment, a
     * block split). {@code start}/{@code end} are the half-open byte range it covered, in the path's
     * read coordinate system; {@code last} marks the contribution that observed end-of-input. The
     * reconciler unions partials by {@code [start,end)} — disjoint ranges sum, an identical range
     * re-observed by another scan of the same file (a sibling FORK branch, a schema-probe pass, a
     * retry) is counted once — and confirms the union tiles {@code [0, end)} with a flagged tail
     * before caching. {@code start < 0} marks a partial that arrived without coverage (an older node);
     * it cannot be addressed, so it renders its file's cover incomplete and uncacheable.
     */
    record PartialChunk(Map<String, Object> stats, long start, long end, boolean last) implements SourceStatsContribution {
        boolean hasCoverage() {
            return start >= 0 && end >= start;
        }
    }

    /** A chunk dropped rows mid-scan (SKIP_ROW); the file's whole contribution set must be discarded to avoid an under-count. */
    record Poison() implements SourceStatsContribution {}

    /**
     * Classifies a raw wire contribution by its marker keys. A poison marker is its own stats-less
     * entry; a partial-marked entry carries the chunk's stats plus its coverage range; anything else
     * carrying a row count is a whole-file read. Marker and coverage keys are stripped from the
     * stats-bearing kinds so the well-known {@code _stats.*} keys merge cleanly.
     */
    static SourceStatsContribution classify(Map<String, Object> raw) {
        if (Boolean.TRUE.equals(raw.get(ExternalStats.CHUNK_HAD_ERRORS_KEY))) {
            return new Poison();
        }
        Map<String, Object> stripped = new HashMap<>(raw);
        boolean isPartial = stripped.remove(ExternalStats.PARTIAL_CHUNK_KEY) != null;
        stripped.remove(ExternalStats.CHUNK_HAD_ERRORS_KEY);
        Object start = stripped.remove(ExternalStats.COVERAGE_START_KEY);
        Object end = stripped.remove(ExternalStats.COVERAGE_END_KEY);
        boolean last = Boolean.TRUE.equals(stripped.remove(ExternalStats.COVERAGE_IS_LAST_KEY));
        if (isPartial == false) {
            return new WholeFile(stripped);
        }
        // A partial without coverage (older node) is flagged un-addressable with start = -1.
        long startOffset = start instanceof Number n ? n.longValue() : -1L;
        long endOffset = end instanceof Number n ? n.longValue() : -1L;
        return new PartialChunk(stripped, startOffset, endOffset, last);
    }
}
