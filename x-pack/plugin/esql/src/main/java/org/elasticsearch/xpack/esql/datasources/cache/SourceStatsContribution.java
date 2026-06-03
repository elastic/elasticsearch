/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Typed view of a single per-file stats contribution shipped back from a data node via
 * {@code DriverCompletionInfo.capturedSourceMetadata}.
 * <p>
 * Contributions travel over the wire as untyped {@code Map<String, Object>} blobs tagged with marker
 * keys ({@link ExternalStats#PARTIAL_CHUNK_KEY}, {@link ExternalStats#FINALIZE_CHUNKS_KEY},
 * {@link ExternalStats#CHUNK_HAD_ERRORS_KEY}). Each kind composes differently — partial chunks
 * are summed, whole-file reads are deduplicated, poison discards the file, finalize gates the merge.
 * Three separate correctness bugs in the reconciler came from the merge logic forgetting to
 * special-case one marker and letting that kind fall through to the summable {@link PartialChunk}
 * default (a SKIP_ROW poison summed, a different-{@code WITH}-options read cross-contaminated, two
 * whole-file reads doubled COUNT(*)).
 * <p>
 * Classifying each blob into this sealed type at the wire boundary, then merging through an
 * exhaustive {@code switch}, makes "did we remember to handle this kind?" a compile error: adding a
 * permitted subtype breaks every switch over {@code SourceStatsContribution} until its merge
 * semantics are written. The wire format stays the untyped map — this type lives only on the
 * coordinator, so there is no transport-version or BWC impact.
 */
sealed interface SourceStatsContribution {

    /** A complete read of the whole file; its row count already covers every row, so duplicates are deduplicated rather than summed. */
    record WholeFile(Map<String, Object> stats) implements SourceStatsContribution {}

    /** One record-aligned slice of a parallel-parsed file; summed with its siblings once a {@link Finalize} proves the set is complete. */
    record PartialChunk(Map<String, Object> stats) implements SourceStatsContribution {}

    /** A chunk dropped rows mid-scan (SKIP_ROW); the file's whole contribution set must be discarded to avoid an under-count. */
    record Poison() implements SourceStatsContribution {}

    /** Completion signal that every chunk of a parallel-parsed file finished cleanly; gates the partial merge and carries no stats. */
    record Finalize() implements SourceStatsContribution {}

    /**
     * Classifies a raw wire contribution by its marker keys. Precedence matches the publish sites:
     * a poison or finalize marker is its own stats-less entry; a partial-marked entry carries the
     * chunk's stats; anything else carrying a row count is a whole-file read. Marker keys are
     * stripped from the stats-bearing kinds so the well-known {@code _stats.*} keys merge cleanly.
     */
    static SourceStatsContribution classify(Map<String, Object> raw) {
        if (Boolean.TRUE.equals(raw.get(ExternalStats.CHUNK_HAD_ERRORS_KEY))) {
            return new Poison();
        }
        if (Boolean.TRUE.equals(raw.get(ExternalStats.FINALIZE_CHUNKS_KEY))) {
            // Defends the "Finalize carries no stats" contract: today's publishers never attach
            // a row count to the finalize marker, but nothing on the wire enforces it. If a future
            // publisher accidentally does, the silent fall-through used to mis-classify it as a
            // WholeFile/PartialChunk and either double-count or short-circuit the partial sum.
            assert raw.containsKey(SourceStatisticsSerializer.STATS_ROW_COUNT) == false : "Finalize marker must not carry stats: " + raw;
            return new Finalize();
        }
        Map<String, Object> stripped = new HashMap<>(raw);
        boolean isPartial = stripped.remove(ExternalStats.PARTIAL_CHUNK_KEY) != null;
        stripped.remove(ExternalStats.FINALIZE_CHUNKS_KEY);
        stripped.remove(ExternalStats.CHUNK_HAD_ERRORS_KEY);
        return isPartial ? new PartialChunk(stripped) : new WholeFile(stripped);
    }
}
