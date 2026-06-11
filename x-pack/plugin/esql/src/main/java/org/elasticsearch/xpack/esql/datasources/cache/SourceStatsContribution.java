/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.Map;

/**
 * Typed view of a single per-file stats contribution shipped back from a data node via
 * {@code DriverCompletionInfo.capturedSourceMetadata}.
 * <p>
 * Contributions arrive at the coordinator as untyped {@code Map<String, Object>} blobs — the flat
 * {@code _stats.*} wire vocabulary that crosses the transport as a generic map and that the optimizer
 * also reads. {@link #classify} is the boundary that turns one such blob into a typed value: the
 * statistics become a {@link SourceStatistics}, and the keying/coverage fields become typed scalars,
 * so the reconciler reasons over types instead of magic string keys. Each kind composes differently
 * and the reconciler routes them through an exhaustive {@code switch}, so a new kind is a compile
 * error until its merge semantics are written. This type lives only on the coordinator — there is no
 * transport-version or BWC impact.
 */
sealed interface SourceStatsContribution {

    /** A complete read of the whole file; its row count already covers every row, so duplicates are deduplicated rather than summed. */
    record WholeFile(SourceStatistics stats, long mtimeMillis, String configFingerprint) implements SourceStatsContribution {}

    /**
     * One range of a parallel-parsed file (a streaming chunk, a record-aligned macro-split segment, a
     * block split). {@code start}/{@code end} are the half-open byte range it covered, in the path's
     * read coordinate system; {@code last} marks the contribution that observed end-of-input.
     * <p>
     * {@code stripeSize} > 0 marks the fragment as canonical-stripe addressed (see
     * {@link ExternalStats#STRIPE_SIZE_KEY}): the producing segmentator cut chunks at stripe
     * boundaries, so this fragment nests within stripe {@code floor(start / stripeSize)} and
     * {@code stripeHead} marks the fragment that starts exactly at a stripe cut (or offset 0). The
     * reconciler folds fragments per stripe — identical ranges from sibling scans (FORK branches,
     * retries) dedup by identity, fragments of one stripe tile its span — and commits complete
     * stripes idempotently into the schema cache. Fragments without stripe addressing
     * ({@code stripeSize <= 0}: older nodes, non-striped read paths such as record-aligned
     * macro-splits and seekable parallel segments) are not cacheable — a deterministic safe miss,
     * never a wrong answer.
     */
    record PartialChunk(
        SourceStatistics stats,
        long mtimeMillis,
        String configFingerprint,
        long start,
        long end,
        boolean last,
        long stripeSize,
        boolean stripeHead
    ) implements SourceStatsContribution {
        boolean hasCoverage() {
            return start >= 0 && end >= start;
        }

        boolean stripeAddressed() {
            return hasCoverage() && stripeSize > 0;
        }

        long stripeOrdinal() {
            return start / stripeSize;
        }
    }

    /** A chunk dropped rows mid-scan (SKIP_ROW); the file's whole contribution set must be discarded to avoid an under-count. */
    record Poison() implements SourceStatsContribution {}

    /**
     * Classifies a raw wire contribution by its marker keys. A poison marker is its own entry; a
     * partial-marked entry carries its coverage range; anything else is a whole-file read. The
     * statistics are parsed into a {@link SourceStatistics} via {@link SourceStatisticsSerializer},
     * which reads only the {@code _stats.row_count} / {@code _stats.columns.*} keys and so naturally
     * ignores the marker, mtime, fingerprint, and coverage keys carried alongside them.
     */
    static SourceStatsContribution classify(Map<String, Object> raw) {
        if (Boolean.TRUE.equals(raw.get(ExternalStats.CHUNK_HAD_ERRORS_KEY))) {
            return new Poison();
        }
        SourceStatistics stats = SourceStatisticsSerializer.extractStatistics(raw).orElse(null);
        long mtime = raw.get(ExternalStats.MTIME_MILLIS_KEY) instanceof Number n ? n.longValue() : -1L;
        String fingerprint = raw.get(ExternalStats.CONFIG_FINGERPRINT_KEY) instanceof String s ? s : null;
        if (raw.containsKey(ExternalStats.PARTIAL_CHUNK_KEY) == false) {
            return new WholeFile(stats, mtime, fingerprint);
        }
        long start = raw.get(ExternalStats.COVERAGE_START_KEY) instanceof Number n ? n.longValue() : -1L;
        long end = raw.get(ExternalStats.COVERAGE_END_KEY) instanceof Number n ? n.longValue() : -1L;
        boolean last = Boolean.TRUE.equals(raw.get(ExternalStats.COVERAGE_IS_LAST_KEY));
        long stripeSize = raw.get(ExternalStats.STRIPE_SIZE_KEY) instanceof Number n ? n.longValue() : -1L;
        boolean stripeHead = Boolean.TRUE.equals(raw.get(ExternalStats.STRIPE_HEAD_KEY));
        return new PartialChunk(stats, mtime, fingerprint, start, end, last, stripeSize, stripeHead);
    }
}
