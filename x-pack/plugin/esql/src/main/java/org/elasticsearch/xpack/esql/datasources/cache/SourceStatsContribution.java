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
 * statistics become a {@link SourceStatistics} and the keying/addressing fields become typed scalars,
 * so the reconciler reasons over types instead of magic string keys. Each kind composes differently
 * and the reconciler routes them through an exhaustive {@code switch}, so a new kind is a compile
 * error until its merge semantics are written. This type lives only on the coordinator — there is no
 * transport-version or BWC impact.
 */
sealed interface SourceStatsContribution {

    /** A complete read of the whole file; its row count already covers every row, so duplicates are deduplicated rather than summed. */
    record WholeFile(SourceStatistics stats, long mtimeMillis, String configFingerprint) implements SourceStatsContribution {}

    /**
     * The records of one canonical stripe that a single chunk observed — the unit of the orthogonal
     * stripe model. Stripes are a pure addressing grid over file content ({@link
     * ExternalStats#STRIPE_SIZE_KEY} bytes each): the producing reader attributes each record to the
     * stripe its start offset falls in ({@code ordinal = floor(recordStartOffset / stripeSize)}),
     * independently of how the read was chunked, split, or distributed. A chunk that spans several
     * stripes emits one fragment per stripe it touched; a chunk boundary that lands mid-stripe splits
     * that stripe across two adjacent chunks' fragments.
     * <p>
     * {@code start}/{@code end} are the half-open byte sub-range of stripe {@code ordinal} this fragment
     * covered — the stripe's grid cell clamped to the chunk's byte range (in the file's read coordinate
     * system). A chunk boundary landing mid-stripe makes these endpoints chunk/grid byte positions, NOT
     * record boundaries. Scan-invariance does not come from the endpoints: it comes from ATTRIBUTION — each
     * record is counted into {@code floor(recordStart / B)} by its OWN start offset, so it lands in the same
     * stripe regardless of chunking, and sibling fragments tile the same grid cell. That is what makes the
     * reconciler's per-stripe interval-cover dedup exact: scan A covering a stripe in one fragment and scan B
     * splitting it at a different chunk boundary fold to the same stripe stats. {@code atStripeStart} marks the fragment whose
     * chunk covers the stripe's left grid line ({@code splitStartByte <= k*B}) — a byte-cover predicate, NOT
     * "holds the stripe's first record"; a stripe whose first record lands in the next chunk is still anchored
     * by the chunk owning its left grid line. {@code atStripeEnd} marks the fragment whose chunk covers the
     * right grid line ({@code chunkAbsEnd >= (k+1)*B}) or is file-final. {@code eof} marks the fragment that
     * observed end-of-input — the file's last stripe. Fragments without stripe addressing ({@code stripeSize <= 0}: older nodes,
     * readers not yet emitting stripes) are not cacheable — a deterministic safe miss, never wrong.
     */
    record StripeFragment(
        SourceStatistics stats,
        long mtimeMillis,
        String configFingerprint,
        long stripeSize,
        long ordinal,
        long start,
        long end,
        boolean atStripeStart,
        boolean atStripeEnd,
        boolean eof
    ) implements SourceStatsContribution {
        boolean stripeAddressed() {
            return stripeSize > 0 && ordinal >= 0 && start >= 0 && end >= start;
        }
    }

    /** A chunk dropped rows mid-scan (SKIP_ROW); the file's whole contribution set must be discarded to avoid an under-count. */
    record Poison() implements SourceStatsContribution {}

    /**
     * Classifies a raw wire contribution by its marker keys. A poison marker is its own entry; a
     * chunk marker ({@link ExternalStats#PARTIAL_CHUNK_KEY}) makes it a stripe fragment — carrying its
     * ordinal + record-canonical sub-range + tiling anchors when the producing reader is
     * stripe-addressed, or no stripe fields (and therefore {@link StripeFragment#stripeAddressed()}
     * false → a deterministic safe miss) when it is an older node or a reader not yet emitting stripes;
     * anything else is a whole-file read. A chunk fragment must NEVER fall through to {@link WholeFile}
     * — that would treat a partial cover as authoritative and over-count. The statistics are parsed
     * into a {@link SourceStatistics} via {@link SourceStatisticsSerializer}, which reads only the
     * {@code _stats.row_count} / {@code _stats.columns.*} keys and so naturally ignores the marker,
     * mtime, fingerprint, and stripe-addressing keys carried alongside them.
     */
    static SourceStatsContribution classify(Map<String, Object> raw) {
        if (Boolean.TRUE.equals(raw.get(ExternalStats.CHUNK_HAD_ERRORS_KEY))) {
            return new Poison();
        }
        SourceStatistics stats = SourceStatisticsSerializer.extractStatistics(raw).orElse(null);
        long mtime = raw.get(ExternalStats.MTIME_MILLIS_KEY) instanceof Number n ? n.longValue() : -1L;
        String fingerprint = raw.get(ExternalStats.CONFIG_FINGERPRINT_KEY) instanceof String s ? s : null;
        if (Boolean.TRUE.equals(raw.get(ExternalStats.PARTIAL_CHUNK_KEY)) == false) {
            return new WholeFile(stats, mtime, fingerprint);
        }
        long stripeSize = raw.get(ExternalStats.STRIPE_SIZE_KEY) instanceof Number n ? n.longValue() : -1L;
        long ordinal = raw.get(ExternalStats.STRIPE_ORDINAL_KEY) instanceof Number n ? n.longValue() : -1L;
        long start = raw.get(ExternalStats.COVERAGE_START_KEY) instanceof Number n ? n.longValue() : -1L;
        long end = raw.get(ExternalStats.COVERAGE_END_KEY) instanceof Number n ? n.longValue() : -1L;
        boolean atStart = Boolean.TRUE.equals(raw.get(ExternalStats.STRIPE_AT_START_KEY));
        boolean atEnd = Boolean.TRUE.equals(raw.get(ExternalStats.STRIPE_AT_END_KEY));
        boolean eof = Boolean.TRUE.equals(raw.get(ExternalStats.COVERAGE_IS_LAST_KEY));
        return new StripeFragment(stats, mtime, fingerprint, stripeSize, ordinal, start, end, atStart, atEnd, eof);
    }
}
