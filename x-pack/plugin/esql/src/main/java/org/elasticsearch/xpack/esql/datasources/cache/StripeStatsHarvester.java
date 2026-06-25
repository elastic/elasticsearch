/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.TreeMap;

/**
 * Shared canonical-stripe statistics harvester for the row-format readers (CSV / TSV / NDJSON). Both
 * readers attribute each record to its canonical stripe by the record's OWN file-global start offset
 * ({@code ordinal = floor(recordStartOffset / B)}) and feed this harvester; at close the harvester emits
 * one stripe-addressed contribution per stripe the chunk's byte range overlaps, using the
 * <em>byte-range cover</em> model.
 * <p>
 * The byte-range cover model is the one proven correct at multi-stripe / multi-chunk scale: the emit loop
 * walks every stripe ordinal the chunk's half-open byte range {@code [splitStartByte, chunkAbsEnd)}
 * overlaps — including stripes with no records (a stripe whose first record lands in the next chunk; this
 * chunk still owns that stripe's left edge and must anchor it). Each fragment's byte sub-range is the
 * chunk range clamped to the stripe's grid cell ({@code [max(splitStartByte, k*B), min(chunkAbsEnd,
 * (k+1)*B))}), so sibling chunks' fragments for a split stripe tile contiguously and the coordinator's
 * per-stripe interval-cover fold reaches whole-file completeness. Cover anchors are pure byte-range-overlap
 * predicates:
 * <ul>
 *   <li>{@code atStart} — this chunk covers the stripe's left grid line ({@code splitStartByte <= k*B}).</li>
 *   <li>{@code atEnd} — this chunk covers the right grid line ({@code chunkAbsEnd >= (k+1)*B}) or this is the
 *   file-final chunk.</li>
 *   <li>{@code eof} — file-final chunk and this is its last stripe.</li>
 * </ul>
 * Record attribution is by each record's own start, so per-stripe row counts are scan-invariant; misaligned
 * sibling tilings collapse to one answer.
 * <p>
 * The harvester owns the per-stripe {@link StripeAccum} (rows + projected {@code cols} + all-schema
 * {@code allCols}) and the emit loop. The readers own the per-format extraction of typed values into those
 * accumulators (CSV from the output page / raw record, NDJSON from the output page / widened decoded page),
 * because the value-extraction differs by format; everything downstream of attribution is shared here.
 * <p>
 * Not thread-safe: each instance is owned by exactly one batch iterator over that iterator's lifetime.
 */
public final class StripeStatsHarvester {

    /**
     * One stripe's running stats: row count + per-column min/max/null. Byte ranges and cover anchors are
     * derived from the chunk's byte geometry at emit, not accumulated here.
     */
    public static final class StripeAccum {
        /** Projected-column stats (PROJECTED/ALL), fed from the output page's blocks. */
        public ColumnStatsAccumulator cols;
        /**
         * ALL-scope full-file-schema stats, fed from the raw parsed record / widened decoded page (every file
         * column, including the unprojected ones the output page never carries). Merged with {@link #cols} at
         * emission so ALL's committed column set is a strict superset of PROJECTED's.
         */
        public ColumnStatsAccumulator allCols;
        public long rows;
    }

    private final long stripeSize;
    private final boolean fileFinal;
    /** Per-stripe accumulators keyed by ordinal; the emit loop reads them by byte-range geometry. */
    private final TreeMap<Long, StripeAccum> stripeAccums = new TreeMap<>();

    public StripeStatsHarvester(long stripeSize, boolean fileFinal) {
        this.stripeSize = stripeSize;
        this.fileFinal = fileFinal;
    }

    /** Stripe ordinal a record starting at {@code recordStartOffset} is attributed to. */
    public long ordinalOf(long recordStartOffset) {
        return Math.floorDiv(recordStartOffset, stripeSize);
    }

    /** Whether any stripe has been touched (drives the close-time emit-vs-skip decision). */
    public boolean isEmpty() {
        return stripeAccums.isEmpty();
    }

    /** Returns the accumulator for {@code ordinal}, creating it (zero rows, no columns) on first touch. */
    public StripeAccum getOrCreate(long ordinal) {
        return stripeAccums.computeIfAbsent(ordinal, k -> new StripeAccum());
    }

    /**
     * Folds the projected accumulator and the ALL-scope full-schema accumulator into one committed
     * column-stats map. The full-schema map (when present) is a superset of the projected one and the two
     * agree on shared columns, so the projected map is overlaid first and the full-schema map fills in (and
     * re-affirms) the rest. Either may be {@code null}: COUNT commits neither, PROJECTED only the projected
     * one, ALL both.
     */
    public static Map<String, ExternalStats.ColumnStats> mergeColumnStats(ColumnStatsAccumulator projected, ColumnStatsAccumulator all) {
        Map<String, ExternalStats.ColumnStats> projectedSnapshot = projected == null ? Map.of() : projected.snapshot();
        Map<String, ExternalStats.ColumnStats> allSnapshot = all == null ? Map.of() : all.snapshot();
        if (allSnapshot.isEmpty()) {
            return projectedSnapshot;
        }
        if (projectedSnapshot.isEmpty()) {
            return allSnapshot;
        }
        Map<String, ExternalStats.ColumnStats> merged = new LinkedHashMap<>(projectedSnapshot);
        merged.putAll(allSnapshot);
        return merged;
    }

    /**
     * Emits one stripe-addressed fragment for every stripe the chunk's byte range
     * {@code [splitStartByte, splitStartByte + chunkBytes)} overlaps. A {@code chunkBytes <= 0} (unknown or
     * empty byte range) is a safe miss — nothing is emitted, so a warm aggregate re-scans rather than serving
     * a wrong answer. Each emitted contribution carries the well-known {@code _stats.*} addressing keys the
     * coordinator's interval-cover fold consumes.
     *
     * @param sourceLocation    file path key for the capture sink
     * @param splitStartByte    file-global byte offset of this chunk's first byte (decompressed-stream coordinate)
     * @param chunkBytes        bytes this chunk consumed (decompressed coordinate); {@code <= 0} ⇒ safe miss
     * @param pinnedMtimeMillis mtime pinned at iterator open
     * @param fingerprint       config fingerprint over the full file schema
     * @param schema            full file schema (drives the per-column serialization)
     */
    public void emit(
        String sourceLocation,
        long splitStartByte,
        long chunkBytes,
        long pinnedMtimeMillis,
        String fingerprint,
        List<Attribute> schema
    ) {
        if (chunkBytes <= 0) {
            return; // unknown / empty byte range — safe miss
        }
        long chunkAbsEnd = splitStartByte + chunkBytes;
        long firstOrdinal = Math.floorDiv(splitStartByte, stripeSize);
        long lastOrdinal = Math.floorDiv(chunkAbsEnd - 1, stripeSize);
        for (long ordinal = firstOrdinal; ordinal <= lastOrdinal; ordinal++) {
            long gridStart = ordinal * stripeSize;
            long gridEnd = gridStart + stripeSize;
            long start = Math.max(splitStartByte, gridStart);
            long end = Math.min(chunkAbsEnd, gridEnd);
            boolean atStart = splitStartByte <= gridStart;
            boolean atEnd = chunkAbsEnd >= gridEnd || fileFinal;
            boolean eof = fileFinal && ordinal == lastOrdinal;
            StripeAccum acc = stripeAccums.get(ordinal);
            long rows = acc == null ? 0L : acc.rows;
            // PROJECTED/COUNT commit acc.cols (projected columns / none). ALL additionally commits acc.allCols
            // (every file column), so the committed set is a strict superset of PROJECTED's.
            Map<String, ExternalStats.ColumnStats> cols = mergeColumnStats(acc == null ? null : acc.cols, acc == null ? null : acc.allCols);
            ExternalStats.Stats statsRecord = new ExternalStats.Stats(rows, OptionalLong.empty(), cols);
            SourceStatistics sourceStats = TextFormatStats.build(Optional.of(statsRecord), OptionalLong.empty(), schema);
            Map<String, Object> base = new HashMap<>();
            base.put(ExternalStats.MTIME_MILLIS_KEY, pinnedMtimeMillis);
            base.put(ExternalStats.CONFIG_FINGERPRINT_KEY, fingerprint);
            base.put(ExternalStats.PARTIAL_CHUNK_KEY, Boolean.TRUE);
            base.put(ExternalStats.STRIPE_SIZE_KEY, stripeSize);
            base.put(ExternalStats.STRIPE_ORDINAL_KEY, ordinal);
            base.put(ExternalStats.COVERAGE_START_KEY, start);
            base.put(ExternalStats.COVERAGE_END_KEY, end);
            base.put(ExternalStats.STRIPE_AT_START_KEY, atStart);
            base.put(ExternalStats.STRIPE_AT_END_KEY, atEnd);
            base.put(ExternalStats.COVERAGE_IS_LAST_KEY, eof);
            Map<String, Object> flat = SourceStatisticsSerializer.embedStatistics(base, sourceStats);
            ExternalStatsCapture.record(sourceLocation, flat);
        }
    }
}
