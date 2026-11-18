/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.SplitStats;
import org.elasticsearch.xpack.esql.datasources.spi.SourceStatistics;
import org.elasticsearch.xpack.esql.datasources.spi.StripeStatsSink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

/**
 * Holds one stripe's statistics live while that stripe is being read, so a read stages one record per stripe
 * rather than one per parse chunk.
 * <p>
 * Chunks are cut on buffer fill at a record boundary, never on the stripe grid, so several chunks each observe
 * a piece of one stripe. A chunk hands its piece here, it joins the piece it continues, and only a stripe
 * whose grid cell is fully covered -- or whatever a finished read leaves behind -- reaches the capture sink.
 * <p>
 * The join is by byte adjacency rather than arrival, so chunks parsed out of order fold to the same records as
 * chunks parsed in order. Two pieces join only where the reconciler's own interval cover would chain them, and
 * the fold is {@link SplitStats#fold}, the law that cover applies. A read leaving a gap stages its pieces
 * separately: nothing here claims coverage of bytes no chunk read.
 * <p>
 * One instance serves one read of one storage object, shared by every chunk's parser thread. The staged state
 * is one record per stripe in flight, so a single monitor is cheaper than per-stripe locks and makes
 * {@link #flush} trivially consistent.
 */
public final class StripeStatsStaging implements StripeStatsSink {

    /** A stripe of one file; pieces of different stripes never join. */
    private record StripeKey(String sourceLocation, long ordinal) {}

    /**
     * One contiguous byte range within a single stripe, and the statistics over the records in it.
     * {@code atStripeStart} is the left-most piece's anchor and {@code atStripeEnd}/{@code eof} the
     * right-most's, because a join keeps the geometry of the piece owning each edge.
     */
    private static final class StripeSpan {
        private final StripeStatsSink.ReadIdentity identity;
        private long start;
        private long end;
        private boolean atStripeStart;
        private boolean atStripeEnd;
        private boolean eof;
        private SplitStats stats;

        private StripeSpan(
            StripeStatsSink.ReadIdentity identity,
            long start,
            long end,
            boolean atStripeStart,
            boolean atStripeEnd,
            boolean eof,
            SplitStats stats
        ) {
            this.identity = identity;
            this.start = start;
            this.end = end;
            this.atStripeStart = atStripeStart;
            this.atStripeEnd = atStripeEnd;
            this.eof = eof;
            this.stats = stats;
        }

        /** Whether this span covers its stripe's whole grid cell: both grid lines anchored, nothing missing between. */
        private boolean covers() {
            return atStripeStart && atStripeEnd;
        }

        /** Whether {@code other} continues this span: same read, byte-adjacent. Column sets are the fold's business. */
        private boolean joins(StripeSpan other) {
            return identity.equals(other.identity) && (end == other.start || other.end == start);
        }

        /** Absorbs the byte-adjacent {@code other}, keeping each edge's own anchors. */
        private void absorb(StripeSpan other) {
            StripeSpan left = end == other.start ? this : other;
            StripeSpan right = left == this ? other : this;
            SplitStats folded = SplitStats.fold(List.of(left.stats, right.stats), false);
            assert folded != null : "folding two present stripe pieces cannot produce nothing";
            this.start = left.start;
            this.end = right.end;
            this.atStripeStart = left.atStripeStart;
            this.atStripeEnd = right.atStripeEnd;
            this.eof = right.eof;
            this.stats = folded;
        }
    }

    private final ConcurrentMap<String, List<Map<String, Object>>> sink;
    private final Map<StripeKey, List<StripeSpan>> open = new HashMap<>();

    public StripeStatsStaging(ConcurrentMap<String, List<Map<String, Object>>> sink) {
        this.sink = Objects.requireNonNull(sink, "a staging area needs a sink to stage into");
    }

    /**
     * Takes one chunk's piece of stripe {@code ordinal} covering {@code [start, end)}, joins it to the pieces it
     * continues, and stages the stripe as one record once its grid cell is fully covered.
     *
     * @param statistics the piece's statistics, already filtered to the file schema by {@link TextFormatStats}.
     *                   No schema reaches here: the filter has done its work, and a record holding one would
     *                   count its entries from the statistics and write them from the schema
     */
    @Override
    public void contribute(
        String sourceLocation,
        StripeStatsSink.ReadIdentity identity,
        long ordinal,
        long start,
        long end,
        boolean atStripeStart,
        boolean atStripeEnd,
        boolean eof,
        SourceStatistics statistics
    ) {
        SplitStats compact = compact(statistics);
        // A piece always carries a row count: TextFormatStats.build sets one for every stripe the emit loop
        // walks, including the empty ones. Assert rather than absorb it, and safe-miss in production.
        assert compact != null : "a stripe piece reached staging without a row count";
        if (compact == null) {
            return;
        }
        StripeSpan span = new StripeSpan(identity, start, end, atStripeStart, atStripeEnd, eof, compact);
        StripeKey key = new StripeKey(sourceLocation, ordinal);
        synchronized (this) {
            List<StripeSpan> spans = open.computeIfAbsent(key, k -> new ArrayList<>(1));
            // Absorb neighbours until none is left: the arriving piece may be the one that finally bridges two
            // pieces already held, so one pass over the list is not enough.
            boolean absorbed = true;
            while (absorbed) {
                absorbed = false;
                for (int i = spans.size() - 1; i >= 0; i--) {
                    if (span.joins(spans.get(i))) {
                        span.absorb(spans.remove(i));
                        absorbed = true;
                        break;
                    }
                }
            }
            if (span.covers() == false) {
                spans.add(span);
                return;
            }
            if (spans.isEmpty()) {
                open.remove(key);
            }
        }
        stage(sourceLocation, ordinal, span);
    }

    /**
     * The compact form of one piece's statistics. Here rather than on {@link SplitStats}, which converts only
     * between its own wire form and the flat {@code _stats.*} map and does not otherwise know the SPI type.
     * Key-equivalent to the flat-map route, which
     * {@code StripeStatsStagingTests#testCompactingTypedStatisticsMatchesTheFlatMapRoute} holds; neither route
     * can carry a servability marker, so it is unavailable on both rather than lost on this one.
     */
    @Nullable
    static SplitStats compact(SourceStatistics statistics) {
        if (statistics == null || statistics.rowCount().isPresent() == false) {
            return null;
        }
        SplitStats.Builder builder = new SplitStats.Builder().rowCount(statistics.rowCount().getAsLong());
        statistics.sizeInBytes().ifPresent(builder::sizeInBytes);
        for (Map.Entry<String, SourceStatistics.ColumnStatistics> column : statistics.columnStatistics().orElse(Map.of()).entrySet()) {
            SourceStatistics.ColumnStatistics cs = column.getValue();
            int ordinal = builder.addColumn(column.getKey());
            cs.nullCount().ifPresent(nullCount -> builder.nullCount(ordinal, nullCount));
            cs.valueCount().ifPresent(valueCount -> builder.valueCount(ordinal, valueCount));
            cs.minValue().ifPresent(min -> builder.min(ordinal, min));
            cs.maxValue().ifPresent(max -> builder.max(ordinal, max));
            cs.sizeInBytes().ifPresent(sizeBytes -> builder.sizeBytes(ordinal, sizeBytes));
        }
        return builder.build();
    }

    /**
     * How many stripes this area still holds pieces of. A covered stripe is staged and its key removed, so on a
     * clean whole-file read this ends at zero rather than at one entry per stripe in the file.
     */
    synchronized int openStripes() {
        return open.size();
    }

    /**
     * Stages every stripe this read left partly covered. Only the coordinator's interval cover can tell whether
     * another read's pieces complete a stripe, so withholding them would throw away coverage this read has.
     * Idempotent, so a close path may call it more than once.
     */
    public void flush() {
        List<Map.Entry<StripeKey, List<StripeSpan>>> remaining;
        synchronized (this) {
            remaining = List.copyOf(open.entrySet());
            open.clear();
        }
        for (Map.Entry<StripeKey, List<StripeSpan>> entry : remaining) {
            for (StripeSpan span : entry.getValue()) {
                stage(entry.getKey().sourceLocation(), entry.getKey().ordinal(), span);
            }
        }
    }

    /** Appends one stripe-addressed record for {@code span}, in the compact form it was folded in. */
    private void stage(String sourceLocation, long ordinal, StripeSpan span) {
        ExternalStatsCapture.recordTo(
            sink,
            sourceLocation,
            new StagedStripeRecord(span.identity, ordinal, span.start, span.end, span.atStripeStart, span.atStripeEnd, span.eof, span.stats)
        );
    }
}
