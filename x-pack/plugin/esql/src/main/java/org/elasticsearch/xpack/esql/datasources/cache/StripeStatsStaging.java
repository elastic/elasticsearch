/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
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
 * Chunks are cut on buffer fill at a record boundary, never on the stripe grid, so several chunks each observe a
 * piece of one stripe and each piece carries that stripe's whole column summary. A chunk now hands its piece
 * here, where it joins the piece it continues, and only a stripe whose grid cell is fully covered — or whatever a
 * finished read leaves behind — reaches the capture sink.
 * <p>
 * The join is by byte adjacency, not by arrival: a piece that touches no neighbour yet is held as it is and
 * joined when the piece between them arrives, so chunks parsed out of order fold to the same records as chunks
 * parsed in order. Two pieces join only when the reconciler's own interval cover would chain them — same stripe
 * of the same grid, same file version and read, one ending on the byte the other starts on — and the fold is
 * {@link SplitStats#fold}, the law the reconciler applies to a covering chain. A read that leaves a gap in a
 * stripe stages its pieces separately and the coordinator covers them exactly as before: nothing here claims
 * coverage of bytes no chunk read.
 * <p>
 * One instance serves one read of one storage object and is shared by every chunk's parser thread. The staged
 * state is small (one record per stripe in flight) and the critical sections run twice per stripe a closing chunk
 * overlapped — a chunk hands over one piece per ordinal in its byte range — so a single monitor is cheaper than
 * per-stripe locks and makes {@link #flush} trivially consistent.
 */
public final class StripeStatsStaging implements StripeStatsSink {

    /** A stripe of one file; pieces of different stripes never join. */
    private record StripeKey(String sourceLocation, long ordinal) {}

    /**
     * One contiguous byte range within a single stripe that this read has covered, and the statistics over the
     * records in it. {@code atStripeStart} is the left-most piece's anchor and {@code atStripeEnd}/{@code eof}
     * the right-most's, because a join keeps the geometry of the piece that owns each edge — the same rule the
     * reconciler's covering chain follows.
     */
    private static final class StripeSpan {
        private final StripeStatsSink.ReadIdentity identity;
        private final List<Attribute> schema;
        private long start;
        private long end;
        private boolean atStripeStart;
        private boolean atStripeEnd;
        private boolean eof;
        private SplitStats stats;

        private StripeSpan(
            StripeStatsSink.ReadIdentity identity,
            List<Attribute> schema,
            long start,
            long end,
            boolean atStripeStart,
            boolean atStripeEnd,
            boolean eof,
            SplitStats stats
        ) {
            this.identity = identity;
            this.schema = schema;
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

        /**
         * Whether {@code other} continues this span: same read, byte-adjacent, and described by the same schema.
         * <p>
         * The schema is part of the test because {@link #absorb} folds both pieces' column sets and keeps only one
         * piece's schema, while {@link StagedStripeRecord} counts its entries from the statistics and writes them
         * from the schema. Joining two pieces whose schemas name different columns would leave a record reporting
         * a size larger than the entries it yields, and the map's length prefix is written from that size -- so the
         * reader would take the following bytes as further entries. Pieces that disagree stage separately, exactly
         * as pieces of different reads already do, and the coordinator covers them as it covers any other pair.
         */
        private boolean joins(StripeSpan other) {
            return identity.equals(other.identity) && Objects.equals(schema, other.schema) && (end == other.start || other.end == start);
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

    /**
     * The one schema list the records of this read share. A reader may hand a chunk an EQUAL BUT DISTINCT list:
     * {@code NdJsonFormatReader} merges the bound read schema with its resolved-schema projection once per chunk,
     * and {@link Attribute#withNullability} clones an attribute whose nullability differs from the projection's,
     * so each chunk can carry its own copy of every column. A
     * staged record keeps the list it was staged with until the query ends, so without this the read retains one
     * list per completing chunk rather than one per read -- measured at 9,640,554 live attributes against 94,096
     * staged records on a 105-column NDJSON read, where the control arm held about twenty thousand because it
     * flattened each piece immediately and let the list die with its iterator.
     */
    private List<Attribute> sharedSchema;

    public StripeStatsStaging(ConcurrentMap<String, List<Map<String, Object>>> sink) {
        this.sink = Objects.requireNonNull(sink, "a staging area needs a sink to stage into");
    }

    /**
     * Takes one chunk's piece of stripe {@code ordinal} covering {@code [start, end)}, joins it to the pieces it
     * continues, and stages the stripe as one record once its grid cell is fully covered.
     *
     * @param statistics the piece's own statistics, already filtered to {@code schema} by
     *                   {@link TextFormatStats}, so a piece retains only what a published record carries
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
        SourceStatistics statistics,
        List<Attribute> schema
    ) {
        SplitStats compact = SplitStats.fromStatistics(statistics);
        // A piece always carries a row count: TextFormatStats.build sets one for every stripe the emit loop
        // walks, including the empty ones. Assert rather than absorb it, and safe-miss in production.
        assert compact != null : "a stripe piece reached staging without a row count";
        if (compact == null) {
            return;
        }
        StripeSpan span = new StripeSpan(identity, shared(schema), start, end, atStripeStart, atStripeEnd, eof, compact);
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
     * How many stripes this area is still holding pieces of — the live state a long read carries. A stripe whose
     * cell is covered is staged and its key REMOVED, rather than left behind as an empty list, so this tracks the
     * stripes in flight and not the stripes the read has touched. On a clean whole-file read that difference is
     * the whole of it: every stripe completes, so this ends at zero rather than at one entry per stripe in the
     * file.
     */
    synchronized int openStripes() {
        return open.size();
    }

    /**
     * Stages every stripe this read left partly covered. A read that stopped short — a gap between chunks, a
     * limit, a cancellation — stages its pieces as they are, because only the coordinator's interval cover can
     * tell whether another read's pieces complete the stripe; withholding them would throw away coverage this
     * read genuinely has. Idempotent, so a close path may call it more than once.
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

    /**
     * Appends one stripe-addressed record for {@code span} to the capture sink, in the compact form the span was
     * folded in. A staged record builds its flat {@code _stats.*} map on demand and keeps the compact form, so
     * the pile a query leaves in the sink is proportional to the statistics rather than to the string-keyed
     * entries they serialize as — see {@link StagedStripeRecord}.
     */
    private void stage(String sourceLocation, long ordinal, StripeSpan span) {
        ExternalStatsCapture.recordTo(
            sink,
            sourceLocation,
            new StagedStripeRecord(
                span.identity,
                ordinal,
                span.start,
                span.end,
                span.atStripeStart,
                span.atStripeEnd,
                span.eof,
                span.stats,
                span.schema
            )
        );
    }

    /**
     * The shared list equal to {@code schema}, or {@code schema} itself when this read has not seen an equal one.
     * <p>
     * {@code SchemaInterner} already interns attribute lists by equality and charges the breaker for what it
     * keeps, and this deliberately does not reuse it: that interner lives for a resolve and is reached only from
     * {@code ExternalSourceResolver} and {@code SchemaReconciliation}, while these lists are produced per parse
     * chunk on a reader thread and must die with the read. One slot per staging area is what bounds this one -
     * it holds a single list, not a map, so it cannot outgrow the read that owns it.
     * <p>
     * Synchronized because the only caller builds its span OUTSIDE the critical section below, on whichever
     * parser thread closed the chunk. Equality is what makes the substitution invisible: a clone keeps the
     * attribute's {@code id()}, so the per-chunk lists of one read compare equal element-wise, and only names are
     * ever read back out of the list -- {@code TextFormatStats.buildColumns} uses the schema solely as
     * {@code cached.get(a.name())}. Where the lists are NOT equal nothing is shared and nothing changes.
     */
    private synchronized List<Attribute> shared(List<Attribute> schema) {
        if (schema == null) {
            return null;
        }
        if (sharedSchema == null) {
            sharedSchema = schema;
            return schema;
        }
        return sharedSchema.equals(schema) ? sharedSchema : schema;
    }

}
