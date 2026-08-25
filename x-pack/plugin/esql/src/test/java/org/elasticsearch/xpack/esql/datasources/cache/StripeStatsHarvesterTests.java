/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.SourceStatisticsSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

/**
 * Byte-range-cover GEOMETRY tests for the shared {@link StripeStatsHarvester}, driven DIRECTLY at the
 * harvester's own layer — no CSV / NDJSON reader, no parsing. The harvester is the format-agnostic component
 * both row-format readers feed: each reader attributes a record to {@code floor(recordStart / B)} and bumps
 * that stripe's row count; at close the harvester emits one stripe-addressed fragment per stripe the chunk's
 * byte range overlaps. These cases pin that emit behavior — dense ordinals, contiguous tiling, empty stripes
 * for oversized records, the partial trailing stripe of a non-final chunk, a record starting on a stripe
 * boundary — so the cover is exercised at the closest layer rather than only through a reader's decode path.
 * <p>
 * The reader-level files ({@code CsvStripeStatsCaptureTests} / {@code NdJsonStripeStatsCaptureTests}) keep the
 * end-to-end exact-count tests through the production reconciler; the format-agnostic geometry lives here.
 */
public class StripeStatsHarvesterTests extends ESTestCase {

    // A fixed record width so byte offsets are predictable; mirrors the NDJSON fixture's 8-byte record.
    private static final int RECORD_BYTES = 8;

    private static final List<Attribute> SCHEMA = List.of(
        new ReferenceAttribute(Source.EMPTY, null, "a", DataType.LONG, Nullability.TRUE, null, false)
    );

    /** A parsed view of one emitted stripe fragment. */
    private record Frag(long ordinal, long rows, long start, long end, boolean atStart, boolean atEnd, boolean eof) {}

    /**
     * Drives the harvester directly: feeds {@code recordCount} fixed-width records starting at file byte
     * {@code firstRecordStart} (each {@link #RECORD_BYTES} apart), attributing each to its canonical stripe by
     * its own start offset, then emits the cover for the chunk byte range {@code [splitStartByte,
     * splitStartByte + chunkBytes)} at grid {@code stripeSize}. Returns every emitted fragment, ordinal-sorted.
     */
    private List<Frag> emitCover(
        long stripeSize,
        boolean fileFinal,
        long splitStartByte,
        long chunkBytes,
        long firstRecordStart,
        int recordCount
    ) {
        StripeStatsHarvester harvester = new StripeStatsHarvester(stripeSize, fileFinal);
        for (int i = 0; i < recordCount; i++) {
            long recordStart = firstRecordStart + (long) i * RECORD_BYTES;
            harvester.getOrCreate(harvester.ordinalOf(recordStart)).rows++;
        }
        String path = "memory://harvester-" + UUID.randomUUID();
        ConcurrentMap<String, List<Map<String, Object>>> sink = ExternalStatsCapture.newSink();
        try (var handle = ExternalStatsCapture.bind(sink)) {
            harvester.emit(path, splitStartByte, chunkBytes, 1000L, "fp", SCHEMA);
        }
        List<Map<String, Object>> raw = sink.get(path);
        List<Frag> frags = new ArrayList<>();
        if (raw != null) {
            for (Map<String, Object> m : raw) {
                assertTrue("a stripe fragment must carry the partial-chunk marker", m.containsKey(ExternalStats.PARTIAL_CHUNK_KEY));
                assertTrue("a stripe fragment must carry a stripe ordinal", m.containsKey(ExternalStats.STRIPE_ORDINAL_KEY));
                frags.add(
                    new Frag(
                        ((Number) m.get(ExternalStats.STRIPE_ORDINAL_KEY)).longValue(),
                        ((Number) m.get(SourceStatisticsSerializer.STATS_ROW_COUNT)).longValue(),
                        ((Number) m.get(ExternalStats.COVERAGE_START_KEY)).longValue(),
                        ((Number) m.get(ExternalStats.COVERAGE_END_KEY)).longValue(),
                        (Boolean) m.get(ExternalStats.STRIPE_AT_START_KEY),
                        (Boolean) m.get(ExternalStats.STRIPE_AT_END_KEY),
                        (Boolean) m.get(ExternalStats.COVERAGE_IS_LAST_KEY)
                    )
                );
            }
        }
        frags.sort((x, y) -> Long.compare(x.ordinal, y.ordinal));
        return frags;
    }

    /**
     * Asserts the fragments form a complete, non-double-counting cover of a dense file of {@code totalRows}
     * records over {@code totalBytes} bytes emitted by one file-final scan under the byte-range cover model:
     * ordinals dense from 0; every stripe complete on both sides (atStart and atEnd); only the terminal stripe
     * eof; the head covers byte 0; the byte sub-ranges tile contiguously across stripe boundaries and the last
     * stripe closes to the file's byte length; and the rows sum exactly. Empty (zero-row) stripes carry their
     * grid-width byte sub-range (start &lt; end), not a zero-length range.
     */
    private void assertDenseFileFinalCover(List<Frag> frags, long totalRows, long totalBytes) {
        assertFalse("a non-empty file must emit at least one stripe fragment", frags.isEmpty());
        long rowSum = 0;
        long expectedOrdinal = 0;
        long expectedStart = 0;
        for (int i = 0; i < frags.size(); i++) {
            Frag f = frags.get(i);
            assertEquals("ordinals must be dense from 0 (empties fill oversized-record gaps)", expectedOrdinal, f.ordinal);
            assertTrue("every stripe of a file-final scan must anchor its left edge", f.atStart);
            assertTrue("every stripe of a file-final scan must be complete on the right", f.atEnd);
            assertEquals("only the terminal stripe is eof", i == frags.size() - 1, f.eof);
            assertTrue("coverage end must not precede start", f.end >= f.start);
            assertEquals("byte sub-ranges must tile contiguously across stripe boundaries", expectedStart, f.start);
            expectedStart = f.end;
            rowSum += f.rows;
            expectedOrdinal++;
        }
        assertEquals("the head fragment must cover the file's first byte", 0L, frags.get(0).start);
        assertEquals("the last stripe must close to the file's byte length", totalBytes, frags.get(frags.size() - 1).end);
        assertEquals("per-stripe rows must sum to the file's true row count", totalRows, rowSum);
    }

    public void testDoubleExtremumOrdersNegativeZeroBelowPositiveZeroLikeRuntime() {
        // The runtime Min/MaxDoubleAggregator use Math.min/Math.max, which order -0.0 below +0.0; the
        // harvested extreme must match bit-for-bit regardless of arrival order. A primitive `<`/`>` compares
        // -0.0 == +0.0 and cannot cross them, so it would leave whichever arrived first -- a served MIN/MAX
        // that diverges from a full scan.
        Attribute[] cols = { new ReferenceAttribute(Source.EMPTY, null, "d", DataType.DOUBLE, Nullability.TRUE, null, false) };
        for (boolean positiveFirst : new boolean[] { true, false }) {
            ColumnStatsAccumulator acc = ColumnStatsAccumulator.forProjectedAttributes(cols);
            acc.acceptValueAt(0, positiveFirst ? 0.0 : -0.0);
            acc.acceptValueAt(0, positiveFirst ? -0.0 : 0.0);
            ExternalStats.ColumnStats stats = acc.snapshot().get("d");
            assertEquals(
                "MIN must be -0.0 (positiveFirst=" + positiveFirst + ")",
                Double.doubleToRawLongBits(-0.0),
                Double.doubleToRawLongBits((Double) stats.min())
            );
            assertEquals(
                "MAX must be +0.0 (positiveFirst=" + positiveFirst + ")",
                Double.doubleToRawLongBits(0.0),
                Double.doubleToRawLongBits((Double) stats.max())
            );
        }
    }

    public void testStripeSmallerThanRecordSpanTilesDenselyAndSumsExact() {
        // B=16, 10 records 8 bytes each (80 bytes): every ~2 records cross a stripe line.
        List<Frag> frags = emitCover(16, true, 0, 10L * RECORD_BYTES, 0, 10);
        assertDenseFileFinalCover(frags, 10, 10L * RECORD_BYTES);
    }

    public void testCoverSplitsAcrossMultipleStripes() {
        // 96-byte file on a 24-byte grid: the whole-file cover must span several stripe fragments, not one.
        List<Frag> frags = emitCover(24, true, 0, 12L * RECORD_BYTES, 0, 12);
        assertDenseFileFinalCover(frags, 12, 12L * RECORD_BYTES);
        assertTrue("the cover must span multiple stripe fragments", frags.size() > 1);
    }

    public void testRecordLargerThanStripeEmitsEmptyStripes() {
        // B=3 < record width (8): stripe lines fall between records, so skipped ordinals must surface as
        // explicit empty (zero-row) fragments to keep the cover contiguous.
        List<Frag> frags = emitCover(3, true, 0, 5L * RECORD_BYTES, 0, 5); // 40 bytes
        assertDenseFileFinalCover(frags, 5, 40);
        long emptyCount = frags.stream().filter(f -> f.rows == 0).count();
        assertTrue("oversized records must create at least one empty stripe", emptyCount > 0);
    }

    public void testRecordStartOnStripeBoundary() {
        // B=8 == record width: every record's start lands exactly on a stripe line, opening its stripe cleanly.
        List<Frag> frags = emitCover(8, true, 0, 6L * RECORD_BYTES, 0, 6);
        assertDenseFileFinalCover(frags, 6, 6L * RECORD_BYTES);
        // Each stripe holds exactly one record.
        for (Frag f : frags) {
            assertEquals("a grid aligned to the record width holds exactly one record per stripe", 1L, f.rows);
        }
    }

    public void testNonFinalChunkTrailingStripeIsNotMarkedComplete() {
        // One big stripe over a 64-byte non-file-final chunk: its trailing stripe must be a partial right
        // fragment (atEnd=false, eof=false) so the next chunk's continuation completes the cover.
        List<Frag> frags = emitCover(1024, false, 0, 8L * RECORD_BYTES, 0, 8);
        assertEquals("the whole non-final chunk is one stripe here", 1, frags.size());
        Frag only = frags.get(0);
        assertTrue("a non-final chunk's first stripe still anchors atStart at offset 0", only.atStart);
        assertFalse("a non-final chunk's trailing stripe must NOT be complete-on-the-right", only.atEnd);
        assertFalse("a non-final chunk must NOT mark its trailing stripe terminal", only.eof);
        assertEquals("the partial trailing stripe still counts its records", 8L, only.rows);
    }

    public void testNonFirstChunkAnchorsRightButNotLeft() {
        // A non-first, non-final middle chunk starting mid-stripe: it covers the stripe's right grid line
        // (atEnd) but not its left (atStart=false), so the prior chunk owns the left edge. B=16, chunk
        // [8, 32): stripe 0 is partial-left, stripe 1 is complete-on-both-sides.
        List<Frag> frags = emitCover(16, false, 8, 24, 8, 3); // records at 8, 16, 24
        assertEquals("chunk [8,32) spans stripes 0 and 1", 2, frags.size());
        Frag s0 = frags.get(0);
        assertEquals(0L, s0.ordinal);
        assertFalse("a chunk starting past the stripe's left line must NOT anchor atStart", s0.atStart);
        assertTrue("a chunk reaching the stripe's right line anchors atEnd", s0.atEnd);
        assertEquals("stripe 0's sub-range starts at the chunk's first byte", 8L, s0.start);
        assertEquals("stripe 0's sub-range ends at the grid line", 16L, s0.end);
        Frag s1 = frags.get(1);
        assertEquals(1L, s1.ordinal);
        assertTrue("stripe 1's left line falls inside the chunk", s1.atStart);
        assertTrue(s1.atEnd);
        assertFalse("a non-final chunk never marks eof", s1.eof);
    }
}
