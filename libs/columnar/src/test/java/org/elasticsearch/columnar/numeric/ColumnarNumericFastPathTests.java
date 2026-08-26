/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.numeric;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.elasticsearch.columnar.ColumnarTestUtils.readNumericMeta;
import static org.elasticsearch.columnar.ColumnarTestUtils.singleValuedCursor;
import static org.elasticsearch.columnar.ColumnarTestUtils.sparseSingleValuedCursor;

/**
 * Correctness of the dense single-valued fast paths on {@link ColumnarNumericBinaryDocValues}: the
 * vectorized range iterator (both its per-doc {@code matches()} path and its bulk {@code intoBitSet}
 * override) and the bulk {@link ColumnarNumericBinaryDocValues#bulkLongs} reader must agree with a
 * brute-force scan of the same values. Runs across value distributions, segment sizes that straddle
 * block boundaries, and a range of selectivities including empty and full matches.
 */
public class ColumnarNumericFastPathTests extends ESTestCase {

    private byte[] segmentId;
    private final List<IndexInput> opened = new ArrayList<>();

    public void testRangeAndBulkAcrossWorkloads() throws IOException {
        for (int iter = 0; iter < 40; iter++) {
            int maxDoc = randomFrom(1, 127, 128, 129, 200, between(300, 4000));
            long[] values = new long[maxDoc];
            switch (between(0, 3)) {
                case 0 -> { // narrow range: good selectivity for a mid band
                    for (int d = 0; d < maxDoc; d++) {
                        values[d] = between(0, 1000);
                    }
                }
                case 1 -> { // monotonic timestamps
                    long base = randomLong() >> 20;
                    for (int d = 0; d < maxDoc; d++) {
                        base += between(0, 50);
                        values[d] = base;
                    }
                }
                case 2 -> { // full-range random
                    for (int d = 0; d < maxDoc; d++) {
                        values[d] = randomLong();
                    }
                }
                default -> { // low cardinality
                    long[] alphabet = new long[between(1, 8)];
                    for (int i = 0; i < alphabet.length; i++) {
                        alphabet[i] = randomLong();
                    }
                    for (int d = 0; d < maxDoc; d++) {
                        values[d] = alphabet[between(0, alphabet.length - 1)];
                    }
                }
            }

            try (Directory dir = newDirectory()) {
                try {
                    ColumnarNumericBinaryDocValues dv = writeAndOpen(dir, values, false).dv();
                    assertRangeQueries(values, dv);
                    assertBulkReads(dir, values);
                } finally {
                    IOUtils.close(opened);
                    opened.clear();
                }
            }
        }
    }

    public void testBulkLongsBailsWhenDuplicatesPossible() throws IOException {
        long[] values = new long[64];
        for (int d = 0; d < values.length; d++) {
            values[d] = between(0, 1000);
        }
        try (Directory dir = newDirectory()) {
            try {
                ColumnarNumericBinaryDocValues dv = writeAndOpen(dir, values, false).dv();
                // {0, 1, 1, 3}: endpoints (0..3, count 4) look dense, but doc 1 repeats. Reading a contiguous
                // slice would be wrong, so with mayContainDuplicates the bulk path must decline and never
                // touch the sink, leaving the caller to read per document.
                int[] docs = { 0, 1, 1, 3 };
                boolean applied = dv.bulkLongs(docs, 0, docs.length, true, (block, from, length) -> {
                    throw new AssertionError("sink must not be touched when duplicates are possible");
                });
                assertFalse("bulk path must decline when duplicates are possible", applied);
            } finally {
                IOUtils.close(opened);
                opened.clear();
            }
        }
    }

    public void testSkipperAwareRange() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            int maxDoc = randomFrom(200, between(4097, 20000), between(1, 130));
            long[] values = new long[maxDoc];
            if (randomBoolean()) {
                // monotonic: intervals are cleanly skippable
                long base = randomLong() >> 20;
                for (int d = 0; d < maxDoc; d++) {
                    base += between(0, 20);
                    values[d] = base;
                }
            } else {
                for (int d = 0; d < maxDoc; d++) {
                    values[d] = between(0, 5000);
                }
            }

            try (Directory dir = newDirectory()) {
                try {
                    Opened opened = writeAndOpen(dir, values, true);
                    assertNotNull("a skip-indexed column must carry a skipper", opened.meta().skipper());

                    DocValuesSkipper globals = freshSkipper(opened);
                    assertEquals(maxDoc, globals.docCount());
                    assertEquals(Arrays.stream(values).min().getAsLong(), globals.minValue());
                    assertEquals(Arrays.stream(values).max().getAsLong(), globals.maxValue());
                    assertEquals(1, globals.maxValueCount());

                    for (int q = 0; q < 6; q++) {
                        long pivot = values[between(0, maxDoc - 1)];
                        int half = between(0, 1500);
                        long lo = pivot - half;
                        long hi = pivot + between(0, half + 1);

                        FixedBitSet expected = new FixedBitSet(maxDoc);
                        for (int d = 0; d < maxDoc; d++) {
                            if (values[d] >= lo && values[d] <= hi) {
                                expected.set(d);
                            }
                        }

                        // Path A: skipper-aware matches(), driven doc by doc.
                        DocIdSetIterator disi = opened.dv().rangeIterator(lo, hi);
                        assertNotNull(disi);
                        FixedBitSet viaMatches = new FixedBitSet(maxDoc);
                        for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
                            viaMatches.set(doc);
                        }
                        assertEquals("skipper range via matches() [" + lo + "," + hi + "]", expected, viaMatches);

                        // Path B: skipper-aware bulk intoBitSet.
                        DocIdSetIterator disi2 = opened.dv().rangeIterator(lo, hi);
                        TwoPhaseIterator tpi = TwoPhaseIterator.unwrap(disi2);
                        assertNotNull(tpi);
                        tpi.approximation().nextDoc();
                        FixedBitSet viaIntoBitSet = new FixedBitSet(maxDoc);
                        tpi.intoBitSet(maxDoc, viaIntoBitSet, 0);
                        assertEquals("skipper range via intoBitSet [" + lo + "," + hi + "]", expected, viaIntoBitSet);
                    }
                } finally {
                    IOUtils.close(opened);
                    opened.clear();
                }
            }
        }
    }

    private void assertRangeQueries(long[] values, ColumnarNumericBinaryDocValues dv) throws IOException {
        int maxDoc = values.length;
        for (int q = 0; q < 8; q++) {
            long lo;
            long hi;
            switch (between(0, 3)) {
                case 0 -> { // empty band below the domain
                    lo = Long.MIN_VALUE;
                    hi = Long.MIN_VALUE;
                }
                case 1 -> { // full domain
                    lo = Long.MIN_VALUE;
                    hi = Long.MAX_VALUE;
                }
                default -> { // a random band around a real value
                    long pivot = values[between(0, maxDoc - 1)];
                    long half = randomBoolean() ? 0 : between(1, 500);
                    lo = pivot - half;
                    hi = pivot + between(0, (int) half + 1);
                }
            }

            FixedBitSet expected = new FixedBitSet(maxDoc);
            for (int d = 0; d < maxDoc; d++) {
                if (values[d] >= lo && values[d] <= hi) {
                    expected.set(d);
                }
            }

            // Path A: drive the DISI directly (per-doc matches()).
            DocIdSetIterator disi = dv.rangeIterator(lo, hi);
            assertNotNull("dense single-valued must expose a range iterator", disi);
            FixedBitSet viaMatches = new FixedBitSet(maxDoc);
            for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
                viaMatches.set(doc);
            }
            assertEquals("range via matches() [" + lo + "," + hi + "]", expected, viaMatches);

            // Path B: the bulk intoBitSet override, reached through TwoPhaseIterator.unwrap.
            DocIdSetIterator disi2 = dv.rangeIterator(lo, hi);
            TwoPhaseIterator tpi = TwoPhaseIterator.unwrap(disi2);
            assertNotNull(tpi);
            DocIdSetIterator approximation = tpi.approximation();
            approximation.nextDoc();
            FixedBitSet viaIntoBitSet = new FixedBitSet(maxDoc);
            tpi.intoBitSet(maxDoc, viaIntoBitSet, 0);
            assertEquals("range via intoBitSet [" + lo + "," + hi + "]", expected, viaIntoBitSet);

            assertDocIDRunEndContract(dv, expected, lo, hi);
        }
    }

    private void assertBulkReads(Directory dir, long[] values) throws IOException {
        int maxDoc = values.length;
        // Read a random ascending subset of docs in bulk and compare to a per-doc scan.
        for (int q = 0; q < 4; q++) {
            List<Integer> picks = new ArrayList<>();
            for (int d = 0; d < maxDoc; d++) {
                if (randomBoolean()) {
                    picks.add(d);
                }
            }
            if (picks.isEmpty()) {
                picks.add(between(0, maxDoc - 1));
            }
            int[] docs = picks.stream().mapToInt(Integer::intValue).toArray();

            ColumnarNumericBinaryDocValues dv = open(dir, maxDoc).dv();
            long[] collected = new long[docs.length];
            boolean applied = dv.bulkLongs(docs, 0, docs.length, false, new LongBlockSink() {
                private int pos;

                @Override
                public void appendLongs(long[] block, int from, int length) {
                    System.arraycopy(block, from, collected, pos, length);
                    pos += length;
                }
            });
            assertTrue("dense single-valued must support bulk reads", applied);
            for (int i = 0; i < docs.length; i++) {
                assertEquals("bulk value at doc " + docs[i], values[docs[i]], collected[i]);
            }
        }
    }

    /**
     * A sparse column serves the same bulk value path a dense one does. Ordinals rather than document ids
     * drive the run detection, so a sparse column's runs are the stretches where requested documents are
     * adjacent in the presence set — which is a weaker condition than adjacent document ids, and means a
     * sparse column can produce longer runs than a dense one over the same document batch.
     */
    public void testBulkLongsOnSparseColumn() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            final int maxDoc = randomFrom(1, 129, 200, between(300, 4000));
            // Mix densities: very sparse exercises IndexedDISI SPARSE blocks, mostly-dense its DENSE blocks
            // (where docIDRunEnd reports whole words), and everything between straddles the two.
            final double density = randomFrom(0.02, 0.3, 0.75, 0.95, 1.0);
            final Long[] values = new Long[maxDoc];
            for (int d = 0; d < maxDoc; d++) {
                if (random().nextDouble() < density) {
                    values[d] = (long) between(-10_000, 10_000);
                }
            }

            try (Directory dir = newDirectory()) {
                try {
                    final Opened opened = writeAndOpenSparse(dir, values, false);
                    // Only documents that have a value: bulkLongs promises one value per requested document.
                    final int[] present = IntStream.range(0, maxDoc).filter(d -> values[d] != null).toArray();
                    if (present.length == 0) {
                        continue;
                    }
                    final int offset = between(0, present.length - 1);
                    final int count = between(1, present.length - offset);

                    final List<Long> actual = new ArrayList<>();
                    final boolean applied = opened.dv().bulkLongs(present, offset, count, false, (block, from, length) -> {
                        for (int i = 0; i < length; i++) {
                            actual.add(block[from + i]);
                        }
                    });
                    assertTrue("sparse columns must take the bulk path", applied);

                    final List<Long> expected = new ArrayList<>();
                    for (int i = 0; i < count; i++) {
                        expected.add(values[present[offset + i]]);
                    }
                    assertEquals(expected, actual);
                } finally {
                    IOUtils.close(opened);
                    opened.clear();
                }
            }
        }
    }

    /** A document with no value has no value to append, so the bulk path declines rather than inventing one. */
    public void testBulkLongsDeclinesWhenADocumentHasNoValue() throws IOException {
        final Long[] values = new Long[64];
        for (int d = 0; d < values.length; d++) {
            values[d] = d == 7 ? null : (long) between(0, 1000);
        }
        try (Directory dir = newDirectory()) {
            try {
                final ColumnarNumericBinaryDocValues dv = writeAndOpenSparse(dir, values, false).dv();
                final int[] docs = { 5, 6, 7, 8 }; // doc 7 has no value
                final boolean applied = dv.bulkLongs(docs, 0, docs.length, false, (block, from, length) -> {
                    throw new AssertionError("sink must not be touched when a document has no value");
                });
                assertFalse("bulk path must decline when a requested document has no value", applied);
            } finally {
                IOUtils.close(opened);
                opened.clear();
            }
        }
    }

    private Opened writeAndOpenSparse(Directory dir, Long[] values, boolean withSkipper) throws IOException {
        segmentId = new byte[16];
        random().nextBytes(segmentId);
        int numDocsWithField = 0;
        for (Long value : values) {
            if (value != null) {
                numDocsWithField++;
            }
        }
        NumericColumnMetadata written;
        try (
            IndexOutput out = dir.createOutput("num.cnd", IOContext.DEFAULT);
            IndexOutput skip = dir.createOutput("num.cns", IOContext.DEFAULT)
        ) {
            ColumnarCodecUtil.writeHeader(out, "ColumNARData", FormatVersion.CURRENT, segmentId, "");
            ColumnarCodecUtil.writeHeader(skip, "ColumNARSkipIndex", FormatVersion.CURRENT, segmentId, "");
            written = NumericColumnWriter.write(
                values.length,
                numDocsWithField,
                numDocsWithField,
                () -> sparseSingleValuedCursor(values),
                NumericPipeline.defaultPipeline(randomValidBlockSize()),
                BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                withSkipper ? SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID) : null,
                dir,
                IOContext.DEFAULT,
                out,
                skip
            );
            ColumnarCodecUtil.writeFooter(out);
            ColumnarCodecUtil.writeFooter(skip);
        }
        try (IndexOutput meta = dir.createOutput("num.cnm", IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(meta, "ColumNARMeta", FormatVersion.CURRENT, segmentId, "");
            written.writeTo(meta);
            ColumnarCodecUtil.writeFooter(meta);
        }
        return open(dir, values.length);
    }

    /**
     * A sparse column serves the same vectorized range iterator a dense one does and must agree with a
     * brute-force scan. Documents with no value never match, which is exactly what the run arithmetic can
     * get wrong: a run of matching ordinals is only a run of matching documents for as long as the
     * documents stay present, so a run claimed past a gap would set bits for documents that have no value.
     */
    public void testRangeOnSparseColumn() throws IOException {
        for (int iter = 0; iter < 30; iter++) {
            final int maxDoc = randomFrom(1, 129, 200, between(300, 5000));
            final double density = randomFrom(0.02, 0.3, 0.75, 0.95, 1.0);
            final Long[] values = new Long[maxDoc];
            for (int d = 0; d < maxDoc; d++) {
                if (random().nextDouble() < density) {
                    values[d] = (long) between(-500, 500);
                }
            }
            final boolean withSkipper = randomBoolean();
            try (Directory dir = newDirectory()) {
                try {
                    assertSparseRangeQueries(values, writeAndOpenSparse(dir, values, withSkipper).dv());
                } finally {
                    IOUtils.close(opened);
                    opened.clear();
                }
            }
        }
    }

    private void assertSparseRangeQueries(Long[] values, ColumnarNumericBinaryDocValues dv) throws IOException {
        final int maxDoc = values.length;
        final List<Long> present = new ArrayList<>();
        for (Long value : values) {
            if (value != null) {
                present.add(value);
            }
        }

        for (int q = 0; q < 8; q++) {
            final long lo;
            final long hi;
            if (present.isEmpty() || between(0, 3) == 0) {
                lo = randomBoolean() ? Long.MIN_VALUE : Long.MAX_VALUE;
                hi = lo == Long.MIN_VALUE && randomBoolean() ? Long.MAX_VALUE : lo;
            } else {
                final long pivot = randomFrom(present);
                final long half = randomBoolean() ? 0 : between(1, 300);
                lo = pivot - half;
                hi = pivot + between(0, (int) half + 1);
            }

            final FixedBitSet expected = new FixedBitSet(maxDoc);
            for (int d = 0; d < maxDoc; d++) {
                if (values[d] != null && values[d] >= lo && values[d] <= hi) {
                    expected.set(d);
                }
            }
            final String band = " [" + lo + "," + hi + "]";

            // Path A: drive the DISI directly (per-doc matches()).
            final DocIdSetIterator disi = dv.rangeIterator(lo, hi);
            assertNotNull("a single-valued column must expose a range iterator" + band, disi);
            final FixedBitSet viaMatches = new FixedBitSet(maxDoc);
            for (int doc = disi.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = disi.nextDoc()) {
                viaMatches.set(doc);
            }
            assertEquals("range via matches()" + band, expected, viaMatches);

            // Path B: the bulk intoBitSet override in one shot.
            final TwoPhaseIterator whole = TwoPhaseIterator.unwrap(dv.rangeIterator(lo, hi));
            assertNotNull(whole);
            whole.approximation().nextDoc();
            final FixedBitSet viaIntoBitSet = new FixedBitSet(maxDoc);
            whole.intoBitSet(maxDoc, viaIntoBitSet, 0);
            assertEquals("range via intoBitSet" + band, expected, viaIntoBitSet);

            // Path C: the same in two chunks, so the run loop has to stop mid-column and resume. A run
            // truncated by upTo must not be re-credited or skipped when the next call picks up.
            final TwoPhaseIterator chunked = TwoPhaseIterator.unwrap(dv.rangeIterator(lo, hi));
            assertNotNull(chunked);
            chunked.approximation().nextDoc();
            final FixedBitSet viaChunks = new FixedBitSet(maxDoc);
            final int mid = Math.max(1, maxDoc / 2);
            chunked.intoBitSet(mid, viaChunks, 0);
            if (chunked.approximation().docID() != DocIdSetIterator.NO_MORE_DOCS) {
                chunked.intoBitSet(maxDoc, viaChunks, 0);
            }
            assertEquals("range via chunked intoBitSet" + band, expected, viaChunks);

            // Path D: the docIDRunEnd contract.
            assertDocIDRunEndContract(dv, expected, lo, hi);
        }
    }

    /**
     * The {@code docIDRunEnd} contract that bulk scorers rely on: every document in
     * {@code [docID(), docIDRunEnd())} is a real match, and asking does not move the approximation.
     *
     * <p>The approximation here is the column iterator, so its members are the documents that have a
     * value, not the documents whose value is in range.
     *
     * <p>Run twice: the first pass asks before {@code matches()} has confirmed anything, so an
     * implementation that assumes its caller confirmed first would claim a run over an untested document.
     * The second pass confirms first.
     */
    private void assertDocIDRunEndContract(ColumnarNumericBinaryDocValues dv, FixedBitSet expected, long lo, long hi) throws IOException {
        final String band = " [" + lo + "," + hi + "]";

        final TwoPhaseIterator unconfirmed = TwoPhaseIterator.unwrap(dv.rangeIterator(lo, hi));
        assertNotNull(unconfirmed);
        final DocIdSetIterator unconfirmedDocs = unconfirmed.approximation();
        for (int doc = unconfirmedDocs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = unconfirmedDocs.nextDoc()) {
            final int runEnd = unconfirmed.docIDRunEnd();
            assertEquals("docIDRunEnd moved the approximation at doc " + doc + band, doc, unconfirmedDocs.docID());
            for (int d = doc; d < runEnd; d++) {
                assertTrue("unconfirmed docIDRunEnd from doc " + doc + " claimed doc " + d + band, expected.get(d));
            }
        }

        final TwoPhaseIterator confirmed = TwoPhaseIterator.unwrap(dv.rangeIterator(lo, hi));
        assertNotNull(confirmed);
        final DocIdSetIterator confirmedDocs = confirmed.approximation();
        for (int doc = confirmedDocs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = confirmedDocs.nextDoc()) {
            final boolean matches = confirmed.matches();
            assertEquals("matches() at doc " + doc + band, expected.get(doc), matches);
            if (matches == false) {
                continue;
            }
            final int runEnd = confirmed.docIDRunEnd();
            assertEquals("docIDRunEnd moved the approximation at doc " + doc + band, doc, confirmedDocs.docID());
            for (int d = doc; d < runEnd; d++) {
                assertTrue("docIDRunEnd from doc " + doc + " claimed doc " + d + band, expected.get(d));
            }
        }
    }

    /** A readable column plus the state needed to build fresh readers/skippers over the same data. */
    private record Opened(
        ColumnarNumericBinaryDocValues dv,
        NumericColumnMetadata meta,
        IndexInput data,
        IndexInput skipIndex,
        int maxDoc
    ) {}

    private Opened writeAndOpen(Directory dir, long[] values, boolean withSkipper) throws IOException {
        segmentId = new byte[16];
        random().nextBytes(segmentId);
        NumericColumnMetadata written;
        try (
            IndexOutput out = dir.createOutput("num.cnd", IOContext.DEFAULT);
            IndexOutput skip = dir.createOutput("num.cns", IOContext.DEFAULT)
        ) {
            ColumnarCodecUtil.writeHeader(out, "ColumNARData", FormatVersion.CURRENT, segmentId, "");
            ColumnarCodecUtil.writeHeader(skip, "ColumNARSkipIndex", FormatVersion.CURRENT, segmentId, "");
            written = NumericColumnWriter.write(
                values.length,
                values.length,
                values.length,
                () -> singleValuedCursor(values),
                NumericPipeline.defaultPipeline(randomValidBlockSize()),
                BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                withSkipper ? SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID) : null,
                dir,
                IOContext.DEFAULT,
                out,
                skip
            );
            ColumnarCodecUtil.writeFooter(out);
            ColumnarCodecUtil.writeFooter(skip);
        }
        try (IndexOutput meta = dir.createOutput("num.cnm", IOContext.DEFAULT)) {
            ColumnarCodecUtil.writeHeader(meta, "ColumNARMeta", FormatVersion.CURRENT, segmentId, "");
            written.writeTo(meta);
            ColumnarCodecUtil.writeFooter(meta);
        }
        return open(dir, values.length);
    }

    private Opened open(Directory dir, int maxDoc) throws IOException {
        final NumericColumnMetadata read = readNumericMeta(dir, "num.cnm", segmentId, maxDoc);
        IndexInput data = dir.openInput("num.cnd", IOContext.DEFAULT);
        opened.add(data);
        CodecUtil.checksumEntireFile(data);
        ColumnarCodecUtil.checkHeader(data, "ColumNARData", segmentId, "");
        IndexInput skipIndex = dir.openInput("num.cns", IOContext.DEFAULT);
        opened.add(skipIndex);
        CodecUtil.checksumEntireFile(skipIndex);
        ColumnarCodecUtil.checkHeader(skipIndex, "ColumNARSkipIndex", segmentId, "");
        NumericColumnReader reader = new NumericColumnReader(read, data);
        ColumnIterator iterator = reader.iterator();
        return new Opened(
            new ColumnarNumericBinaryDocValues(reader, iterator, maxDoc, read.skipper(), skipIndex),
            read,
            data,
            skipIndex,
            maxDoc
        );
    }

    /** A fresh (unadvanced) skipper over the column; {@link org.apache.lucene.index.DocValuesSkipper} is stateful. */
    private static NumericColumnSkipper freshSkipper(Opened opened) throws IOException {
        return new NumericColumnSkipper(opened.meta().skipper(), opened.skipIndex());
    }

}
