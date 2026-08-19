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

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.elasticsearch.columnar.ColumnarTestUtils.readNumericMeta;
import static org.elasticsearch.columnar.ColumnarTestUtils.singleValuedCursor;

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
        boolean vectorizable = iterator.isDense() && read.multiValued() == false;
        return new Opened(
            new ColumnarNumericBinaryDocValues(reader, iterator, maxDoc, vectorizable, read.skipper(), skipIndex),
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
