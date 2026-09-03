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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnIteratorMetadata;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.columnar.ColumnarTestUtils.randomValidBlockSize;
import static org.elasticsearch.columnar.ColumnarTestUtils.readNumericMeta;

/**
 * End-to-end round-trip of numeric columns through a {@link Directory}, single- and multi-valued in
 * one format. Values are checked to come back in the exact order they were written (never
 * reordered), and the single-valued fast path (no value-address table) is exercised alongside
 * multi-valued columns with varying per-document counts.
 */
public class NumericColumnTests extends ESTestCase {

    public void testEmptyColumn() throws IOException {
        assertColumn(new long[between(1, 1000)][]);
    }

    public void testSingleValuedDense() throws IOException {
        int maxDoc = between(1, 3000);
        long[][] docs = new long[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            docs[d] = new long[] { randomLong() };
        }
        assertColumn(docs);
    }

    public void testSingleValuedSparse() throws IOException {
        int maxDoc = between(100, 4000);
        long[][] docs = new long[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            if (random().nextDouble() < 0.3) {
                docs[d] = new long[] { randomLong() };
            }
        }
        assertColumn(docs);
    }

    public void testMultiValued() throws IOException {
        int maxDoc = between(100, 4000);
        long[][] docs = new long[maxDoc][];
        for (int d = 0; d < maxDoc; d++) {
            if (random().nextDouble() < 0.6) {
                int count = between(1, 6);
                long[] values = new long[count];
                for (int i = 0; i < count; i++) {
                    values[i] = randomBoolean() ? between(-1000, 1000) : randomLong();
                }
                docs[d] = values;
            }
        }
        assertColumn(docs);
    }

    public void testBlockBoundaryValueCounts() throws IOException {
        // One doc holding exactly a block boundary's worth of values, plus neighbours.
        for (int n : new int[] { 127, 128, 129, 256, 257 }) {
            long[][] docs = new long[3][];
            docs[0] = new long[] { 42 };
            docs[1] = new long[n];
            for (int i = 0; i < n; i++) {
                docs[1][i] = 1000L * i;
            }
            docs[2] = new long[] { -7 };
            assertColumn(docs);
        }
    }

    public void testPartialBlockMonotonic() throws IOException {
        // Value counts that are NOT multiples of BLOCK_SIZE (128) with strictly ascending timestamps.
        // The final partial block is padded with the last real value (not zero); zero-padding would
        // break the monotonic delta/offset detection and bloat the block. Exact round-trip proves the
        // padding is harmless.
        for (int n : new int[] { 1, 5, 130, 200 }) {
            long[][] docs = new long[n][];
            long ts = 1_700_000_000_000L;
            for (int d = 0; d < n; d++) {
                ts += between(1, 1000);
                docs[d] = new long[] { ts };
            }
            assertColumn(docs);
        }
    }

    public void testLargeNumValuesMetadataRoundTrips() throws IOException {
        final long numValues = randomLongBetween((long) Integer.MAX_VALUE + 1, (long) Integer.MAX_VALUE * 2);
        final int blockSize = randomValidBlockSize();
        final int numDocsWithField = between(1, 10);

        // NOTE: OFFSET_DENSE avoids writing a sparse-iterator data structure; OFFSET_EMPTY would
        // trigger an early return in writeTo that skips numValues encoding entirely.
        final ColumnIteratorMetadata iteratorMeta = new ColumnIteratorMetadata(
            ColumnIteratorMetadata.OFFSET_DENSE,
            0L,
            (short) -1,
            (byte) -1,
            numDocsWithField,
            numDocsWithField
        );
        final NumericColumnMetadata meta = new NumericColumnMetadata(
            iteratorMeta,
            numDocsWithField,
            numValues,
            blockSize,
            BlockBytesCodec.IDENTITY_ID,
            ForTerminal.ID,
            new byte[] { DeltaTransform.ID },
            0L,
            0L,
            0L,
            new byte[0],
            0L,
            0L,
            new byte[0],
            null
        );

        final byte[] buf = new byte[256];
        final ByteArrayDataOutput out = new ByteArrayDataOutput(buf);
        meta.writeTo(out);
        final ByteArrayDataInput in = new ByteArrayDataInput(buf, 0, out.getPosition());
        final NumericColumnMetadata roundTripped = NumericColumnMetadata.readFrom(in, numDocsWithField, FormatVersion.CURRENT);

        assertEquals(numValues, roundTripped.numValues());
        assertEquals((numValues + blockSize - 1) / blockSize, roundTripped.numBlocks());
    }

    private void assertColumn(long[][] docValues) throws IOException {
        int numDocsWithField = 0;
        int numValues = 0;
        for (long[] values : docValues) {
            if (values != null && values.length > 0) {
                numDocsWithField++;
                numValues += values.length;
            }
        }
        int maxDoc = docValues.length;
        byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);

        try (Directory dir = newDirectory()) {
            NumericColumnMetadata written;
            try (
                IndexOutput out = dir.createOutput("num.cnd", IOContext.DEFAULT);
                IndexOutput skip = dir.createOutput("num.cns", IOContext.DEFAULT)
            ) {
                ColumnarCodecUtil.writeHeader(out, "ColumNARData", FormatVersion.CURRENT, segmentId, "");
                ColumnarCodecUtil.writeHeader(skip, "ColumNARSkipIndex", FormatVersion.CURRENT, segmentId, "");
                written = NumericColumnWriter.write(
                    maxDoc,
                    numDocsWithField,
                    numValues,
                    () -> cursor(docValues),
                    NumericPipeline.defaultPipeline(randomValidBlockSize()),
                    BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                    SkipIndexCodec.forId(SkipIndexCodec.MULTI_LEVEL_ID),
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

            final NumericColumnMetadata read = readNumericMeta(dir, "num.cnm", segmentId, maxDoc);
            assertEquals(numValues > numDocsWithField, read.multiValued());

            try (IndexInput data = dir.openInput("num.cnd", IOContext.DEFAULT)) {
                CodecUtil.checksumEntireFile(data);
                ColumnarCodecUtil.checkHeader(data, "ColumNARData", segmentId, "");
                NumericColumnReader reader = new NumericColumnReader(read, data);

                ColumnIterator iterator = reader.iterator();
                for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                    int rank = iterator.rank();
                    long first = reader.firstValueAddress(rank);
                    long count = reader.valueCount(rank);
                    assertEquals("value count at doc " + doc, docValues[doc].length, count);
                    for (int i = 0; i < count; i++) {
                        // exact written order, never sorted
                        assertEquals("doc " + doc + " value " + i, docValues[doc][i], reader.valueAt(first + i));
                    }
                }
            }
        }
    }

    /** A fresh {@link SortedNumericDocValues} over the per-document values, yielding them in array order. */
    private static NumericColumnValues cursor(long[][] docValues) {
        return new NumericColumnValues() {
            private int doc = -1;
            private int valueIndex;

            @Override
            public int valueCount() {
                return docValues[doc].length;
            }

            @Override
            public long nextValue() {
                return docValues[doc][valueIndex++];
            }

            @Override
            public int docID() {
                return doc;
            }

            @Override
            public int nextDoc() {
                for (doc = doc + 1; doc < docValues.length; doc++) {
                    if (docValues[doc] != null && docValues[doc].length > 0) {
                        valueIndex = 0;
                        return doc;
                    }
                }
                return doc = DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                return docValues.length;
            }
        };
    }

}
