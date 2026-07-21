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
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.elasticsearch.columnar.substrate.BlockBytesCodec;
import org.elasticsearch.columnar.substrate.ColumnIterator;
import org.elasticsearch.columnar.substrate.ColumnarCodecUtil;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

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
            try (IndexOutput out = dir.createOutput("num.cnd", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(out, "ColumnarNumericData", segmentId, "");
                written = NumericColumnWriter.write(
                    maxDoc,
                    numDocsWithField,
                    numValues,
                    () -> cursor(docValues),
                    BlockBytesCodec.forId(BlockBytesCodec.IDENTITY_ID),
                    dir,
                    IOContext.DEFAULT,
                    out
                );
                ColumnarCodecUtil.writeFooter(out);
            }

            try (IndexOutput meta = dir.createOutput("num.cnm", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(meta, "ColumnarNumericMeta", segmentId, "");
                written.writeTo(meta);
                ColumnarCodecUtil.writeFooter(meta);
            }

            NumericColumnMetadata read;
            try (ChecksumIndexInput meta = dir.openChecksumInput("num.cnm")) {
                ColumnarCodecUtil.checkHeader(meta, "ColumnarNumericMeta", segmentId, "");
                read = NumericColumnMetadata.readFrom(meta, maxDoc);
                ColumnarCodecUtil.checkFooter(meta);
            }
            assertEquals(numValues > numDocsWithField, read.multiValued());

            try (IndexInput data = dir.openInput("num.cnd", IOContext.DEFAULT)) {
                CodecUtil.checksumEntireFile(data);
                ColumnarCodecUtil.checkHeader(data, "ColumnarNumericData", segmentId, "");
                NumericColumnReader reader = new NumericColumnReader(read, data);

                ColumnIterator iterator = reader.iterator();
                for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                    int rank = iterator.index();
                    int first = reader.firstOrdinal(rank);
                    int count = reader.valueCount(rank);
                    assertEquals("value count at doc " + doc, docValues[doc].length, count);
                    for (int i = 0; i < count; i++) {
                        // exact written order, never sorted
                        assertEquals("doc " + doc + " value " + i, docValues[doc][i], reader.valueForOrdinal(first + i));
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
