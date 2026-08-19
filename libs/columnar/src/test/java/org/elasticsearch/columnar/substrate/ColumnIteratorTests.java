/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.substrate;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.columnar.FormatVersion;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

/**
 * Round-trips the column iterator through a real {@link Directory} for the empty, dense, and sparse
 * shapes, checking that iteration, value ordinals ({@link ColumnIterator#index()}),
 * {@link ColumnIterator#advanceExact}, and {@link ColumnIterator#intoBitSet} all agree with a
 * reference {@link FixedBitSet} of the documents that have a value.
 */
public class ColumnIteratorTests extends ESTestCase {

    private static final String DATA_NAME = "ColumnIteratorData";
    private static final String META_NAME = "ColumnIteratorMeta";

    public void testEmpty() throws IOException {
        int maxDoc = between(1, 5000);
        assertRoundTrip(maxDoc, new FixedBitSet(maxDoc));
    }

    public void testDense() throws IOException {
        int maxDoc = between(1, 5000);
        FixedBitSet all = new FixedBitSet(maxDoc);
        all.set(0, maxDoc);
        assertRoundTrip(maxDoc, all);
    }

    public void testSingleDocument() throws IOException {
        int maxDoc = between(1, 5000);
        FixedBitSet bits = new FixedBitSet(maxDoc);
        bits.set(between(0, maxDoc - 1));
        assertRoundTrip(maxDoc, bits);
    }

    public void testBoundaryDocuments() throws IOException {
        int maxDoc = between(2, 5000);
        FixedBitSet bits = new FixedBitSet(maxDoc);
        bits.set(0);
        bits.set(maxDoc - 1);
        assertRoundTrip(maxDoc, bits);
    }

    public void testVerySparse() throws IOException {
        assertRoundTrip(10000, randomBits(10000, 0.01));
    }

    public void testMostlyDense() throws IOException {
        // Exercises IndexedDISI's internal DENSE blocks: many docs present, but not all.
        assertRoundTrip(10000, randomBits(10000, 0.9));
    }

    public void testRandom() throws IOException {
        for (int iter = 0; iter < 20; iter++) {
            int maxDoc = between(1, 8000);
            assertRoundTrip(maxDoc, randomBits(maxDoc, randomDoubleBetween(0.0, 1.0, true)));
        }
    }

    private FixedBitSet randomBits(int maxDoc, double density) {
        FixedBitSet bits = new FixedBitSet(maxDoc);
        for (int doc = 0; doc < maxDoc; doc++) {
            if (random().nextDouble() < density) {
                bits.set(doc);
            }
        }
        return bits;
    }

    private void assertRoundTrip(int maxDoc, FixedBitSet expected) throws IOException {
        int cardinality = expected.cardinality();
        byte[] segmentId = new byte[16];
        random().nextBytes(segmentId);

        try (Directory dir = newDirectory()) {
            ColumnIteratorMetadata written;
            try (IndexOutput out = dir.createOutput("iterator.cnd", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(out, DATA_NAME, FormatVersion.CURRENT, segmentId, "");
                DocIdSetIterator docsWithField = new BitSetIterator(expected, cardinality);
                written = ColumnIteratorWriter.write(docsWithField, cardinality, maxDoc, out);
                ColumnarCodecUtil.writeFooter(out);
            }

            try (IndexOutput meta = dir.createOutput("iterator.cnm", IOContext.DEFAULT)) {
                ColumnarCodecUtil.writeHeader(meta, META_NAME, FormatVersion.CURRENT, segmentId, "");
                written.writeTo(meta);
                ColumnarCodecUtil.writeFooter(meta);
            }

            // Metadata survives a serialization round-trip.
            ColumnIteratorMetadata read;
            try (ChecksumIndexInput meta = dir.openChecksumInput("iterator.cnm")) {
                final FormatVersion formatVersion = ColumnarCodecUtil.checkHeader(meta, META_NAME, segmentId, "");
                read = ColumnIteratorMetadata.readFrom(meta, maxDoc, formatVersion);
                ColumnarCodecUtil.checkFooter(meta);
            }
            assertEquals(written, read);
            assertEquals(cardinality, read.numDocsWithField());

            try (IndexInput data = dir.openInput("iterator.cnd", IOContext.DEFAULT)) {
                CodecUtil.checksumEntireFile(data);
                ColumnarCodecUtil.checkHeader(data, DATA_NAME, segmentId, "");
                ColumnIteratorReader reader = new ColumnIteratorReader(read, data);
                assertIteration(reader, expected, cardinality);
                assertAdvanceExact(reader, expected, maxDoc);
                assertIntoBitSet(reader, expected, maxDoc);
                assertIntoBitSetResumes(reader, expected, maxDoc);
            }
        }
    }

    private void assertIteration(ColumnIteratorReader reader, FixedBitSet expected, int cardinality) throws IOException {
        ColumnIterator iterator = reader.iterator();
        assertEquals(cardinality, iterator.cost());
        int rank = 0;
        BitSetIterator reference = new BitSetIterator(expected, cardinality);
        for (int doc = reference.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = reference.nextDoc()) {
            assertEquals(doc, iterator.nextDoc());
            assertEquals("value ordinal at doc " + doc, rank, iterator.index());
            rank++;
        }
        assertEquals(DocIdSetIterator.NO_MORE_DOCS, iterator.nextDoc());
        assertEquals(cardinality, rank);
    }

    private void assertAdvanceExact(ColumnIteratorReader reader, FixedBitSet expected, int maxDoc) throws IOException {
        ColumnIterator iterator = reader.iterator();
        int seen = 0;
        for (int doc = 0; doc < maxDoc; doc++) {
            boolean present = expected.get(doc);
            assertEquals("iterator at doc " + doc, present, iterator.advanceExact(doc));
            if (present) {
                assertEquals("value ordinal at doc " + doc, seen, iterator.index());
                seen++;
            }
        }
    }

    private void assertIntoBitSet(ColumnIteratorReader reader, FixedBitSet expected, int maxDoc) throws IOException {
        ColumnIterator iterator = reader.iterator();
        FixedBitSet actual = new FixedBitSet(maxDoc);
        if (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            iterator.intoBitSet(maxDoc, actual, 0);
        }
        assertEquals(expected, actual);
    }

    /** A partial range fills only the docs below upTo, and a second call resumes to complete the set. */
    private void assertIntoBitSetResumes(ColumnIteratorReader reader, FixedBitSet expected, int maxDoc) throws IOException {
        ColumnIterator iterator = reader.iterator();
        FixedBitSet actual = new FixedBitSet(maxDoc);
        if (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int upTo = Math.max(1, maxDoc / 2);
            iterator.intoBitSet(upTo, actual, 0);
            for (int doc = 0; doc < maxDoc; doc++) {
                boolean expectedBelowUpTo = doc < upTo && expected.get(doc);
                assertEquals("partial fill at doc " + doc, expectedBelowUpTo, actual.get(doc));
            }
            iterator.intoBitSet(maxDoc, actual, 0);
        }
        assertEquals(expected, actual);
    }
}
