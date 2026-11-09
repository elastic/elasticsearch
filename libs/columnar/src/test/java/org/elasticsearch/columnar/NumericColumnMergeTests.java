/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * Exercises the merge fast path: values are written across several segments (with deletions), then
 * force-merged. The merge re-runs the encoder pipeline over the source segments, reading their values
 * in bulk off disk. The merged column must hold exactly the surviving values, in doc order, and stay
 * queryable.
 */
public class NumericColumnMergeTests extends ESTestCase {

    /** A numeric column is written without a temporary file, on flush and on merge alike. */
    public void testANumericColumnWritesNoTemporaryFile() throws IOException {
        final int numDocs = 3000;
        final FieldType type = columnarBinaryFieldType();
        final NumericBinaryPayload.Builder builder = new NumericBinaryPayload.Builder();
        try (Directory real = newDirectory()) {
            final ColumnarTestUtils.TempOutputRecorder dir = new ColumnarTestUtils.TempOutputRecorder(real);
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.LONG))
                .setMergePolicy(new LogDocMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < numDocs; d++) {
                    final Document doc = new Document();
                    final long[] values = d % 3 == 0 ? new long[] { d, d * 7L } : new long[] { randomLong() };
                    doc.add(new Field(FIELD, BytesRef.deepCopyOf(builder.encode(values, values.length)), type));
                    writer.addDocument(doc);
                    if ((d + 1) % 1000 == 0) {
                        writer.commit();
                    }
                }
                writer.forceMerge(1);
            }
            assertEquals("temporary files asked for", Set.of(), dir.columnarSuffixes);
            try (DirectoryReader reader = DirectoryReader.open(real)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.leaves().get(0).reader().getBinaryDocValues(FIELD).cost());
            }
        }
    }

    private static final String FIELD = "value";
    private static final String ID = "id";

    /**
     * Null slots are dropped on the way into the column, and a document left with no value reads back as missing the
     * field, on flush and on merge alike. Pairing such a document with multi-valued ones also checks that it doesn't
     * throw off how the column is addressed.
     */
    // TODO: update once the column keeps null slots and empty arrays.
    public void testNullSlotsAndEmptyArraysAreDropped() throws IOException {
        final int numDocs = between(10, 3000);
        final FieldType type = columnarBinaryFieldType();
        final NumericBinaryPayload.Builder builder = new NumericBinaryPayload.Builder();
        final List<List<Long>> expected = new ArrayList<>();
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.LONG))
                .setMergePolicy(new LogDocMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < numDocs; d++) {
                    builder.reset();
                    final List<Long> values = new ArrayList<>();
                    switch (d % 4) {
                        case 0 -> {
                        } // an empty array
                        case 1 -> builder.addNull();
                        case 2 -> {
                            builder.addValue(d);
                            builder.addNull();
                            builder.addValue(-d);
                            values.add((long) d);
                            values.add((long) -d);
                        }
                        case 3 -> {
                            builder.addValue(d);
                            values.add((long) d);
                        }
                        default -> throw new AssertionError();
                    }
                    expected.add(values.isEmpty() ? null : values);
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, BytesRef.deepCopyOf(builder.build()), type));
                    writer.addDocument(doc);
                    if (rarely()) {
                        writer.commit();
                    }
                }
                writer.commit();
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertColumn(reader, expected);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                assertColumn(reader, expected);
            }
        }
    }

    private static void assertColumn(DirectoryReader reader, List<List<Long>> expected) throws IOException {
        final long[][] decoded = { new long[8] };
        int base = 0;
        for (var leaf : reader.leaves()) {
            final BinaryDocValues dv = leaf.reader().getBinaryDocValues(FIELD);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                final List<Long> want = expected.get(base + doc);
                if (want == null) {
                    assertTrue("doc " + (base + doc) + " has no value", dv == null || dv.advanceExact(doc) == false);
                } else {
                    assertTrue("doc " + (base + doc) + " has a value", dv.advanceExact(doc));
                    final int count = NumericBinaryPayload.decode(dv.binaryValue(), decoded);
                    final List<Long> actual = new ArrayList<>(count);
                    for (int i = 0; i < count; i++) {
                        actual.add(decoded[0][i]);
                    }
                    assertEquals(want, actual);
                }
            }
            base += leaf.reader().maxDoc();
        }
    }

    public void testMergePreservesValues() throws IOException {
        for (int iter = 0; iter < 8; iter++) {
            final int numDocs = between(200, 5000);
            final long[] values = new long[numDocs];
            for (int d = 0; d < numDocs; d++) {
                values[d] = randomBoolean() ? between(0, 2000) : randomLong();
            }
            final boolean[] deleted = new boolean[numDocs];

            final FieldType type = columnarBinaryFieldType();

            try (Directory dir = newDirectory()) {
                // LogDocMergePolicy merges adjacent segments, so the merged order stays insertion order
                // and the ordered check below also verifies per-document association.
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.LONG))
                    .setMergePolicy(new LogDocMergePolicy());
                final NumericBinaryPayload.Builder builder = new NumericBinaryPayload.Builder();
                final int batch = Math.max(1, numDocs / between(2, 6));
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int d = 0; d < numDocs; d++) {
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                        doc.add(new Field(FIELD, BytesRef.deepCopyOf(builder.encode(new long[] { values[d] }, 1)), type));
                        writer.addDocument(doc);
                        if ((d + 1) % batch == 0) {
                            writer.commit(); // force a segment boundary so the merge has real work
                        }
                    }
                    for (int d = 0; d < numDocs; d++) {
                        if (random().nextInt(6) == 0) {
                            writer.deleteDocuments(new Term(ID, Integer.toString(d)));
                            deleted[d] = true;
                        }
                    }
                    writer.forceMerge(1);
                }

                final List<Long> expected = new ArrayList<>();
                for (int d = 0; d < numDocs; d++) {
                    if (deleted[d] == false) {
                        expected.add(values[d]);
                    }
                }

                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals("force-merged to one segment", 1, reader.leaves().size());
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
                    final long[][] decoded = { new long[8] };
                    final List<Long> actual = new ArrayList<>();
                    for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                        final int count = NumericBinaryPayload.decode(dv.binaryValue(), decoded);
                        for (int i = 0; i < count; i++) {
                            actual.add(decoded[0][i]);
                        }
                    }
                    assertEquals("merged column holds the surviving values in order", expected, actual);

                    // The merged column stays queryable.
                    final long lo = between(0, 2000);
                    final long hi = lo + between(0, 500);
                    long matches = 0;
                    for (long value : expected) {
                        if (value >= lo && value <= hi) {
                            matches++;
                        }
                    }
                    final IndexSearcher searcher = new IndexSearcher(reader);
                    assertEquals("range query after merge", matches, searcher.count(new ColumnarNumericRangeQuery(FIELD, lo, hi)));
                }
            }
        }
    }

}
