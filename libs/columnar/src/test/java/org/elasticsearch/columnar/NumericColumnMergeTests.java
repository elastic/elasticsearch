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
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.columnar.numeric.NumericBinaryPayload;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * Exercises the merge fast path: values are written across several segments (with deletions), then
 * force-merged. The merge re-runs the encoder pipeline over the source segments, reading their values
 * in bulk off disk. The merged column must hold exactly the surviving values, in doc order, and stay
 * queryable.
 */
public class NumericColumnMergeTests extends ESTestCase {

    private static final String FIELD = "value";
    private static final String ID = "id";

    public void testMergePreservesValues() throws IOException {
        for (int iter = 0; iter < 8; iter++) {
            final int numDocs = between(200, 5000);
            final long[] values = new long[numDocs];
            for (int d = 0; d < numDocs; d++) {
                values[d] = randomBoolean() ? between(0, 2000) : randomLong();
            }
            final boolean[] deleted = new boolean[numDocs];

            final FieldType type = columnarBinaryFieldType(ColumnarFieldType.LONG);

            try (Directory dir = newDirectory()) {
                // LogDocMergePolicy merges adjacent segments, so the merged order stays insertion order
                // and the ordered check below also verifies per-document association.
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
                final BytesRefBuilder builder = new BytesRefBuilder();
                final int batch = Math.max(1, numDocs / between(2, 6));
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int d = 0; d < numDocs; d++) {
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                        doc.add(
                            new Field(FIELD, BytesRef.deepCopyOf(NumericBinaryPayload.encode(new long[] { values[d] }, 1, builder)), type)
                        );
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
