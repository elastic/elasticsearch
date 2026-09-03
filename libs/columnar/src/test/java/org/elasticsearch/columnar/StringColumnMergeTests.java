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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * Drives string columns through the real Lucene write path — {@link IndexWriter}, several segments, deletions,
 * then a force-merge — so both the ingest path (payloads from the mapper) and the merge path (values read in
 * bulk off a source segment via the column's own reader) are exercised end to end at the {@code BINARY}
 * surface. {@code StringColumnTests} covers the writer and reader directly against a {@code Directory}; this is
 * the same column driven by Lucene, and the counterpart of {@link NumericColumnMergeTests}.
 *
 * <p>Run over repeated and over all-distinct values: the two compress very differently, and a merge unions
 * whatever the flushed segments held.
 */
public class StringColumnMergeTests extends ESTestCase {

    private static final String FIELD = "keyword";
    private static final String ID = "id";

    /** A handful of terms repeated across every segment, including the empty value. */
    public void testRepeatedValuesRoundTripAndMerge() throws IOException {
        String[] terms = { "nginx", "apache", "kafka", "elasticsearch", "" };
        assertRoundTripAndMerge(numDocs -> {
            String[] values = new String[numDocs];
            for (int d = 0; d < numDocs; d++) {
                values[d] = randomFrom(terms);
            }
            return values;
        });
    }

    /**
     * A head of repeated terms over a tail seen once each, so every segment carries a dictionary that let
     * values escape. The union of those dictionaries cannot stand for the merged column — it would not name
     * the escaped values — so the merge surveys instead of carrying ordinals over.
     */
    public void testDictionaryWithEscapesRoundTripsAndMerges() throws IOException {
        String[] head = { "GET", "POST", "PUT", "DELETE" };
        assertRoundTripAndMerge(numDocs -> {
            String[] values = new String[numDocs];
            for (int d = 0; d < numDocs; d++) {
                values[d] = rarely() ? "rare-" + d + "-" + randomAlphaOfLength(between(1, 12)) : randomFrom(head);
            }
            return values;
        });
    }

    /**
     * Terms that differ between segments, so the merged vocabulary is a union of dictionaries none of which
     * holds it all, and each segment's ordinals mean something different in the merged column.
     */
    public void testDisjointDictionariesMerge() throws IOException {
        assertRoundTripAndMerge(numDocs -> {
            String[] values = new String[numDocs];
            for (int d = 0; d < numDocs; d++) {
                // Terms drift as documents are added, so segments flushed at different times disagree.
                int band = d / Math.max(1, numDocs / 4);
                values[d] = "band" + band + "-" + (d % 3);
            }
            return values;
        });
    }

    /**
     * Segments of shapes chosen at random, so the merge meets combinations nobody named: a dictionary
     * segment beside a plain one, segments whose dictionaries overlap partly or not at all, and segments
     * that escaped different amounts. Every one of those picks a different path through the merge.
     */
    public void testRandomShapesMerge() throws IOException {
        assertRoundTripAndMerge(numDocs -> {
            final String[] values = new String[numDocs];
            // The shape changes part way through, so segments flushed at different times disagree.
            final int shapes = between(2, 5);
            final int span = Math.max(1, numDocs / shapes);
            for (int d = 0; d < numDocs; d++) {
                values[d] = switch ((d / span) % 4) {
                    // Nothing repeats: this stretch stays plain.
                    case 0 -> "u-" + d + "-" + randomAlphaOfLength(between(1, 10));
                    // A few terms: a dictionary that names everything.
                    case 1 -> "t" + (d % between(2, 6));
                    // A head over a tail: a dictionary that lets values escape.
                    case 2 -> rarely() ? "rare-" + d : "h" + (d % 4);
                    // Terms shared with the stretch above, so the union overlaps rather than being disjoint.
                    default -> "h" + (d % 8);
                };
            }
            return values;
        });
    }

    /** Every value distinct, so nothing repeats within or across segments. */
    public void testDistinctValuesRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(numDocs -> {
            String[] values = new String[numDocs];
            for (int d = 0; d < numDocs; d++) {
                values[d] = "term-" + d + "-" + randomAlphaOfLength(between(1, 20));
            }
            return values;
        });
    }

    private interface ValueGenerator {
        String[] generate(int numDocs);
    }

    private void assertRoundTripAndMerge(ValueGenerator generator) throws IOException {
        for (int iter = 0; iter < 4; iter++) {
            final int numDocs = between(200, 3000);
            final String[] values = generator.generate(numDocs);
            final boolean[] deleted = new boolean[numDocs];
            final FieldType type = stringFieldType();

            try (Directory dir = newDirectory()) {
                // LogDocMergePolicy merges adjacent segments, so the merged order stays insertion order and the
                // ordered check below also verifies per-document association.
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
                final BytesRefBuilder builder = new BytesRefBuilder();
                final int batch = Math.max(1, numDocs / between(2, 6));
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int d = 0; d < numDocs; d++) {
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                        doc.add(new Field(FIELD, BytesRef.deepCopyOf(encode(values[d], builder)), type));
                        writer.addDocument(doc);
                        if ((d + 1) % batch == 0) {
                            writer.commit(); // force a segment boundary so the merge has real work
                        }
                    }

                    // Read the values back before merging, so the per-segment ingest path is checked too.
                    try (DirectoryReader reader = DirectoryReader.open(writer)) {
                        final List<String> actual = new ArrayList<>();
                        for (var leaf : reader.leaves()) {
                            actual.addAll(readValues(leaf.reader()));
                        }
                        assertEquals("values before merge", List.of(values), actual);
                    }

                    for (int d = 0; d < numDocs; d++) {
                        if (random().nextInt(6) == 0) {
                            writer.deleteDocuments(new Term(ID, Integer.toString(d)));
                            deleted[d] = true;
                        }
                    }
                    writer.forceMerge(1);
                }

                final List<String> expected = new ArrayList<>();
                for (int d = 0; d < numDocs; d++) {
                    if (deleted[d] == false) {
                        expected.add(values[d]);
                    }
                }

                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals("force-merged to one segment", 1, reader.leaves().size());
                    assertEquals(
                        "merged column holds the surviving values in order",
                        expected,
                        readValues(reader.leaves().get(0).reader())
                    );
                }
            }
        }
    }

    /** Every document's value, in doc order, as the column hands it back. */
    private static List<String> readValues(LeafReader leaf) throws IOException {
        final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
        final List<String> actual = new ArrayList<>();
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
            actual.add(dv.binaryValue().utf8ToString());
        }
        return actual;
    }

    private static BytesRef encode(String value, BytesRefBuilder builder) {
        builder.copyChars(value);
        return builder.get();
    }

    private static FieldType stringFieldType() {
        return columnarBinaryFieldType(ColumnarFieldType.STRING);
    }
}
