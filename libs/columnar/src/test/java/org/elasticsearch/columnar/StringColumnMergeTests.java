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
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.SlowCodecReaderWrapper;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;

/**
 * Drives string columns through the real Lucene write path — {@link IndexWriter}, several segments, deletions,
 * then a force-merge — so both the ingest path (payloads from the mapper) and the merge path (slots read in
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
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new String[] { randomFrom(terms) };
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
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new String[] { rarely() ? "rare-" + d + "-" + randomAlphaOfLength(between(1, 12)) : randomFrom(head) };
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
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                // Terms drift as documents are added, so segments flushed at different times disagree.
                int band = d / Math.max(1, numDocs / 4);
                values[d] = new String[] { "band" + band + "-" + (d % 3) };
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
            final String[][] values = new String[numDocs][];
            // The shape changes part way through, so segments flushed at different times disagree.
            final int shapes = between(2, 5);
            final int span = Math.max(1, numDocs / shapes);
            for (int d = 0; d < numDocs; d++) {
                final String value = switch ((d / span) % 4) {
                    // Nothing repeats: this stretch stays plain.
                    case 0 -> "u-" + d + "-" + randomAlphaOfLength(between(1, 10));
                    // A few terms: a dictionary that names everything.
                    case 1 -> "t" + (d % between(2, 6));
                    // A head over a tail: a dictionary that lets values escape.
                    case 2 -> rarely() ? "rare-" + d : "h" + (d % 4);
                    // Terms shared with the stretch above, so the union overlaps rather than being disjoint.
                    default -> "h" + (d % 8);
                };
                // A slot count that varies with the shape, so the merge meets multi-valued documents beside
                // single-valued ones under every layout above.
                values[d] = d % 7 == 2 ? new String[] { value, value + "-b", null } : new String[] { value };
            }
            return values;
        });
    }

    /** Every value distinct, so nothing repeats within or across segments. */
    public void testDistinctValuesRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(numDocs -> {
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new String[] { "term-" + d + "-" + randomAlphaOfLength(between(1, 20)) };
            }
            return values;
        });
    }

    /** A spread of slot counts, so the merged column carries a value-address table the sources may not have. */
    public void testMultiValuedRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(numDocs -> {
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new String[between(1, 6)];
                for (int s = 0; s < values[d].length; s++) {
                    values[d][s] = randomFrom("nginx", "apache", "", "term-" + d + "-" + s);
                }
            }
            return values;
        });
    }

    /**
     * Null slots among the values. Most documents hold none, so a segment may well have no null table at all
     * while the segment it merges with does.
     */
    public void testNullSlotsRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(numDocs -> {
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                if (rarely()) {
                    // A document dense with nulls, which still has to keep its one value.
                    values[d] = new String[] { null, null, "kept-" + d, null };
                    continue;
                }
                values[d] = new String[between(1, 5)];
                for (int s = 0; s < values[d].length; s++) {
                    if (values[d].length > 1 && randomBoolean()) {
                        continue;
                    }
                    values[d][s] = randomFrom("nginx", "", "term-" + d + "-" + s);
                }
            }
            return values;
        });
    }

    /**
     * Documents whose only slot is null, mixed with documents holding one value. The mapper writes a payload
     * for an all-null array, so this is a shape the codec really sees, and it is the one where the slots stay
     * in step with the documents while not every slot holds a value — so the column carries no addressing
     * table and its nulls are recorded by the layout alone. The empty string is among the values, which is
     * the thing a null must not be merged into.
     */
    public void testLoneNullSlotsRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(numDocs -> {
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new String[] { random().nextDouble() < 0.3 ? null : randomFrom("nginx", "", "term-" + d % 50) };
            }
            return values;
        });
    }

    /**
     * What the merged column recorded, against a count of the documents that survived. A merge of our own
     * columns takes these from what each input recorded instead of walking every cursor to re-derive them,
     * and a merge that cannot — a deleted document means a segment's totals over-state what the merge takes
     * from it — falls back to counting. Both have to arrive at the same three numbers, and the round-trip
     * assertions elsewhere would not notice if they did not: a column can hold the right slots and still
     * describe itself wrongly.
     */
    public void testMergedTotalsMatchTheDocumentsThatSurvived() throws IOException {
        for (boolean deleting : new boolean[] { false, true }) {
            // Deterministic, so the expected totals are arithmetic rather than a second implementation:
            // 300 documents, every third holding two slots, every fifth of those slots null.
            final int numDocs = 300;
            final String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = d % 3 == 0
                    ? new String[] { d % 5 == 0 ? null : "term-" + (d % 7), "second-" + (d % 4) }
                    : new String[] { d % 5 == 0 ? null : "term-" + (d % 7) };
            }
            // Every seventh document, so both a single- and a multi-slot document are removed.
            final boolean[] deleted = new boolean[numDocs];
            if (deleting) {
                for (int d = 0; d < numDocs; d += 7) {
                    deleted[d] = true;
                }
            }

            int expectedDocs = 0;
            long expectedValues = 0;
            long expectedNulls = 0;
            for (int d = 0; d < numDocs; d++) {
                if (deleted[d]) {
                    continue;
                }
                expectedDocs++;
                expectedValues += values[d].length;
                for (String slot : values[d]) {
                    if (slot == null) {
                        expectedNulls++;
                    }
                }
            }

            final FieldType type = columnarBinaryFieldType();
            try (Directory dir = newDirectory()) {
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                    .setMergePolicy(new LogDocMergePolicy());
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int d = 0; d < numDocs; d++) {
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                        doc.add(new Field(FIELD, encode(values[d]), type));
                        writer.addDocument(doc);
                        if ((d + 1) % 60 == 0) {
                            writer.commit(); // five segments, so the merge has several inputs to sum
                        }
                    }
                    for (int d = 0; d < numDocs; d++) {
                        if (deleted[d]) {
                            writer.deleteDocuments(new Term(ID, Integer.toString(d)));
                        }
                    }
                    writer.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals("force-merged to one segment", 1, reader.leaves().size());
                    final StringColumnReader column = columnOf(reader.leaves().get(0).reader());
                    final String what = deleting ? "with deletions (counted)" : "no deletions (recorded)";
                    assertEquals(what + ": numDocsWithField", expectedDocs, column.numDocsWithField());
                    assertEquals(what + ": numValues", expectedValues, column.numValues());
                    assertEquals(what + ": numNullSlots", expectedNulls, column.numNullSlots());
                }
            }
        }
    }

    /**
     * A merge whose inputs are not all our own columns. Reading a foreign segment means decoding its payloads
     * rather than its slots, and it also puts the merge on the counting pass, since there is nothing recorded
     * to sum. Both halves are otherwise unexercised: every other merge here writes all of its inputs with the
     * columnar codec.
     */
    public void testMergeWithAForeignSegment() throws IOException {
        final int numDocs = 200;
        final String[][] values = new String[numDocs][];
        for (int d = 0; d < numDocs; d++) {
            values[d] = switch (d % 4) {
                case 0 -> new String[] { "nginx", null, "kafka" };
                case 1 -> new String[] { null };
                case 2 -> new String[] { "" };
                default -> new String[] { "term-" + (d % 9) };
            };
        }
        final FieldType type = columnarBinaryFieldType();
        try (Directory foreign = newDirectory(); Directory dir = newDirectory()) {
            // Written with the columnar codec, then handed over with the column hidden, so the merge meets it
            // as a plain BinaryDocValues carrying payloads and has to decode them.
            try (IndexWriter writer = new IndexWriter(foreign, new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING)))) {
                for (int d = 0; d < numDocs / 2; d++) {
                    final Document doc = new Document();
                    doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                    doc.add(new Field(FIELD, encode(values[d]), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }

            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                .setMergePolicy(new LogDocMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = numDocs / 2; d < numDocs; d++) {
                    final Document doc = new Document();
                    doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                    doc.add(new Field(FIELD, encode(values[d]), type));
                    writer.addDocument(doc);
                }
                writer.commit();
                try (DirectoryReader source = ColumnarTestUtils.hideTheColumn(DirectoryReader.open(foreign))) {
                    final List<CodecReader> readers = new ArrayList<>();
                    for (var leaf : source.leaves()) {
                        readers.add(SlowCodecReaderWrapper.wrap(leaf.reader()));
                    }
                    writer.addIndexes(readers.toArray(new CodecReader[0]));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("force-merged to one segment", 1, reader.leaves().size());
                // addIndexes appends, so the foreign half lands after the half written here.
                final List<List<String>> expected = new ArrayList<>();
                for (int d = numDocs / 2; d < numDocs; d++) {
                    expected.add(Arrays.asList(values[d]));
                }
                for (int d = 0; d < numDocs / 2; d++) {
                    expected.add(Arrays.asList(values[d]));
                }
                assertSlots(expected, readBlobs(reader.leaves().get(0).reader()));
            }
        }
    }

    /** The merged leaf's column, so a test can read what it recorded rather than what it holds. */
    private static StringColumnReader columnOf(LeafReader leaf) throws IOException {
        final BinaryDocValues values = leaf.getBinaryDocValues(FIELD);
        assertTrue("expected a columnar column, got " + values, values instanceof ColumnarStringBinaryDocValues);
        return ((ColumnarStringBinaryDocValues) values).reader();
    }

    private interface ValueGenerator {
        String[][] generate(int numDocs);
    }

    private void assertRoundTripAndMerge(ValueGenerator generator) throws IOException {
        for (int iter = 0; iter < 4; iter++) {
            final int numDocs = between(200, 3000);
            final String[][] values = generator.generate(numDocs);
            final boolean[] deleted = new boolean[numDocs];
            // Some runs delete nothing, so the merge also meets segments that give up every document they
            // hold — which is what lets it take their totals from what they recorded instead of counting.
            final boolean deleting = randomBoolean();
            final FieldType type = columnarBinaryFieldType();

            try (Directory dir = newDirectory()) {
                // LogDocMergePolicy merges adjacent segments, so the merged order stays insertion order and the
                // ordered check below also verifies per-document association.
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec(ColumnarFieldType.STRING))
                    .setMergePolicy(new LogDocMergePolicy());
                final int batch = Math.max(1, numDocs / between(2, 6));
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int d = 0; d < numDocs; d++) {
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                        doc.add(new Field(FIELD, encode(values[d]), type));
                        writer.addDocument(doc);
                        if ((d + 1) % batch == 0) {
                            writer.commit(); // force a segment boundary so the merge has real work
                        }
                    }

                    // Read the values back before merging, so the per-segment ingest path is checked too.
                    try (DirectoryReader reader = DirectoryReader.open(writer)) {
                        final List<BytesRef> blobs = new ArrayList<>();
                        for (var leaf : reader.leaves()) {
                            blobs.addAll(readBlobs(leaf.reader()));
                        }
                        assertSlots(expected(values, new boolean[numDocs]), blobs);
                    }

                    if (deleting) {
                        for (int d = 0; d < numDocs; d++) {
                            if (random().nextInt(6) == 0) {
                                writer.deleteDocuments(new Term(ID, Integer.toString(d)));
                                deleted[d] = true;
                            }
                        }
                    }
                    writer.forceMerge(1);
                }

                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals("force-merged to one segment", 1, reader.leaves().size());
                    assertSlots(expected(values, deleted), readBlobs(reader.leaves().get(0).reader()));
                }
            }
        }
    }

    private static List<List<String>> expected(String[][] values, boolean[] deleted) {
        final List<List<String>> expected = new ArrayList<>();
        for (int d = 0; d < values.length; d++) {
            if (deleted[d] == false) {
                expected.add(Arrays.asList(values[d]));
            }
        }
        return expected;
    }

    /** Every document's blob, in doc order. Decoding needs a slot count, which the caller supplies. */
    private static List<BytesRef> readBlobs(LeafReader leaf) throws IOException {
        final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
        final List<BytesRef> blobs = new ArrayList<>();
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
            blobs.add(BytesRef.deepCopyOf(dv.binaryValue()));
        }
        return blobs;
    }

    /** Decodes each blob back into the slots it was built from. */
    private static void assertSlots(List<List<String>> expected, List<BytesRef> blobs) throws IOException {
        assertEquals("documents with a value", expected.size(), blobs.size());
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        for (int i = 0; i < expected.size(); i++) {
            final List<String> slots = new ArrayList<>();
            for (int slot = decoder.reset(blobs.get(i)); slot > 0; slot--) {
                final BytesRef value = decoder.next();
                slots.add(value == null ? null : value.utf8ToString());
            }
            assertEquals("document " + i, expected.get(i), slots);
        }
    }

    /** Through a builder of its own, deep-copied, so the payload owns its bytes once handed to the document. */
    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }
}
