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
import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarStringFieldType;

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
        assertRoundTripAndMerge(StringBinaryPayload.Framing.SEPARATE_COUNT, numDocs -> {
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new String[] { randomFrom(terms) };
            }
            return values;
        });
    }

    /** Every value distinct, so nothing repeats within or across segments. */
    public void testDistinctValuesRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(StringBinaryPayload.Framing.SEPARATE_COUNT, numDocs -> {
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new String[] { "term-" + d + "-" + randomAlphaOfLength(between(1, 20)) };
            }
            return values;
        });
    }

    /** A spread of slot counts, so the merged column carries a value-address table the sources may not have. */
    public void testMultiValuedRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(StringBinaryPayload.Framing.SEPARATE_COUNT, numDocs -> {
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
     * Null slots among the values, which only the array-order framing can carry. Most documents hold none, so
     * a segment may well have no null table at all while the segment it merges with does.
     */
    public void testNullSlotsRoundTripAndMerge() throws IOException {
        assertRoundTripAndMerge(StringBinaryPayload.Framing.ARRAY_ORDER, numDocs -> {
            String[][] values = new String[numDocs][];
            for (int d = 0; d < numDocs; d++) {
                if (rarely()) {
                    // A document dense with nulls, which still has to keep its one value.
                    values[d] = new String[] { null, null, "kept-" + d, null };
                    continue;
                }
                values[d] = new String[between(1, 5)];
                int nonNull = 0;
                for (int s = 0; s < values[d].length; s++) {
                    if (values[d].length > 1 && randomBoolean()) {
                        continue;
                    }
                    values[d][s] = randomFrom("nginx", "", "term-" + d + "-" + s);
                    nonNull++;
                }
                if (nonNull == 0) {
                    values[d][0] = "kept-" + d;
                }
            }
            return values;
        });
    }

    private interface ValueGenerator {
        String[][] generate(int numDocs);
    }

    private void assertRoundTripAndMerge(StringBinaryPayload.Framing framing, ValueGenerator generator) throws IOException {
        for (int iter = 0; iter < 4; iter++) {
            final int numDocs = between(200, 3000);
            final String[][] values = generator.generate(numDocs);
            final boolean[] deleted = new boolean[numDocs];
            final FieldType type = columnarStringFieldType(framing);

            try (Directory dir = newDirectory()) {
                // LogDocMergePolicy merges adjacent segments, so the merged order stays insertion order and the
                // ordered check below also verifies per-document association.
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodec()).setMergePolicy(new LogDocMergePolicy());
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
                        assertSlots(expected(values, new boolean[numDocs]), blobs, framing);
                    }

                    for (int d = 0; d < numDocs; d++) {
                        if (random().nextInt(6) == 0) {
                            writer.deleteDocuments(new Term(ID, Integer.toString(d)));
                            deleted[d] = true;
                        }
                    }
                    writer.forceMerge(1);
                }

                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals("force-merged to one segment", 1, reader.leaves().size());
                    assertSlots(expected(values, deleted), readBlobs(reader.leaves().get(0).reader()), framing);
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

    /**
     * Decodes each blob against the slot count the document was written with — standing in for the
     * {@code .counts} companion the mapper writes, which is what a real reader consults.
     */
    private static void assertSlots(List<List<String>> expected, List<BytesRef> blobs, StringBinaryPayload.Framing framing)
        throws IOException {
        assertEquals("documents with a value", expected.size(), blobs.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("document " + i, expected.get(i), decodeLegacy(blobs.get(i), expected.get(i).size(), framing));
        }
    }

    /**
     * The framing readers expect, decoded longhand: a lone slot is the whole blob, otherwise a length per slot
     * biased by one where the framing can carry a null.
     */
    private static List<String> decodeLegacy(BytesRef blob, int slotCount, StringBinaryPayload.Framing framing) throws IOException {
        if (slotCount == 1) {
            return List.of(blob.utf8ToString());
        }
        final int bias = framing == StringBinaryPayload.Framing.ARRAY_ORDER ? 1 : 0;
        final ByteArrayDataInput in = new ByteArrayDataInput(blob.bytes, blob.offset, blob.length);
        final List<String> slots = new ArrayList<>(slotCount);
        for (int i = 0; i < slotCount; i++) {
            final int encodedLength = in.readVInt();
            if (encodedLength == 0 && bias == 1) {
                slots.add(null);
                continue;
            }
            final int length = encodedLength - bias;
            final BytesRef slot = new BytesRef(blob.bytes, in.getPosition(), length);
            in.setPosition(in.getPosition() + length);
            slots.add(slot.utf8ToString());
        }
        assertTrue("blob fully consumed", in.eof());
        return slots;
    }

    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return StringBinaryPayload.encode(refs);
    }
}
