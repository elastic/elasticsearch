/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnValues;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodec;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A payload column through Lucene: values from empty to several chunks long, nulls and several slots a document, flushed
 * into segments and force-merged with and without deletions and under an index sort.
 */
public class PayloadColumnMergeTests extends ESTestCase {

    private static final String FIELD = "payload";
    private static final String ID = "id";
    private static final String SORT = "sort";

    public void testRoundTripAndMerge() throws IOException {
        for (int iter = 0; iter < 3; iter++) {
            final StringColumnOptions options = randomOptions();
            final int numDocs = between(50, 300);
            final byte[][][] values = new byte[numDocs][][];
            for (int d = 0; d < numDocs; d++) {
                values[d] = new byte[between(1, 3)][];
                for (int s = 0; s < values[d].length; s++) {
                    values[d][s] = randomValue(options);
                }
            }
            final boolean[] deleted = new boolean[numDocs];
            final boolean deleting = randomBoolean();
            final FieldType type = columnarBinaryFieldType();
            try (Directory dir = newDirectory()) {
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(payloadCodec(options))
                    .setMergePolicy(new LogDocMergePolicy());
                final int batch = Math.max(1, numDocs / between(2, 5));
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    for (int d = 0; d < numDocs; d++) {
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(d), Field.Store.NO));
                        doc.add(new Field(FIELD, encode(values[d]), type));
                        writer.addDocument(doc);
                        if ((d + 1) % batch == 0) {
                            writer.commit();
                        }
                    }
                    try (DirectoryReader reader = DirectoryReader.open(writer)) {
                        final List<List<byte[]>> read = new ArrayList<>();
                        for (var leaf : reader.leaves()) {
                            read.addAll(readSlots(leaf.reader()));
                        }
                        assertSlots(expected(values, new boolean[numDocs]), read);
                    }
                    if (deleting) {
                        for (int d = 0; d < numDocs; d++) {
                            if (random().nextInt(5) == 0) {
                                writer.deleteDocuments(new Term(ID, Integer.toString(d)));
                                deleted[d] = true;
                            }
                        }
                    }
                    writer.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    assertEquals(1, reader.leaves().size());
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    assertSlots(expected(values, deleted), readSlots(leaf));
                    assertTransferredInChunks(leaf, expected(values, deleted), options);
                }
            }
        }
    }

    /**
     * The merge carries over the repeats each input recorded instead of comparing values: a repeat within a segment
     * survives unless the value it repeats was deleted, and an equal value across segments is stored again.
     */
    public void testMergeCarriesRepeatsOverWithoutComparing() throws IOException {
        final StringColumnOptions options = randomOptions();
        final byte[] a = randomByteArrayOfLength(between(1, 64));
        byte[] other;
        do {
            other = randomByteArrayOfLength(between(1, 64));
        } while (other.length == a.length);
        final byte[] b = other;
        // Two segments; neither is in order, and the first value of the second repeats the last of the first.
        final byte[][] first = { b, a, a, a };
        final byte[][] second = { a, a, b };
        final FieldType type = columnarBinaryFieldType();
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(payloadCodec(options)).setMergePolicy(new LogDocMergePolicy());
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                int id = 0;
                for (byte[][] segment : new byte[][][] { first, second }) {
                    for (byte[] value : segment) {
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(id++), Field.Store.NO));
                        doc.add(new Field(FIELD, encode(new byte[][] { value }), type));
                        writer.addDocument(doc);
                    }
                    writer.commit();
                }
                // The second "a" of the first segment, so the third repeats a value that is gone.
                writer.deleteDocuments(new Term(ID, "2"));
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final StringColumnReader column = columnOf(leaf);
                assertFalse(column.hasDictionary());
                assertFalse(column.valuesSorted());
                // Surviving: b a a | a a b. Only the second "a" of the second segment is carried over as a repeat.
                assertSlots(List.of(List.of(b), List.of(a), List.of(a), List.of(a), List.of(a), List.of(b)), readSlots(leaf));
                final StringColumnValues cursor = ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).directValues();
                final boolean[] repeats = new boolean[6];
                int i = 0;
                for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc()) {
                    cursor.nextValue();
                    repeats[i++] = cursor.repeatsPrevious();
                }
                assertArrayEquals(new boolean[] { false, false, false, false, true, false }, repeats);
            }
        }
    }

    /**
     * Under an index sort the merge interleaves its inputs, so an input's recorded repeat may not follow the value it
     * repeats. Values repeat in runs along the key, and each run is split across segments.
     */
    public void testIndexSortedMergeRoundTrips() throws IOException {
        for (int iter = 0; iter < 3; iter++) {
            final StringColumnOptions options = randomOptions();
            final int numDocs = between(100, 600);
            final List<Integer> keys = new ArrayList<>();
            for (int k = 0; k < numDocs; k++) {
                keys.add(k);
            }
            Collections.shuffle(keys, random());
            final byte[][] runs = new byte[numDocs / 4 + 1][];
            for (int r = 0; r < runs.length; r++) {
                runs[r] = randomValue(options);
            }
            // One slot a document, the value fixed by the key's run, so documents next to each other in key order repeat.
            final byte[][][] byKey = new byte[numDocs][][];
            for (int k = 0; k < numDocs; k++) {
                byKey[k] = rarely() ? new byte[][] { runs[k / 4], randomValue(options) } : new byte[][] { runs[k / 4] };
            }
            final boolean[] deletedKey = new boolean[numDocs];
            final FieldType type = columnarBinaryFieldType();
            try (Directory dir = newDirectory()) {
                final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(sortedPayloadCodec(options))
                    .setIndexSort(new Sort(new SortField(SORT, SortField.Type.INT)))
                    .setMergePolicy(new LogDocMergePolicy());
                try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                    final int batch = Math.max(1, numDocs / between(2, 5));
                    for (int d = 0; d < numDocs; d++) {
                        final int key = keys.get(d);
                        final Document doc = new Document();
                        doc.add(new StringField(ID, Integer.toString(key), Field.Store.NO));
                        doc.add(new NumericDocValuesField(SORT, key));
                        doc.add(new Field(FIELD, encode(byKey[key]), type));
                        writer.addDocument(doc);
                        if ((d + 1) % batch == 0) {
                            writer.commit();
                        }
                    }
                    if (randomBoolean()) {
                        for (int k = 0; k < numDocs; k++) {
                            if (random().nextInt(5) == 0) {
                                writer.deleteDocuments(new Term(ID, Integer.toString(k)));
                                deletedKey[k] = true;
                            }
                        }
                    }
                    writer.forceMerge(1);
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    final LeafReader leaf = reader.leaves().get(0).reader();
                    final List<List<byte[]>> expected = expected(byKey, deletedKey);
                    assertSlots(expected, readSlots(leaf));
                    assertTransferredInChunks(leaf, expected, options);
                }
            }
        }
    }

    /**
     * A document's payload over a megabyte is built in a buffer of its own, so the next document is not rebuilt in one
     * that size. Its slots of every shape still come back in order.
     */
    public void testALargePayloadIsNotKept() throws IOException {
        final byte[] large = randomByteArrayOfLength((1 << 20) + between(1, 100_000));
        final byte[] small = randomByteArrayOfLength(between(1, 100));
        final byte[][] first = { large, null, small, new byte[0] };
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(payloadCodec(randomOptions())))) {
                for (byte[][] slots : List.of(first, new byte[][] { small })) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, encode(slots), columnarBinaryFieldType()));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final BinaryDocValues dv = reader.leaves().get(0).reader().getBinaryDocValues(FIELD);
                assertEquals(0, dv.nextDoc());
                final BytesRef firstPayload = dv.binaryValue();
                assertSlots(List.of(Arrays.asList(first)), List.of(decode(firstPayload)));
                final byte[] firstBuffer = firstPayload.bytes;
                assertEquals(1, dv.nextDoc());
                final BytesRef second = dv.binaryValue();
                assertNotSame(firstBuffer, second.bytes);
                assertThat(second.bytes.length, lessThan(1 << 20));
                assertSlots(List.of(List.of(small)), List.of(decode(second)));
            }
        }
    }

    private static List<byte[]> decode(BytesRef payload) throws IOException {
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        final List<byte[]> slots = new ArrayList<>();
        for (int slot = decoder.reset(payload); slot > 0; slot--) {
            final BytesRef value = decoder.next();
            slots.add(value == null ? null : BytesRef.deepCopyOf(value).bytes);
        }
        return slots;
    }

    /** Every value, handed over by the column's reader, arrives in pieces no larger than a chunk and reassembles to itself. */
    private static void assertTransferredInChunks(LeafReader leaf, List<List<byte[]>> expected, StringColumnOptions options)
        throws IOException {
        final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
        final StringColumnValues cursor = ((ColumnarStringBinaryDocValues) dv).directValues();
        final int maxPiece = options.sizes().plainChunks().targetBytes();
        int i = 0;
        for (int doc = cursor.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = cursor.nextDoc(), i++) {
            final List<byte[]> slots = expected.get(i);
            assertEquals(slots.size(), cursor.valueCount());
            for (byte[] slot : slots) {
                cursor.nextValue();
                if (slot == null) {
                    assertEquals(-1, cursor.valueLength());
                    continue;
                }
                assertEquals(slot.length, cursor.valueLength());
                final ByteArrayOutputStream out = new ByteArrayOutputStream();
                cursor.transferValue((bytes, offset, length) -> {
                    assertThat(length, lessThanOrEqualTo(maxPiece));
                    out.write(bytes, offset, length);
                });
                assertArrayEquals(slot, out.toByteArray());
            }
        }
        assertEquals(expected.size(), i);
    }

    private static StringColumnOptions randomOptions() {
        final StringColumnOptions options = StringColumnOptions.PAYLOADS;
        if (randomBoolean()) {
            return options;
        }
        return new StringColumnOptions(
            options.dictionary(),
            options.summary(),
            randomFrom(ChunkCodec.values()),
            options.sizes(),
            options.singleValued(),
            options.enableSkipIndex()
        );
    }

    /** Mostly short, sometimes several chunks long, sometimes empty or null; some compress and some do not. */
    private static byte[] randomValue(StringColumnOptions options) {
        if (rarely()) {
            return null;
        }
        final int chunk = options.sizes().plainChunks().targetBytes();
        final int length = switch (between(0, 9)) {
            case 0 -> 0;
            case 1 -> between(chunk / 2, chunk * 5);
            default -> between(1, 512);
        };
        final byte[] value = new byte[length];
        if (randomBoolean()) {
            random().nextBytes(value);
        } else {
            Arrays.fill(value, (byte) between(0, 3));
        }
        return value;
    }

    private static org.apache.lucene.codecs.Codec payloadCodec(StringColumnOptions options) {
        return columnarCodec(
            new ColumNARDocValuesFormat(
                (fieldName, type) -> NumericPipeline::defaultPipeline,
                field -> ColumnarFieldType.STRING,
                ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
                StringColumnOptionsSelector.always(options)
            )
        );
    }

    /** The payload format for the field under test, and the default for the sort key. */
    private static org.apache.lucene.codecs.Codec sortedPayloadCodec(StringColumnOptions options) {
        final org.apache.lucene.codecs.DocValuesFormat payloads = new ColumNARDocValuesFormat(
            (fieldName, type) -> NumericPipeline::defaultPipeline,
            field -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(options)
        );
        final org.apache.lucene.codecs.DocValuesFormat fallback = new Lucene90DocValuesFormat();
        final org.apache.lucene.codecs.Codec base = TestUtil.getDefaultCodec();
        return new FilterCodec(base.getName(), base) {
            private final org.apache.lucene.codecs.DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public org.apache.lucene.codecs.DocValuesFormat getDocValuesFormatForField(String name) {
                    return name.equals(FIELD) ? payloads : fallback;
                }
            };

            @Override
            public org.apache.lucene.codecs.DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }

    private static StringColumnReader columnOf(LeafReader leaf) throws IOException {
        return ((ColumnarStringBinaryDocValues) leaf.getBinaryDocValues(FIELD)).reader();
    }

    private static List<List<byte[]>> expected(byte[][][] values, boolean[] deleted) {
        final List<List<byte[]>> expected = new ArrayList<>();
        for (int d = 0; d < values.length; d++) {
            if (deleted[d] == false) {
                expected.add(Arrays.asList(values[d]));
            }
        }
        return expected;
    }

    private static List<List<byte[]>> readSlots(LeafReader leaf) throws IOException {
        final BinaryDocValues dv = leaf.getBinaryDocValues(FIELD);
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        final List<List<byte[]>> docs = new ArrayList<>();
        for (int doc = dv.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
            final List<byte[]> slots = new ArrayList<>();
            for (int slot = decoder.reset(dv.binaryValue()); slot > 0; slot--) {
                final BytesRef value = decoder.next();
                slots.add(value == null ? null : BytesRef.deepCopyOf(value).bytes);
            }
            docs.add(slots);
        }
        return docs;
    }

    private static void assertSlots(List<List<byte[]>> expected, List<List<byte[]>> actual) {
        assertEquals("documents", expected.size(), actual.size());
        for (int i = 0; i < expected.size(); i++) {
            assertEquals("slots of document " + i, expected.get(i).size(), actual.get(i).size());
            for (int s = 0; s < expected.get(i).size(); s++) {
                assertArrayEquals("document " + i + " slot " + s, expected.get(i).get(s), actual.get(i).get(s));
            }
        }
    }

    private static BytesRef encode(byte[][] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (byte[] slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }
}
