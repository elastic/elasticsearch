/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.tests.index.ForceMergePolicy;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Correctness duel test for the columnar flattened binary doc values writer. Indexes the same
 * {@link MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull}-framed blobs to two segments,
 * one written with the row block writer ({@code writeColumnarFlattenedBinary=false}) and one with
 * the columnar writer ({@code writeColumnarFlattenedBinary=true}). After force-merging both to a
 * single segment, the test asserts that every binary doc value is byte-identical across the two.
 *
 * <p>This is the critical invariant that protects merge correctness: the columnar block writer
 * transposes key runs into contiguous storage, and {@code transposeBlock} in the producer must
 * reconstruct exactly the same sorted {@code KeyedArrayOrderInlineNull} blob that the row writer
 * returns verbatim. The test generates blobs with keys in sorted order so that the columnar
 * reconstruction (also sorted) matches byte-for-byte.
 *
 * <p>The test also covers null-slot handling: blobs may contain null slots
 * ({@code [0 vint][key bytes][0x00 separator]}) which the columnar writer must preserve through
 * the transpose round-trip.
 */
public class FlattenedColumnarBinaryDuelTests extends ESTestCase {

    private static final String FIELD = "labels._keyed";

    /**
     * Field type carrying the {@code es.FlattenedKeyedBinaryDocValues} attribute. This triggers
     * {@code AbstractTSDBDocValuesConsumer.isFlattenedKeyedColumnar} when the format also has
     * {@code writeColumnarFlattenedBinary=true}. The attribute is harmless for the row arm
     * ({@code writeColumnarFlattenedBinary=false}) because {@code isFlattenedKeyedColumnar} returns
     * {@code false} immediately on that path.
     */
    private static final FieldType FIELD_TYPE_WITH_ATTR = buildFieldType();

    private static FieldType buildFieldType() {
        FieldType t = new FieldType();
        t.setDocValuesType(DocValuesType.BINARY);
        t.putAttribute(
            MultiValuedBinaryDocValuesField.FLATTENED_KEYED_BDV_ATTRIBUTE_KEY,
            MultiValuedBinaryDocValuesField.FLATTENED_KEYED_BDV_ATTRIBUTE_VALUE
        );
        t.freeze();
        return t;
    }

    public void testColumnarMatchesRow() throws IOException {
        int numDocs = randomIntBetween(512, 8192);
        int keyPoolSize = randomIntBetween(3, 50);
        int keysPerDocMax = Math.min(keyPoolSize, 20);

        // Plain ASCII key pool so keys never contain 0x00 (the separator).
        byte[][] keyPool = new byte[keyPoolSize][];
        for (int k = 0; k < keyPoolSize; k++) {
            keyPool[k] = ("key" + k).getBytes(StandardCharsets.UTF_8);
        }

        // Pre-generate all documents so both indexes receive byte-identical blobs.
        List<BytesRef> blobs = new ArrayList<>(numDocs);
        for (int d = 0; d < numDocs; d++) {
            if (rarely()) {
                // Sparse doc: omit the binary field entirely to test document gaps.
                blobs.add(null);
            } else {
                blobs.add(randomBlob(keyPool, keysPerDocMax));
            }
        }

        var rowFormat = new ES819Version3TSDBDocValuesFormat(false, false, false, false);
        var columnarFormat = new ES819Version3TSDBDocValuesFormat(false, false, false, true);

        try (var rowDir = newDirectory(); var columnarDir = newDirectory()) {
            var mergePolicy = new ForceMergePolicy(newLogMergePolicy());

            var rowConfig = newIndexWriterConfig();
            rowConfig.setMergePolicy(mergePolicy);
            rowConfig.setCodec(TestUtil.alwaysDocValuesFormat(rowFormat));

            var columnarConfig = newIndexWriterConfig();
            columnarConfig.setMergePolicy(mergePolicy);
            columnarConfig.setCodec(TestUtil.alwaysDocValuesFormat(columnarFormat));

            try (
                var rowWriter = new RandomIndexWriter(random(), rowDir, rowConfig);
                var columnarWriter = new RandomIndexWriter(random(), columnarDir, columnarConfig)
            ) {
                for (BytesRef blob : blobs) {
                    Document doc = new Document();
                    if (blob != null) {
                        doc.add(new Field(FIELD, blob, FIELD_TYPE_WITH_ATTR));
                    }
                    rowWriter.addDocument(doc);
                    columnarWriter.addDocument(doc);
                }
                rowWriter.forceMerge(1);
                columnarWriter.forceMerge(1);
            }

            try (var rowIr = DirectoryReader.open(rowDir); var columnarIr = DirectoryReader.open(columnarDir)) {
                assertEquals(1, rowIr.leaves().size());
                assertEquals(1, columnarIr.leaves().size());

                LeafReader rowLeaf = rowIr.leaves().getFirst().reader();
                LeafReader columnarLeaf = columnarIr.leaves().getFirst().reader();
                assertEquals(rowLeaf.maxDoc(), columnarLeaf.maxDoc());

                assertBinaryDocValuesMatch(rowLeaf, columnarLeaf);
            }
        }
    }

    /**
     * Duel between the row writer and the sub-chunked columnar writer. The sub-chunked format
     * individually compresses each key's value run; {@code transposeSubchunkedBlock} must
     * reconstruct byte-identical blobs to the plain row writer.
     */
    public void testSubchunkedMatchesRow() throws IOException {
        int numDocs = randomIntBetween(512, 8192);
        int keyPoolSize = randomIntBetween(3, 50);
        int keysPerDocMax = Math.min(keyPoolSize, 20);

        byte[][] keyPool = new byte[keyPoolSize][];
        for (int k = 0; k < keyPoolSize; k++) {
            keyPool[k] = ("key" + k).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }

        List<BytesRef> blobs = new ArrayList<>(numDocs);
        for (int d = 0; d < numDocs; d++) {
            if (rarely()) {
                blobs.add(null);
            } else {
                blobs.add(randomBlob(keyPool, keysPerDocMax));
            }
        }

        // Row format (writeColumnarFlattenedBinary=false, writeSubchunkedFlattenedBinary=false)
        var rowFormat = new ES819Version3TSDBDocValuesFormat(false, false, false, false, false);
        // Sub-chunked format (writeColumnarFlattenedBinary=true, writeSubchunkedFlattenedBinary=true)
        var subchunkedFormat = new ES819Version3TSDBDocValuesFormat(false, false, false, true, true);

        try (var rowDir = newDirectory(); var subchunkedDir = newDirectory()) {
            var mergePolicy = new org.apache.lucene.tests.index.ForceMergePolicy(newLogMergePolicy());

            var rowConfig = newIndexWriterConfig();
            rowConfig.setMergePolicy(mergePolicy);
            rowConfig.setCodec(org.apache.lucene.tests.util.TestUtil.alwaysDocValuesFormat(rowFormat));

            var subchunkedConfig = newIndexWriterConfig();
            subchunkedConfig.setMergePolicy(mergePolicy);
            subchunkedConfig.setCodec(org.apache.lucene.tests.util.TestUtil.alwaysDocValuesFormat(subchunkedFormat));

            try (
                var rowWriter = new org.apache.lucene.tests.index.RandomIndexWriter(random(), rowDir, rowConfig);
                var subchunkedWriter = new org.apache.lucene.tests.index.RandomIndexWriter(random(), subchunkedDir, subchunkedConfig)
            ) {
                for (BytesRef blob : blobs) {
                    Document doc = new Document();
                    if (blob != null) {
                        doc.add(new Field(FIELD, blob, FIELD_TYPE_WITH_ATTR));
                    }
                    rowWriter.addDocument(doc);
                    subchunkedWriter.addDocument(doc);
                }
                rowWriter.forceMerge(1);
                subchunkedWriter.forceMerge(1);
            }

            try (var rowIr = DirectoryReader.open(rowDir); var subchunkedIr = DirectoryReader.open(subchunkedDir)) {
                assertEquals(1, rowIr.leaves().size());
                assertEquals(1, subchunkedIr.leaves().size());

                LeafReader rowLeaf = rowIr.leaves().getFirst().reader();
                LeafReader subchunkedLeaf = subchunkedIr.leaves().getFirst().reader();
                assertEquals(rowLeaf.maxDoc(), subchunkedLeaf.maxDoc());

                assertBinaryDocValuesMatch(rowLeaf, subchunkedLeaf);
            }
        }
    }

    /**
     * Exercises a single block boundary crossing: more docs than the default block threshold
     * ({@code 8096}) to ensure the columnar writer/reader handles multi-block segments correctly.
     */
    public void testMultiBlock() throws IOException {
        int numDocs = 12000; // deliberately > 8096-doc block threshold
        int keyPoolSize = 15;
        byte[][] keyPool = new byte[keyPoolSize][];
        for (int k = 0; k < keyPoolSize; k++) {
            keyPool[k] = ("key" + k).getBytes(StandardCharsets.UTF_8);
        }

        var rowFormat = new ES819Version3TSDBDocValuesFormat(false, false, false, false);
        var columnarFormat = new ES819Version3TSDBDocValuesFormat(false, false, false, true);

        try (var rowDir = newDirectory(); var columnarDir = newDirectory()) {
            var rowConfig = new IndexWriterConfig().setCodec(TestUtil.alwaysDocValuesFormat(rowFormat));
            var columnarConfig = new IndexWriterConfig().setCodec(TestUtil.alwaysDocValuesFormat(columnarFormat));

            try (var rowWriter = new IndexWriter(rowDir, rowConfig); var columnarWriter = new IndexWriter(columnarDir, columnarConfig)) {
                for (int d = 0; d < numDocs; d++) {
                    BytesRef blob = randomBlob(keyPool, Math.min(keyPoolSize, 10));
                    Document doc = new Document();
                    doc.add(new Field(FIELD, blob, FIELD_TYPE_WITH_ATTR));
                    rowWriter.addDocument(doc);
                    columnarWriter.addDocument(doc);
                }
                rowWriter.forceMerge(1);
                columnarWriter.forceMerge(1);
            }

            try (var rowIr = DirectoryReader.open(rowDir); var columnarIr = DirectoryReader.open(columnarDir)) {
                assertBinaryDocValuesMatch(rowIr.leaves().getFirst().reader(), columnarIr.leaves().getFirst().reader());
            }
        }
    }

    // ---- helpers ---------------------------------------------------------------------------------

    /**
     * Builds a random {@link MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull} blob.
     * Keys are sorted (TreeMap) so the columnar reconstruction — which also sorts keys — produces
     * byte-identical output. Slots may be null (via {@link #rarely()}) to exercise null handling.
     *
     * <p>Format: {@code [(valLen+1 or 0) vint][key bytes][0x00 separator][value bytes if non-null]...},
     * no leading count.
     */
    private BytesRef randomBlob(byte[][] keyPool, int keysPerDocMax) {
        int numKeys = randomIntBetween(1, keysPerDocMax);
        // TreeMap sorts by key bytes; columnar reconstruction also uses sorted order.
        TreeMap<BytesRef, BytesRef> kvMap = new TreeMap<>();
        for (int i = 0; i < numKeys; i++) {
            byte[] key = keyPool[randomInt(keyPool.length - 1)];
            byte[] val = randomByteArrayOfLength(randomIntBetween(0, 16));
            kvMap.put(new BytesRef(key), new BytesRef(val));
        }
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            for (var e : kvMap.entrySet()) {
                byte[] key = e.getKey().bytes;
                int keyLen = e.getKey().length;
                boolean isNull = rarely();
                if (isNull) {
                    out.writeVInt(0); // null slot sentinel
                    out.writeBytes(key, 0, keyLen);
                    out.writeByte((byte) 0); // separator
                } else {
                    byte[] val = e.getValue().bytes;
                    int valLen = e.getValue().length;
                    out.writeVInt(valLen + 1); // encodedLen = valLen + 1
                    out.writeBytes(key, 0, keyLen);
                    out.writeByte((byte) 0); // separator
                    out.writeBytes(val, 0, valLen);
                }
            }
            return out.bytes().toBytesRef();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void assertBinaryDocValuesMatch(LeafReader rowLeaf, LeafReader columnarLeaf) throws IOException {
        BinaryDocValues rowBdv = rowLeaf.getBinaryDocValues(FIELD);
        BinaryDocValues columnarBdv = columnarLeaf.getBinaryDocValues(FIELD);

        // Both should be non-null or both null (sparse docs may produce a null if no doc had a value).
        assertEquals("BinaryDocValues presence mismatch", rowBdv == null, columnarBdv == null);
        if (rowBdv == null) {
            return;
        }

        while (true) {
            int rowDoc = rowBdv.nextDoc();
            int columnarDoc = columnarBdv.nextDoc();
            assertEquals("doc ID mismatch", rowDoc, columnarDoc);
            if (rowDoc == NO_MORE_DOCS) {
                break;
            }
            assertEquals("blob mismatch at doc " + rowDoc, rowBdv.binaryValue(), columnarBdv.binaryValue());
        }
    }
}
