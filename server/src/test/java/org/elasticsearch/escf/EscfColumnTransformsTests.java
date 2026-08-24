/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.escf;

import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentString;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class EscfColumnTransformsTests extends ESTestCase {

    public void testStringColumn_denseDrains() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addString(utf8("alpha"));
        b.addString(utf8("beta"));
        b.addString(utf8("gamma"));
        EscfColumn col = EscfColumn.from(b.finish(3));

        assertEquals(EscfColumnKind.STRING, col.kind());
        assertTuples(EscfColumnTransforms.utf8Cursor(col, randomBoolean()), tuple(0, "alpha"), tuple(1, "beta"), tuple(2, "gamma"));
    }

    public void testStringColumn_absentRowsSkipped() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addString(utf8("x"));
        b.addAbsent();
        b.addString(utf8("z"));
        EscfColumn col = EscfColumn.from(b.finish(3));

        assertTuples(EscfColumnTransforms.utf8Cursor(col, randomBoolean()), tuple(0, "x"), tuple(2, "z"));
    }

    public void testLongColumn_canonicalToString() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addLong(42L);
        b.addLong(-1L);
        b.addLong(Long.MAX_VALUE);
        EscfColumn col = EscfColumn.from(b.finish(3));

        assertTuples(
            EscfColumnTransforms.utf8Cursor(col, randomBoolean()),
            tuple(0, "42"),
            tuple(1, "-1"),
            tuple(2, Long.toString(Long.MAX_VALUE))
        );
    }

    public void testLongColumn_sparseDrains() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addAbsent();
        b.addLong(10L);
        b.addAbsent();
        b.addLong(20L);
        EscfColumn col = EscfColumn.from(b.finish(4));

        assertTuples(EscfColumnTransforms.utf8Cursor(col, randomBoolean()), tuple(1, "10"), tuple(3, "20"));
    }

    public void testDoubleColumn_canonicalToString() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addDouble(3.14);
        b.addDouble(-0.5);
        EscfColumn col = EscfColumn.from(b.finish(2));

        assertTuples(
            EscfColumnTransforms.utf8Cursor(col, randomBoolean()),
            tuple(0, Double.toString(3.14)),
            tuple(1, Double.toString(-0.5))
        );
    }

    public void testBoolColumn_trueAndFalse() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addBoolean(true);
        b.addBoolean(false);
        b.addBoolean(true);
        EscfColumn col = EscfColumn.from(b.finish(3));

        assertTuples(EscfColumnTransforms.utf8Cursor(col, randomBoolean()), tuple(0, "true"), tuple(1, "false"), tuple(2, "true"));
    }

    public void testNullInUnion_emitsNullValue() {
        // addNull() promotes to UNION and records a NULL type byte for that row.
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addNull();
        b.addLong(7L);
        EscfColumn col = EscfColumn.from(b.finish(2));

        assertEquals(EscfColumnKind.UNION, col.kind());
        // Null rows emit a tuple with value() == null.
        assertTuples(EscfColumnTransforms.utf8Cursor(col, randomBoolean()), nullTuple(0), tuple(1, "7"));
    }

    public void testAllAbsent_emitsNothing() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addAbsent();
        b.addAbsent();
        EscfColumn col = EscfColumn.from(b.finish(2));

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, EscfColumnTransforms.utf8Cursor(col, randomBoolean()).nextDoc());
    }

    public void testLongArrayColumn_multipleElementsPerDoc() {
        // 3 rows: [[10, 20], [30], []]
        EscfColumnBuilder childBuilder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        childBuilder.addLong(10L);
        childBuilder.addLong(20L);
        childBuilder.addLong(30L);
        EscfColumn child = EscfColumn.from(childBuilder.finish(3));

        // row 0 → elements [0, 2), row 1 → [2, 3), row 2 → [3, 3) empty
        EscfArrayColumn array = new EscfArrayColumn(3, null, child, intsRef(new int[] { 0, 2, 3, 3 }));

        // Row 0 emits two tuples (both with doc-id 0); row 2 is empty → no tuple.
        assertTuples(EscfColumnTransforms.utf8Cursor(array, randomBoolean()), tuple(0, "10"), tuple(0, "20"), tuple(1, "30"));
    }

    public void testStringArrayColumn() {
        byte[] bytes = "helloworld".getBytes(StandardCharsets.UTF_8);
        int[] childOffsets = { 0, 5, 10 };
        EscfColumnData childData = EscfColumnData.ofVarWidth(EscfColumnKind.STRING, 2, null, childOffsets, new BytesArray(bytes));
        EscfColumn child = EscfColumn.from(childData);

        // 2 rows: row 0 → ["hello"], row 1 → ["world"]
        EscfArrayColumn array = new EscfArrayColumn(2, null, child, intsRef(new int[] { 0, 1, 2 }));

        assertTuples(EscfColumnTransforms.utf8Cursor(array, randomBoolean()), tuple(0, "hello"), tuple(1, "world"));
    }

    public void testEmptyArray_emitsNoTuples() {
        // A single row whose element range is empty [0, 0).
        EscfColumnBuilder childBuilder = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        childBuilder.addLong(99L);
        EscfColumn child = EscfColumn.from(childBuilder.finish(1));
        EscfArrayColumn array = new EscfArrayColumn(1, null, child, intsRef(new int[] { 0, 0 }));

        assertEquals(DocIdSetIterator.NO_MORE_DOCS, EscfColumnTransforms.utf8Cursor(array, randomBoolean()).nextDoc());
    }

    public void testMixedLongDoubleUnion() {
        EscfColumnBuilder b = new EscfColumnBuilder(EscfColumnBuilder.CollisionPolicy.MERGE);
        b.addLong(10L);
        b.addDouble(3.14);
        b.addLong(20L);
        EscfColumn col = EscfColumn.from(b.finish(3));

        assertEquals(EscfColumnKind.UNION, col.kind());
        assertTuples(
            EscfColumnTransforms.utf8Cursor(col, randomBoolean()),
            tuple(0, "10"),
            tuple(1, Double.toString(3.14)),
            tuple(2, "20")
        );
    }

    // EscfColumnBuilder always writes LONG/DOUBLE type bytes for scalar UNION rows, never INT/FLOAT.
    // INT and FLOAT only appear as element type bytes inside inline UNION_ARRAY payloads: the
    // encoder writes INT for values that fit in int range and FLOAT for values exactly representable
    // as float. When an array mixes INT and FLOAT elements (different column kinds), the encoder
    // stores the whole array inline as UNION_ARRAY, preserving the original element type bytes.

    public void testUnionArrayWithIntAndFloat_elementTypeBytesInAdvanceArray() throws IOException {
        // [42, 1.5]: 42 → INT element type byte, 1.5 → FLOAT element type byte; mixed kinds
        // force inline UNION_ARRAY storage, so advanceArray() dispatches on INT and FLOAT.
        try (EscfBatch batch = encode("""
            {"f":[42,1.5]}""")) {
            assertEquals(1, batch.docCount());
            EscfColumn col = columnByPath(batch, "f");
            assertEquals(EscfColumnKind.UNION, col.kind());

            assertTuples(
                EscfColumnTransforms.utf8Cursor(col, randomBoolean()),
                tuple(0, Integer.toString(42)),
                tuple(0, Float.toString(1.5f))
            );
        }
    }

    public void testNestedArraysAreFlattened() throws IOException {
        // [[1, 2], [3]] flattens to three tuples from doc 0, matching the row-path behaviour
        // in DocumentParser.parseArrayElements which recurses into nested arrays with the same
        // field name (line 837: START_ARRAY → parseArray(context, lastFieldName)).
        try (EscfBatch batch = encode("""
            {"f":[[1,2],[3]]}""")) {
            EscfColumn col = columnByPath(batch, "f");
            assertTuples(
                EscfColumnTransforms.utf8Cursor(col, randomBoolean()),
                tuple(0, Integer.toString(1)),
                tuple(0, Integer.toString(2)),
                tuple(0, Integer.toString(3))
            );
        }
    }

    public void testUnionArrayWithNullElement() throws IOException {
        // ["a", null]: string + null → UNION_ARRAY; null element emits a tuple with value() == null.
        try (EscfBatch batch = encode("""
            {"f":["a",null]}""")) {
            EscfColumn col = columnByPath(batch, "f");
            assertTuples(EscfColumnTransforms.utf8Cursor(col, randomBoolean()), tuple(0, "a"), nullTuple(0));
        }
    }

    public void testBinaryColumn_throws() {
        int[] offs = { 0, 2 };
        EscfColumnData data = EscfColumnData.ofVarWidth(EscfColumnKind.BINARY, 1, null, offs, new BytesArray(new byte[] { 1, 2 }));
        EscfColumn col = EscfColumn.from(data);

        ObjectTupleCursor<BytesRef> cursor = EscfColumnTransforms.utf8Cursor(col, randomBoolean());
        expectThrows(UnsupportedOperationException.class, cursor::nextDoc);
    }

    private static XContentString.UTF8Bytes utf8(String s) {
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        return new XContentString.UTF8Bytes(bytes, 0, bytes.length);
    }

    private static IntsRef intsRef(int[] ints) {
        return new IntsRef(ints, 0, ints.length);
    }

    /** Returns {docId, expectedUtf8} — a present-value tuple descriptor. */
    private static Object[] tuple(int doc, String value) {
        return new Object[] { doc, value };
    }

    /** Returns a null-value tuple descriptor (JSON null → value() == null). */
    private static Object[] nullTuple(int doc) {
        return new Object[] { doc, null };
    }

    /**
     * Drains the cursor and asserts it produces exactly the given tuples in order.
     * Each expected element is either {@code tuple(doc, string)} or {@code nullTuple(doc)}.
     */
    private static void assertTuples(ObjectTupleCursor<BytesRef> cursor, Object[]... expected) {
        List<Object[]> actual = new ArrayList<>();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef v = cursor.value();
            actual.add(new Object[] { doc, v == null ? null : BytesRef.deepCopyOf(v) });
        }
        assertEquals("tuple count", expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            int expectedDoc = (int) expected[i][0];
            String expectedValue = (String) expected[i][1];
            assertEquals("tuple[" + i + "].doc", expectedDoc, actual.get(i)[0]);
            if (expectedValue == null) {
                assertNull("tuple[" + i + "].value", actual.get(i)[1]);
            } else {
                assertEquals("tuple[" + i + "].value", new BytesRef(expectedValue), actual.get(i)[1]);
            }
        }
    }

    private static EscfBatch encode(String... jsonDocs) throws IOException {
        Recycler<BytesRef> recycler = new BytesRefRecycler(new MockPageCacheRecycler(Settings.EMPTY));
        try (EscfEncoder encoder = new EscfEncoder(recycler)) {
            for (String doc : jsonDocs) {
                encoder.addDocument(new BytesArray(doc), XContentType.JSON, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    private static EscfColumn columnByPath(EscfBatch batch, String path) {
        for (int i = 0; i < batch.schema().leafCount(); i++) {
            if (path.equals(batch.schema().getFullPath(i))) {
                return batch.column(i);
            }
        }
        throw new AssertionError("Column '" + path + "' not found in batch schema");
    }
}
